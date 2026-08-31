package fsm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store/pg"
	"github.com/jninng/go_config_center/internal/testutil"
	"github.com/jninng/go_config_center/internal/watch"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func mustFSM(t *testing.T) (*FSM, *pg.Store, string) {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_fsm_test")
	st, err := pg.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pg.Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(st.Close)
	f, err := New(context.Background(), st, nil)
	if err != nil {
		t.Fatalf("fsm.New: %v", err)
	}
	return f, st, dsn
}

func publishLog(t *testing.T, index, term uint64, ns, dataID, content string, expected *int64) *raft.Log {
	t.Helper()
	cmd, err := raftlog.NewPublish(raftlog.PublishData{
		Namespace: ns, DataId: dataID, DataType: "TEXT",
		Content: content, ExpectedVersion: expected,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b, AppendedAt: time.Now()}
}

func deleteLog(t *testing.T, index, term uint64, ns, dataID string, expected *int64) *raft.Log {
	t.Helper()
	cmd, err := raftlog.NewDelete(raftlog.DeleteData{
		Namespace: ns, DataId: dataID, ExpectedVersion: expected,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b, AppendedAt: time.Now()}
}

func rollbackLog(t *testing.T, index, term uint64, ns, dataID string, toVersion int64) *raft.Log {
	t.Helper()
	cmd, err := raftlog.NewRollback(raftlog.RollbackData{
		Namespace: ns, DataId: dataID, ToVersion: toVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b, AppendedAt: time.Now()}
}

func apply(t *testing.T, f *FSM, l *raft.Log) raftlog.Result {
	t.Helper()
	res, ok := f.Apply(l).(raftlog.Result)
	if !ok {
		t.Fatalf("Apply 返回 %T, want raftlog.Result", f.Apply(l))
	}
	return res
}

func TestApplyPublishReturnsResultAndAdvances(t *testing.T) {
	f, st, _ := mustFSM(t)
	res := apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	if res.Status != raftlog.StatusOK || res.Version != 1 || !res.Created {
		t.Fatalf("result = %+v", res)
	}
	idx, term, _ := st.LoadMeta(context.Background())
	if idx != 1 || term != 1 {
		t.Fatalf("meta = (%d,%d), want (1,1)", idx, term)
	}
}

// 验收项：重放通过 index ≤ last_applied_index 跳过，不产生重复写入。
// 同一实例内重复投递同一 index 返回缓存的原始结果（幂等且结果一致）；
// 跨实例（重启）重放因结果表为空返回 replayed，见 TestFSMRestartSkipsReplayedLogs。
func TestApplySameIndexIsSkippedWithoutDuplicateWrites(t *testing.T) {
	f, st, dsn := mustFSM(t)
	l1 := publishLog(t, 1, 1, t.Name(), "a", "v1", nil)
	l2 := publishLog(t, 2, 1, t.Name(), "a", "v2", nil)
	first1 := apply(t, f, l1)
	first2 := apply(t, f, l2)

	// Raft 可能向同一 FSM 实例重复投递已应用日志。
	res := apply(t, f, l1)
	if res.Status != raftlog.StatusOK || res.Version != first1.Version {
		t.Fatalf("重复投递 index=1 结果 = %+v, want 与首次一致 %+v", res, first1)
	}
	res = apply(t, f, l2)
	if res.Status != raftlog.StatusOK || res.Version != first2.Version {
		t.Fatalf("重复投递 index=2 结果 = %+v, want 与首次一致 %+v", res, first2)
	}

	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1", t.Name()); n != 2 {
		t.Fatalf("history 行数 = %d, want 2（跳过的重放不得写入）", n)
	}
	idx, _, _ := st.LoadMeta(context.Background())
	if idx != 2 {
		t.Fatalf("meta index = %d, want 2", idx)
	}
}

// 重启模拟：新 FSM 实例从 raft_fsm_meta 恢复进度，旧日志全部跳过，新日志继续。
func TestFSMRestartSkipsReplayedLogs(t *testing.T) {
	f1, st, _ := mustFSM(t)
	apply(t, f1, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f1, publishLog(t, 2, 1, t.Name(), "a", "v2", nil))

	f2, err := New(context.Background(), st, nil)
	if err != nil {
		t.Fatal(err)
	}
	res1 := apply(t, f2, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	if res1.Status != raftlog.StatusReplayed || res1.Index != 1 {
		t.Fatalf("重启后重放 index=1: %+v, want replayed 且携带 index", res1)
	}
	res2 := apply(t, f2, publishLog(t, 2, 1, t.Name(), "a", "v2", nil))
	if res2.Status != raftlog.StatusReplayed || res2.Index != 2 {
		t.Fatalf("重启后重放 index=2: %+v, want replayed 且携带 index", res2)
	}
	res := apply(t, f2, publishLog(t, 3, 1, t.Name(), "a", "v3", nil))
	if res.Status != raftlog.StatusOK || res.Version != 3 {
		t.Fatalf("重启后新日志 = %+v, want ok v3", res)
	}
}

// 验收项：CAS 失败发生在 Apply 阶段，结果经 Apply 返回值回传（DESIGN §3.3）。
func TestApplyCASConflictResultCarriedBack(t *testing.T) {
	f, st, _ := mustFSM(t)
	wrong := int64(99)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))

	res := apply(t, f, publishLog(t, 2, 1, t.Name(), "a", "v2", &wrong))
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("result = %+v, want conflict current=1", res)
	}
	idx, _, _ := st.LoadMeta(context.Background())
	if idx != 2 {
		t.Fatalf("meta index = %d, want 2（冲突日志也消耗进度）", idx)
	}
}

// 验收项：Delete 后 GET 404；History 追加 op=delete、content NULL 的 Tombstone 且占用版本号。
func TestApplyDeleteWritesTombstone(t *testing.T) {
	f, st, dsn := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))

	res := apply(t, f, deleteLog(t, 2, 1, t.Name(), "a", nil))
	if res.Status != raftlog.StatusOK || res.Version != 2 {
		t.Fatalf("result = %+v, want ok v2", res)
	}
	if entry, _ := st.GetEntry(context.Background(), t.Name(), "a"); entry != nil {
		t.Fatalf("删除后 GetEntry = %+v, want nil（GET 404）", entry)
	}
	if n := testutil.QueryInt(t, dsn,
		`SELECT count(*) FROM biz_config_history
		 WHERE namespace=$1 AND op='delete' AND content IS NULL AND version=2`, t.Name()); n != 1 {
		t.Fatalf("Tombstone 行数 = %d, want 1", n)
	}
}

// 删除不存在的条目：结果 not_found，但日志已提交，进度照常推进。
func TestApplyDeleteMissingEntryNotFound(t *testing.T) {
	f, st, _ := mustFSM(t)
	res := apply(t, f, deleteLog(t, 1, 1, t.Name(), "ghost", nil))
	if res.Status != raftlog.StatusNotFound {
		t.Fatalf("result = %+v, want not_found", res)
	}
	idx, _, _ := st.LoadMeta(context.Background())
	if idx != 1 {
		t.Fatalf("meta index = %d, want 1（not_found 也消耗进度）", idx)
	}
}

// 验收项：Delete 的 CAS 失败发生在 Apply 阶段，经结果表回传且不写 Tombstone。
func TestApplyDeleteCASConflictCarriedBack(t *testing.T) {
	f, st, dsn := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))

	wrong := int64(99)
	res := apply(t, f, deleteLog(t, 2, 1, t.Name(), "a", &wrong))
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("result = %+v, want conflict current=1", res)
	}
	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1", t.Name()); n != 1 {
		t.Fatalf("history 行数 = %d, want 1（冲突不追加 Tombstone）", n)
	}
	idx, _, _ := st.LoadMeta(context.Background())
	if idx != 2 {
		t.Fatalf("meta index = %d, want 2", idx)
	}
}

// 删除后重建延续版本序列：墓碑占用的版本号不回收（DESIGN §3.1）。
func TestApplyDeleteThenRepublishContinuesVersion(t *testing.T) {
	f, _, _ := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, deleteLog(t, 2, 1, t.Name(), "a", nil))

	res := apply(t, f, publishLog(t, 3, 1, t.Name(), "a", "reborn", nil))
	if res.Status != raftlog.StatusOK || res.Version != 3 || !res.Created {
		t.Fatalf("result = %+v, want ok v3 created", res)
	}
}

// 验收项：Rollback 以旧内容发布新版本，结果经 Apply 回传。
func TestApplyRollbackRestoresOldContent(t *testing.T) {
	f, st, _ := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, publishLog(t, 2, 1, t.Name(), "a", "v2", nil))

	res := apply(t, f, rollbackLog(t, 3, 1, t.Name(), "a", 1))
	if res.Status != raftlog.StatusOK || res.Version != 3 {
		t.Fatalf("result = %+v, want ok v3", res)
	}
	entry, _ := st.GetEntry(context.Background(), t.Name(), "a")
	if entry == nil || entry.Content != "v1" || entry.Version != 3 {
		t.Fatalf("entry = %+v, want v3 内容 v1", entry)
	}
}

// 验收项：无效回滚目标在 Apply 阶段判定（404/409），进度照常推进。
func TestApplyRollbackInvalidTargets(t *testing.T) {
	f, st, _ := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, deleteLog(t, 2, 1, t.Name(), "a", nil))

	res := apply(t, f, rollbackLog(t, 3, 1, t.Name(), "a", 99))
	if res.Status != raftlog.StatusNotFound {
		t.Fatalf("to_version=99 = %+v, want not_found", res)
	}
	res = apply(t, f, rollbackLog(t, 4, 1, t.Name(), "a", 2))
	if res.Status != raftlog.StatusInvalidRollback || res.CurrentVersion != 2 {
		t.Fatalf("to_version=2（墓碑）= %+v, want invalid_rollback current=2", res)
	}
	idx, _, _ := st.LoadMeta(context.Background())
	if idx != 4 {
		t.Fatalf("meta index = %d, want 4", idx)
	}
}

// apply-or-die：PG 故障时 Apply 必须崩溃而不是吞错跳过（否则 meta 出现空洞）。
func TestApplyPanicsWhenStoreUnavailable(t *testing.T) {
	f, st, _ := mustFSM(t)
	st.Close() // 模拟 PG 故障
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("store 不可用时 Apply 应 panic")
		}
	}()
	f.Apply(publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
}

// 结果表仅保留近期条目（DESIGN §3.3）。
func TestResultTableKeepsRecentEntries(t *testing.T) {
	f, _, _ := mustFSM(t)
	f.resultKeep = 4
	for i := uint64(1); i <= 6; i++ {
		apply(t, f, publishLog(t, i, 1, t.Name(), "a", fmt.Sprintf("v%d", i), nil))
	}
	if _, ok := f.Result(2); ok {
		t.Fatal("index=2 应已被淘汰")
	}
	if res, ok := f.Result(6); !ok || res.Version != 6 {
		t.Fatalf("index=6 应在结果表中: %v %v", res, ok)
	}
}

// 验收：FSM Apply 到 Publish/Delete 时唤醒本机挂起的 Watch；未变更状态的日志不唤醒。
func TestApplyNotifiesWatchHub(t *testing.T) {
	dsn := testutil.NewDB(t, "gcc_fsm_test")
	st, err := pg.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	hub := watch.NewHub()
	f, err := New(context.Background(), st, hub)
	if err != nil {
		t.Fatal(err)
	}

	release, events := hub.Register(t.Name(), "a")
	defer release()

	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	select {
	case ev := <-events:
		if ev.Version != 1 || ev.Deleted {
			t.Fatalf("publish 事件 = %+v, want v1 非删除", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("publish 后未唤醒 Watch")
	}

	apply(t, f, deleteLog(t, 2, 1, t.Name(), "a", nil))
	select {
	case ev := <-events:
		if ev.Version != 2 || !ev.Deleted {
			t.Fatalf("delete 事件 = %+v, want v2 删除", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delete 后未唤醒 Watch")
	}

	// CAS 冲突不改变条目状态，不唤醒。
	wrong := int64(99)
	apply(t, f, publishLog(t, 3, 1, t.Name(), "a", "nope", &wrong))
	select {
	case ev := <-events:
		t.Fatalf("冲突不应唤醒: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

// 验收：/metrics 最低集合中 fsm_apply_results_total 按 result 标签区分四类结果（DESIGN §7）。
func TestApplyResultsMetricsLabels(t *testing.T) {
	f, _, _ := mustFSM(t)
	// 覆盖 ok / version_conflict / not_found / invalid_rollback 四类 Apply 结果。
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	wrong := int64(99)
	apply(t, f, publishLog(t, 2, 1, t.Name(), "a", "v2", &wrong))
	apply(t, f, deleteLog(t, 3, 1, t.Name(), "ghost", nil))
	apply(t, f, rollbackLog(t, 4, 1, t.Name(), "a", 99))

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	type seriesKey struct{ name, result string }
	seen := map[seriesKey]bool{}
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
		if mf.GetType() != dto.MetricType_COUNTER || mf.GetName() != "fsm_apply_results_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "result" {
					seen[seriesKey{mf.GetName(), l.GetValue()}] = true
				}
			}
		}
	}
	for _, result := range []string{"ok", "version_conflict", "not_found", "invalid_rollback"} {
		if !seen[seriesKey{"fsm_apply_results_total", result}] {
			t.Fatalf("fsm_apply_results_total 缺少 result=%q 序列（现有 %v）", result, seen)
		}
	}
	for _, name := range []string{
		"fsm_last_applied_index", "fsm_apply_duration_seconds",
		"watch_waiters", "watch_notifications_total",
		"snapshot_duration_seconds", "snapshot_size_bytes", "restore_skipped_total",
	} {
		if !names[name] {
			t.Fatalf("指标缺失: %s", name)
		}
	}
}

func deleteNamespaceLog(t *testing.T, index, term uint64, ns string) *raft.Log {
	t.Helper()
	cmd, err := raftlog.NewDeleteNamespace(raftlog.DeleteNamespaceData{Namespace: ns, Actor: "admin"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := raftlog.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b, AppendedAt: time.Now()}
}

// 验收：delete_namespace 单日志原子删除全部条目（各写 Tombstone）、结果携带
// 被删条目与墓碑版本、进度推进；删除后逐条回滚可重建。
func TestApplyDeleteNamespaceRemovesAllAndAdvances(t *testing.T) {
	f, st, dsn := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, publishLog(t, 2, 1, t.Name(), "a", "v2", nil))
	apply(t, f, publishLog(t, 3, 1, t.Name(), "b", "x", nil))

	res := apply(t, f, deleteNamespaceLog(t, 4, 1, t.Name()))
	if res.Status != raftlog.StatusOK || res.Entries != 2 {
		t.Fatalf("result = %+v, want ok entries=2", res)
	}
	byID := map[string]int64{}
	for _, e := range res.Deleted {
		byID[e.DataId] = e.Version
	}
	if byID["a"] != 3 || byID["b"] != 2 {
		t.Fatalf("墓碑版本 = %v, want a=3 b=2", byID)
	}
	if entry, _ := st.GetEntry(context.Background(), t.Name(), "a"); entry != nil {
		t.Fatalf("删除后 GetEntry(a) = %+v, want nil", entry)
	}
	if n := testutil.QueryInt(t, dsn,
		`SELECT count(*) FROM biz_config_history WHERE namespace=$1 AND op='delete' AND content IS NULL`, t.Name()); n != 2 {
		t.Fatalf("Tombstone 行数 = %d, want 2", n)
	}
	if idx, _, _ := st.LoadMeta(context.Background()); idx != 4 {
		t.Fatalf("meta index = %d, want 4", idx)
	}

	// 删除后回滚重建：a 回滚到墓碑之前的 v1。
	res = apply(t, f, rollbackLog(t, 5, 1, t.Name(), "a", 1))
	if res.Status != raftlog.StatusOK || res.Version != 4 || !res.Created {
		t.Fatalf("回滚重建 = %+v, want ok v4 created", res)
	}
	entry, _ := st.GetEntry(context.Background(), t.Name(), "a")
	if entry == nil || entry.Content != "v1" || entry.Version != 4 {
		t.Fatalf("重建条目 = %+v, want v1 @ v4", entry)
	}
}

// 验收：delete_namespace 逐条唤醒该 Namespace 下挂起的 Watch（Deleted 语义与单条删除一致）。
func TestApplyDeleteNamespaceNotifiesEachWatch(t *testing.T) {
	dsn := testutil.NewDB(t, "gcc_fsm_test")
	st, err := pg.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	hub := watch.NewHub()
	f, err := New(context.Background(), st, hub)
	if err != nil {
		t.Fatal(err)
	}

	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, publishLog(t, 2, 1, t.Name(), "b", "x", nil))
	releaseA, eventsA := hub.Register(t.Name(), "a")
	defer releaseA()
	releaseB, eventsB := hub.Register(t.Name(), "b")
	defer releaseB()

	apply(t, f, deleteNamespaceLog(t, 3, 1, t.Name()))

	for name, ch := range map[string]<-chan watch.Event{"a": eventsA, "b": eventsB} {
		select {
		case ev := <-ch:
			if !ev.Deleted {
				t.Fatalf("%s 事件 = %+v, want deleted", name, ev)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s 挂起 Watch 未在 delete_namespace 后唤醒", name)
		}
	}
}
