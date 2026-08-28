package fsm

import (
	"context"
	"fmt"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
)

func mustFSM(t *testing.T) (*FSM, *store.Store, string) {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_fsm_test")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(st.Close)
	f, err := New(context.Background(), st)
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
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b}
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
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b}
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
	return &raft.Log{Index: index, Term: term, Type: raft.LogCommand, Data: b}
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

	f2, err := New(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if res := apply(t, f2, publishLog(t, 1, 1, t.Name(), "a", "v1", nil)); res.Status != raftlog.StatusReplayed {
		t.Fatalf("重启后重放 index=1: %+v", res)
	}
	if res := apply(t, f2, publishLog(t, 2, 1, t.Name(), "a", "v2", nil)); res.Status != raftlog.StatusReplayed {
		t.Fatalf("重启后重放 index=2: %+v", res)
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
