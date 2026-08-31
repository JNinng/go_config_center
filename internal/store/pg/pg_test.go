package pg

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
)

func mustOpen(t *testing.T) (*Store, string) {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_store_test")
	st, err := Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	t.Cleanup(st.Close)
	return st, dsn
}

func pub(ns, dataID, content string, expected *int64) raftlog.PublishData {
	return raftlog.PublishData{
		Namespace: ns, DataId: dataID, DataType: "TEXT",
		Content: content, ExpectedVersion: expected,
	}
}

func del(ns, dataID string, expected *int64) raftlog.DeleteData {
	return raftlog.DeleteData{Namespace: ns, DataId: dataID, ExpectedVersion: expected}
}

func rb(ns, dataID string, toVersion int64) raftlog.RollbackData {
	return raftlog.RollbackData{Namespace: ns, DataId: dataID, ToVersion: toVersion}
}

func TestApplyPublishFirstVersionIsOne(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()

	res, err := st.ApplyPublish(ctx, pub(t.Name(), "db.password", "s3cret", nil), time.Now(), 1, 1)
	if err != nil {
		t.Fatalf("ApplyPublish: %v", err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 1 || !res.Created {
		t.Fatalf("result = %+v, want ok v1 created", res)
	}

	entry, err := st.GetEntry(ctx, t.Name(), "db.password")
	if err != nil || entry == nil {
		t.Fatalf("GetEntry: %v %v", entry, err)
	}
	if entry.Version != 1 || entry.Content != "s3cret" || entry.DataType != "TEXT" {
		t.Fatalf("entry = %+v", entry)
	}
}

func TestApplyPublishSecondVersionIncrements(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	res, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "2", nil), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 2 || res.Created {
		t.Fatalf("result = %+v, want ok v2 not created", res)
	}
	entry, _ := st.GetEntry(ctx, t.Name(), "a")
	if entry.Version != 2 || entry.Content != "2" {
		t.Fatalf("entry = %+v", entry)
	}
}

// CAS 不匹配发生在 Apply 阶段：结果为 conflict，但日志已提交，raft_fsm_meta 必须照常推进。
func TestApplyPublishCASConflictStillAdvancesMeta(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	wrong := int64(99)
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}

	res, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "2", &wrong), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("result = %+v, want conflict current=1", res)
	}

	idx, term, err := st.LoadMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 2 || term != 1 {
		t.Fatalf("meta = (%d,%d), want (2,1)：无效操作的日志也消耗进度", idx, term)
	}
	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1 AND data_id='a'", t.Name()); n != 1 {
		t.Fatalf("history 行数 = %d, want 1（冲突不追加 History）", n)
	}
}

func TestApplyPublishCASCases(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	zero := int64(0)
	one := int64(1)

	// expected=0 在不存在的条目上通过（create-if-absent，DESIGN §4.1）
	res, _ := st.ApplyPublish(ctx, pub(t.Name(), "new", "x", &zero), time.Now(), 1, 1)
	if res.Status != raftlog.StatusOK || res.Version != 1 {
		t.Fatalf("expected=0 create: %+v", res)
	}
	// expected=0 在已存在条目上冲突
	res, _ = st.ApplyPublish(ctx, pub(t.Name(), "new", "y", &zero), time.Now(), 2, 1)
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("expected=0 on existing: %+v", res)
	}
	// expected=1 匹配当前版本，通过
	res, _ = st.ApplyPublish(ctx, pub(t.Name(), "new", "z", &one), time.Now(), 3, 1)
	if res.Status != raftlog.StatusOK || res.Version != 2 {
		t.Fatalf("expected=1 match: %+v", res)
	}
}

// DESIGN §4.1：expected_version=0 在已删除（墓碑）条目上也要通过——删除后重建；
// N>0 与墓碑版本匹配同样通过。删除 API 属票 03，这里直接落 Tombstone 行模拟。
func TestApplyPublishCASAfterTombstone(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	ns := t.Name()
	// 条目曾发布到 v2，后被删除（Tombstone v3）：biz_configs 无行。
	if _, err := st.Pool().Exec(ctx, `
		INSERT INTO biz_config_history (namespace, data_id, version, op)
		VALUES ($1, 'a', 1, 'publish'), ($1, 'a', 2, 'publish'), ($1, 'a', 3, 'delete')`, ns); err != nil {
		t.Fatal(err)
	}

	zero := int64(0)
	res, err := st.ApplyPublish(ctx, pub(ns, "a", "reborn", &zero), time.Now(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 4 || !res.Created {
		t.Fatalf("墓碑后 expected=0 应重建: %+v, want ok v4 created", res)
	}

	// 再次置为墓碑（v5），expected=5 匹配墓碑版本 → 通过，重建为 v6。
	if _, err := st.Pool().Exec(ctx, `
		DELETE FROM biz_configs WHERE namespace=$1 AND data_id='a'`, ns); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Pool().Exec(ctx, `
		INSERT INTO biz_config_history (namespace, data_id, version, op)
		VALUES ($1, 'a', 5, 'delete')`, ns); err != nil {
		t.Fatal(err)
	}
	five := int64(5)
	res, err = st.ApplyPublish(ctx, pub(ns, "a", "reborn2", &five), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 6 || !res.Created {
		t.Fatalf("expected=墓碑版本应通过: %+v, want ok v6 created", res)
	}

	// expected 指向重建前的旧版本 → 冲突，current 为当前存活版本。
	four := int64(4)
	res, _ = st.ApplyPublish(ctx, pub(ns, "a", "stale", &four), time.Now(), 3, 1)
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 6 {
		t.Fatalf("过期 expected 应冲突: %+v, want conflict current=6", res)
	}
}

// 三表同事务：重开 Store（模拟重启），meta 与业务数据都在。
func TestStateSurvivesStoreReopen(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), time.Now(), 7, 2); err != nil {
		t.Fatal(err)
	}
	st.Close()

	st2, err := Open(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	idx, term, err := st2.LoadMeta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if idx != 7 || term != 2 {
		t.Fatalf("meta = (%d,%d), want (7,2)", idx, term)
	}
	entry, _ := st2.GetEntry(ctx, t.Name(), "a")
	if entry == nil || entry.Version != 1 {
		t.Fatalf("entry = %+v", entry)
	}
}

func TestGetEntryMissingReturnsNil(t *testing.T) {
	st, _ := mustOpen(t)
	entry, err := st.GetEntry(context.Background(), t.Name(), "nope")
	if err != nil {
		t.Fatal(err)
	}
	if entry != nil {
		t.Fatalf("entry = %+v, want nil", entry)
	}
}

// 删除写 Tombstone（ADR-0010）：biz_configs 行消失，History 追加 op=delete、
// content 为 NULL 的记录，版本号照常消耗，meta 同事务推进。
func TestApplyDeleteWritesTombstone(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}

	res, err := st.ApplyDelete(ctx, del(t.Name(), "a", nil), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 2 {
		t.Fatalf("result = %+v, want ok v2", res)
	}
	if entry, _ := st.GetEntry(ctx, t.Name(), "a"); entry != nil {
		t.Fatalf("删除后 GetEntry = %+v, want nil", entry)
	}
	var op string
	var content, dataType *string
	if err := st.Pool().QueryRow(ctx,
		`SELECT op, content, data_type FROM biz_config_history
		 WHERE namespace=$1 AND data_id='a' AND version=2`, t.Name()).
		Scan(&op, &content, &dataType); err != nil {
		t.Fatal(err)
	}
	if op != "delete" || content != nil || dataType != nil {
		t.Fatalf("Tombstone = (op=%s, content=%v, data_type=%v), want (delete, NULL, NULL)", op, content, dataType)
	}
	if idx, _, _ := st.LoadMeta(ctx); idx != 2 {
		t.Fatalf("meta index = %d, want 2", idx)
	}
}

// 条目不存在或已删除 → not_found（无物可删），但日志已提交，进度照常推进。
func TestApplyDeleteMissingReturnsNotFound(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()

	res, err := st.ApplyDelete(ctx, del(t.Name(), "ghost", nil), time.Now(), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusNotFound {
		t.Fatalf("result = %+v, want not_found", res)
	}
	if idx, _, _ := st.LoadMeta(ctx); idx != 1 {
		t.Fatalf("meta index = %d, want 1（无效删除也消耗进度）", idx)
	}
}

// Delete 的 CAS（DESIGN §4.1）：expected=0 与删除互斥恒冲突；N 不符冲突且不写 Tombstone。
func TestApplyDeleteCASMismatch(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}

	zero := int64(0)
	res, err := st.ApplyDelete(ctx, del(t.Name(), "a", &zero), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("expected=0 删除存活条目: %+v, want conflict current=1", res)
	}

	wrong := int64(99)
	res, err = st.ApplyDelete(ctx, del(t.Name(), "a", &wrong), time.Now(), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("expected=99: %+v, want conflict current=1", res)
	}

	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1", t.Name()); n != 1 {
		t.Fatalf("history 行数 = %d, want 1（冲突不追加 Tombstone）", n)
	}
	if idx, _, _ := st.LoadMeta(ctx); idx != 3 {
		t.Fatalf("meta index = %d, want 3", idx)
	}
}

// 删除后重建延续同一版本序列（DESIGN §3.1）：墓碑占用的版本号不回收。
func TestRepublishAfterDeleteContinuesVersion(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if res, _ := st.ApplyDelete(ctx, del(t.Name(), "a", nil), time.Now(), 2, 1); res.Status != raftlog.StatusOK {
		t.Fatalf("delete: %+v", res)
	}
	res, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "reborn", nil), time.Now(), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 3 || !res.Created {
		t.Fatalf("删除后重建 = %+v, want ok v3 created", res)
	}
}

// ListHistory 倒序分页（DESIGN §5.1）：含 Tombstone，支持 limit / before_version。
func TestListHistory(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	withTags := pub(t.Name(), "a", "v1", nil)
	withTags.Tags = map[string]string{"team": "core"}
	if _, err := st.ApplyPublish(ctx, withTags, time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v2", nil), time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if res, _ := st.ApplyDelete(ctx, del(t.Name(), "a", nil), time.Now(), 3, 1); res.Status != raftlog.StatusOK {
		t.Fatalf("delete: %+v", res)
	}

	records, err := st.ListHistory(ctx, t.Name(), "a", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || records[0].Version != 3 || records[2].Version != 1 {
		t.Fatalf("records = %+v, want 倒序 [3 2 1]", records)
	}
	if records[0].Op != "delete" || records[0].Content != nil || records[0].DataType != nil {
		t.Fatalf("墓碑记录 = %+v, want op=delete content NULL", records[0])
	}
	if records[2].Op != "publish" || records[2].Content == nil || records[2].Tags["team"] != "core" {
		t.Fatalf("publish 记录 = %+v, want 含 content 与 tags", records[2])
	}

	if records, _ = st.ListHistory(ctx, t.Name(), "a", 2, nil); len(records) != 2 ||
		records[0].Version != 3 || records[1].Version != 2 {
		t.Fatalf("limit=2 = %+v, want [3 2]", records)
	}
	two := int64(2)
	if records, _ = st.ListHistory(ctx, t.Name(), "a", 50, &two); len(records) != 1 || records[0].Version != 1 {
		t.Fatalf("before_version=2 = %+v, want [1]", records)
	}
	if records, _ = st.ListHistory(ctx, t.Name(), "a", store.MaxHistoryLimit+10, nil); len(records) != 3 {
		t.Fatalf("超上限 limit 收敛后 = %d 条, want 3", len(records))
	}
	if records, _ = st.ListHistory(ctx, t.Name(), "nope", 0, nil); len(records) != 0 {
		t.Fatalf("不存在的条目 = %d 条, want 0", len(records))
	}
}

// Rollback 以旧内容发布新版本（ADR-0006）：内容恢复、History 追加 publish 记录、版本延续。
func TestApplyRollbackPublishesOldContentAsNewVersion(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	v1 := pub(t.Name(), "a", "v1-content", nil)
	v1.Tags = map[string]string{"team": "core"}
	if _, err := st.ApplyPublish(ctx, v1, time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v2-content", nil), time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}

	res, err := st.ApplyRollback(ctx, rb(t.Name(), "a", 1), time.Now(), 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 3 || res.Created {
		t.Fatalf("result = %+v, want ok v3", res)
	}
	entry, _ := st.GetEntry(ctx, t.Name(), "a")
	if entry == nil || entry.Version != 3 || entry.Content != "v1-content" || entry.Tags["team"] != "core" {
		t.Fatalf("entry = %+v, want v3 内容与 tags 均为 v1 版", entry)
	}
	var op string
	if err := st.Pool().QueryRow(ctx,
		`SELECT op FROM biz_config_history WHERE namespace=$1 AND data_id='a' AND version=3`,
		t.Name()).Scan(&op); err != nil {
		t.Fatal(err)
	}
	if op != "publish" {
		t.Fatalf("History 记录 op = %s, want publish", op)
	}
}

// 无效目标：History 中不存在 → not_found；指向墓碑或 ≥ 当前版本 → invalid_rollback；
// 二者都不追加 History，进度照常推进。
func TestApplyRollbackInvalidTargets(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v2", nil), time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if res, _ := st.ApplyDelete(ctx, del(t.Name(), "a", nil), time.Now(), 3, 1); res.Status != raftlog.StatusOK {
		t.Fatalf("delete: %+v", res)
	}

	res, err := st.ApplyRollback(ctx, rb(t.Name(), "a", 99), time.Now(), 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusNotFound {
		t.Fatalf("to_version=99 = %+v, want not_found", res)
	}
	res, err = st.ApplyRollback(ctx, rb(t.Name(), "a", 3), time.Now(), 5, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusInvalidRollback || res.CurrentVersion != 3 {
		t.Fatalf("to_version=3（墓碑）= %+v, want invalid_rollback current=3", res)
	}
	res, err = st.ApplyRollback(ctx, rb(t.Name(), "ghost", 1), time.Now(), 6, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusNotFound {
		t.Fatalf("条目从未存在 = %+v, want not_found", res)
	}
	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1", t.Name()); n != 3 {
		t.Fatalf("history 行数 = %d, want 3（无效回滚不追加记录）", n)
	}
	if idx, _, _ := st.LoadMeta(ctx); idx != 6 {
		t.Fatalf("meta index = %d, want 6（无效回滚也消耗进度）", idx)
	}
}

// 链式回滚：回滚后仍可回滚到任何既存版本；删除后回滚可重建条目（Tombstone 语义）。
func TestApplyRollbackChainAndRebuild(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v2", nil), time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if res, _ := st.ApplyRollback(ctx, rb(t.Name(), "a", 1), time.Now(), 3, 1); res.Version != 3 {
		t.Fatalf("rollback to 1: %+v", res)
	}
	res, err := st.ApplyRollback(ctx, rb(t.Name(), "a", 2), time.Now(), 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 4 {
		t.Fatalf("链式回滚 to 2 = %+v, want ok v4", res)
	}
	if entry, _ := st.GetEntry(ctx, t.Name(), "a"); entry == nil || entry.Content != "v2" {
		t.Fatalf("entry = %+v, want 内容 v2", entry)
	}

	// 删除后回滚：current 为墓碑版本 5，回滚到 v4 重建为 v6。
	if res, _ := st.ApplyDelete(ctx, del(t.Name(), "a", nil), time.Now(), 5, 1); res.Status != raftlog.StatusOK {
		t.Fatalf("delete: %+v", res)
	}
	res, err = st.ApplyRollback(ctx, rb(t.Name(), "a", 4), time.Now(), 6, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 6 || !res.Created {
		t.Fatalf("墓碑后回滚 = %+v, want ok v6 created", res)
	}
}

// 回滚目标解析在 Apply 事务内：同一事务读到的 current 决定有效性判定。
func TestApplyRollbackToCurrentVersionRejected(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	res, err := st.ApplyRollback(ctx, rb(t.Name(), "a", 1), time.Now(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusInvalidRollback {
		t.Fatalf("回滚到当前版本 = %+v, want invalid_rollback", res)
	}
}

// Migrate 幂等，可重复执行。
func TestMigrateIdempotent(t *testing.T) {
	st, _ := mustOpen(t)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("二次 Migrate: %v", err)
	}
}

// 条目列表（DESIGN §5.1）：data_id 字典序、keyset 分页、已删除条目不出现、
// limit 收敛与元数据不含 content。
func TestListEntriesKeysetPagination(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	for _, id := range []string{"b", "a", "c"} {
		if _, err := st.ApplyPublish(ctx, pub(t.Name(), id, "content-of-"+id, nil), time.Now(), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	if res, err := st.ApplyDelete(ctx, del(t.Name(), "b", nil), time.Now(), 0, 0); err != nil || res.Status != raftlog.StatusOK {
		t.Fatalf("delete b = %+v %v", res, err)
	}

	names := func(entries []store.EntryMeta) []string {
		out := make([]string, 0, len(entries))
		for _, e := range entries {
			out = append(out, e.DataId)
		}
		return out
	}

	page1, err := st.ListEntries(ctx, t.Name(), 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(page1); len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("第一页 = %v, want [a c]（b 已删除不出现）", got)
	}
	if page1[0].Version != 1 || page1[0].DataType != "TEXT" {
		t.Fatalf("元数据 = %+v", page1[0])
	}

	page2, err := st.ListEntries(ctx, t.Name(), 2, &page1[1].DataId)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(page2); len(got) != 0 {
		t.Fatalf("第二页 = %v, want 空", got)
	}

	// limit ≤0 取默认值，超过上限收敛。
	if n, err := st.ListEntries(ctx, t.Name(), 0, nil); err != nil || len(n) != 2 {
		t.Fatalf("limit=0 = %d 条 %v, want 默认页大小下的 2 条", len(n), err)
	}
	if n, err := st.ListEntries(ctx, t.Name(), store.MaxListLimit+1, nil); err != nil || len(n) != 2 {
		t.Fatalf("limit 超上限 = %d 条 %v, want 收敛后 2 条", len(n), err)
	}
}

// Namespace 清单：去重、字典序、条目全部删除的 Namespace 不出现。
func TestListNamespacesDistinctOrdered(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()
	for _, ns := range []string{"prod", "dev", "dev"} {
		if _, err := st.ApplyPublish(ctx, pub(ns, "a", "v1", nil), time.Now(), 0, 0); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.ListNamespaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "dev" || got[1] != "prod" {
		t.Fatalf("namespaces = %v, want [dev prod]", got)
	}

	if _, err := st.ApplyDelete(ctx, del("dev", "a", nil), time.Now(), 0, 0); err != nil {
		t.Fatal(err)
	}
	got, err = st.ListNamespaces(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "prod" {
		t.Fatalf("dev 清空后 namespaces = %v, want [prod]", got)
	}
}

// Open 对不存在的库自动建库后重连（DESIGN §6.2），建出的库可直接迁移使用。
func TestOpenAutoCreatesMissingDatabase(t *testing.T) {
	adminDSN := testutil.AdminDSN()
	if adminDSN == "" {
		t.Skip("GCC_TEST_PG_DSN 未设置，跳过 PG 集成测试")
	}
	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatalf("解析管理员 DSN: %v", err)
	}
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	name := fmt.Sprintf("gcc_autocreate_%d_%d", os.Getpid(), time.Now().UnixNano())
	dsn := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, name, sslmode)

	ctx := context.Background()
	st, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("Open 应自动创建不存在的库 %s: %v", name, err)
	}
	defer st.Close()
	t.Cleanup(func() {
		admin, err := pgx.Connect(ctx, adminDSN)
		if err != nil {
			return
		}
		defer admin.Close(ctx)
		_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
	})

	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if _, _, err := st.LoadMeta(ctx); err != nil {
		t.Fatalf("LoadMeta: %v", err)
	}
}

// 删除 Namespace（DESIGN §5.1）：单事务删光存活条目并各写 Tombstone（墓碑版本 =
// 当前版本+1）、进度推进、Namespace 移出清单；空 Namespace 幂等；删后可逐条回滚重建。
func TestApplyDeleteNamespace(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	ns := t.Name()
	if _, err := st.ApplyPublish(ctx, pub(ns, "a", "v1", nil), time.Now(), 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(ns, "a", "v2", nil), time.Now(), 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.ApplyPublish(ctx, pub(ns, "b", "x", nil), time.Now(), 3, 1); err != nil {
		t.Fatal(err)
	}

	res, err := st.ApplyDeleteNamespace(ctx, raftlog.DeleteNamespaceData{Namespace: ns, Actor: "admin"}, time.Now(), 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Entries != 2 {
		t.Fatalf("result = %+v, want ok entries=2", res)
	}
	byID := map[string]int64{}
	for _, e := range res.Deleted {
		byID[e.DataId] = e.Version
	}
	if byID["a"] != 3 || byID["b"] != 2 {
		t.Fatalf("墓碑版本 = %v, want a=3（v2+1）b=2（v1+1）", byID)
	}

	if n := testutil.QueryInt(t, dsn, "SELECT count(*) FROM biz_configs WHERE namespace=$1", ns); n != 0 {
		t.Fatalf("biz_configs 剩余 %d 行, want 0", n)
	}
	if n := testutil.QueryInt(t, dsn,
		"SELECT count(*) FROM biz_config_history WHERE namespace=$1 AND op='delete' AND content IS NULL", ns); n != 2 {
		t.Fatalf("Tombstone 行数 = %d, want 2", n)
	}
	if namespaces, err := st.ListNamespaces(ctx); err != nil || len(namespaces) != 0 {
		t.Fatalf("删除后 namespaces = %v %v, want 空（无存活条目）", namespaces, err)
	}
	if idx, _, _ := st.LoadMeta(ctx); idx != 4 {
		t.Fatalf("meta index = %d, want 4", idx)
	}

	// 幂等：再次删除空 Namespace。
	res, err = st.ApplyDeleteNamespace(ctx, raftlog.DeleteNamespaceData{Namespace: ns}, time.Now(), 5, 1)
	if err != nil || res.Status != raftlog.StatusOK || res.Entries != 0 {
		t.Fatalf("重复删除 = %+v %v, want ok entries=0", res, err)
	}

	// 可回滚：对 a 回滚到墓碑之前的 v1，条目重建。
	res, err = st.ApplyRollback(ctx, rb(ns, "a", 1), time.Now(), 6, 1)
	if err != nil || res.Status != raftlog.StatusOK || !res.Created {
		t.Fatalf("删除后回滚重建 = %+v %v, want ok created v4", res, err)
	}
	entry, err := st.GetEntry(ctx, ns, "a")
	if err != nil || entry == nil || entry.Content != "v1" || entry.Version != 4 {
		t.Fatalf("重建条目 = %+v %v, want v1 @ v4", entry, err)
	}
}
