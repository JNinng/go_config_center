package store

import (
	"context"
	"testing"

	"github.com/jninng/go_config_center/internal/raftlog"
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

func TestApplyPublishFirstVersionIsOne(t *testing.T) {
	st, _ := mustOpen(t)
	ctx := context.Background()

	res, err := st.ApplyPublish(ctx, pub(t.Name(), "db.password", "s3cret", nil), 1, 1)
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
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), 1, 1); err != nil {
		t.Fatal(err)
	}
	res, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "2", nil), 2, 1)
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
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), 1, 1); err != nil {
		t.Fatal(err)
	}

	res, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "2", &wrong), 2, 1)
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
	res, _ := st.ApplyPublish(ctx, pub(t.Name(), "new", "x", &zero), 1, 1)
	if res.Status != raftlog.StatusOK || res.Version != 1 {
		t.Fatalf("expected=0 create: %+v", res)
	}
	// expected=0 在已存在条目上冲突
	res, _ = st.ApplyPublish(ctx, pub(t.Name(), "new", "y", &zero), 2, 1)
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 1 {
		t.Fatalf("expected=0 on existing: %+v", res)
	}
	// expected=1 匹配当前版本，通过
	res, _ = st.ApplyPublish(ctx, pub(t.Name(), "new", "z", &one), 3, 1)
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
	res, err := st.ApplyPublish(ctx, pub(ns, "a", "reborn", &zero), 1, 1)
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
	res, err = st.ApplyPublish(ctx, pub(ns, "a", "reborn2", &five), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != raftlog.StatusOK || res.Version != 6 || !res.Created {
		t.Fatalf("expected=墓碑版本应通过: %+v, want ok v6 created", res)
	}

	// expected 指向重建前的旧版本 → 冲突，current 为当前存活版本。
	four := int64(4)
	res, _ = st.ApplyPublish(ctx, pub(ns, "a", "stale", &four), 3, 1)
	if res.Status != raftlog.StatusVersionConflict || res.CurrentVersion != 6 {
		t.Fatalf("过期 expected 应冲突: %+v, want conflict current=6", res)
	}
}

// 三表同事务：重开 Store（模拟重启），meta 与业务数据都在。
func TestStateSurvivesStoreReopen(t *testing.T) {
	st, dsn := mustOpen(t)
	ctx := context.Background()
	if _, err := st.ApplyPublish(ctx, pub(t.Name(), "a", "1", nil), 7, 2); err != nil {
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

// Migrate 幂等，可重复执行。
func TestMigrateIdempotent(t *testing.T) {
	st, _ := mustOpen(t)
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("二次 Migrate: %v", err)
	}
}
