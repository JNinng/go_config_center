// FSM 幂等性属性测试（DESIGN §8.1）：随机化的 publish/delete/rollback 混合日志序列
// 在重复投递、重启重放、快照/Restore 交错下，PG 终态与逐条结果必须逐次一致——
// 这是跳过式恢复（ADR-0007）正确性的直接证明。
package fsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/jackc/pgx/v5"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
)

// logSpec 是生成后固定的日志序列元素：同一 seed 的所有运行共用同一序列。
type logSpec struct {
	index, term uint64
	data        []byte
}

func genLogs(t *testing.T, seed int64, count int) []logSpec {
	rng := rand.New(rand.NewSource(seed))
	namespaces := []string{"dev", "prod", "team-a"}
	dataIds := []string{"db.password", "app.yaml", "feature.flag", "retry.policy", "quota"}

	specs := make([]logSpec, 0, count)
	for i := 1; i <= count; i++ {
		ns := namespaces[rng.Intn(len(namespaces))]
		id := dataIds[rng.Intn(len(dataIds))]
		var (
			cmd raftlog.Command
			err error
		)
		switch op := rng.Intn(10); {
		case op < 5: // publish：含 CAS 冲突项
			var expected *int64
			switch rng.Intn(10) {
			case 0:
				zero := int64(0)
				expected = &zero
			case 1, 2:
				e := int64(rng.Intn(7)) // 大概率与当前不匹配
				expected = &e
			}
			tags := map[string]string{}
			if rng.Intn(2) == 0 {
				tags["gen"] = fmt.Sprintf("s%d", rng.Intn(3))
			}
			cmd, err = raftlog.NewPublish(raftlog.PublishData{
				Namespace: ns, DataId: id, DataType: "TEXT",
				Content:         fmt.Sprintf("content-%d-%d", seed, i),
				Tags:            tags,
				ExpectedVersion: expected,
			})
		case op < 8: // delete
			var expected *int64
			if rng.Intn(2) == 0 {
				e := int64(rng.Intn(5))
				expected = &e
			}
			cmd, err = raftlog.NewDelete(raftlog.DeleteData{
				Namespace: ns, DataId: id, ExpectedVersion: expected,
			})
		default: // rollback：目标随机，含大量无效项
			cmd, err = raftlog.NewRollback(raftlog.RollbackData{
				Namespace: ns, DataId: id,
				ToVersion: int64(1 + rng.Intn(8)),
			})
		}
		if err != nil {
			t.Fatalf("生成日志 %d: %v", i, err)
		}
		b, err := raftlog.Encode(cmd)
		if err != nil {
			t.Fatal(err)
		}
		specs = append(specs, logSpec{index: uint64(i), term: uint64(1 + i/20), data: b})
	}
	return specs
}

func toRaftLogs(specs []logSpec) []*raft.Log {
	logs := make([]*raft.Log, len(specs))
	for i, s := range specs {
		logs[i] = &raft.Log{Index: s.index, Term: s.term, Type: raft.LogCommand, Data: s.data}
	}
	return logs
}

// snapshotState 摘取与运行无关的 PG 终态（排除自增 id 与时间戳）。
func snapshotState(t *testing.T, dsn string) []string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("连接摘要库: %v", err)
	}
	defer conn.Close(ctx)

	var lines []string
	rows, err := conn.Query(ctx, `
		SELECT namespace, data_id, data_type, content, version, COALESCE(tags::text, '')
		FROM biz_configs ORDER BY namespace, data_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ns, id, dt, content, tags string
		var version int64
		if err := rows.Scan(&ns, &id, &dt, &content, &version, &tags); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("C|%s|%s|%s|%s|%d|%s", ns, id, dt, content, version, tags))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	rows, err = conn.Query(ctx, `
		SELECT namespace, data_id, version, op, COALESCE(data_type, ''), COALESCE(content, ''),
		       COALESCE(tags::text, ''), COALESCE(actor, '')
		FROM biz_config_history ORDER BY namespace, data_id, version`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var ns, id, dt, content, tags, actor string
		var version int64
		var op string
		if err := rows.Scan(&ns, &id, &version, &op, &dt, &content, &tags, &actor); err != nil {
			t.Fatal(err)
		}
		lines = append(lines, fmt.Sprintf("H|%s|%s|%d|%s|%s|%s|%s|%s", ns, id, version, op, dt, content, tags, actor))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	var idx, term int64
	if err := conn.QueryRow(ctx,
		`SELECT last_applied_index, last_applied_term FROM raft_fsm_meta WHERE id = 1`).Scan(&idx, &term); err != nil {
		t.Fatal(err)
	}
	lines = append(lines, fmt.Sprintf("M|%d|%d", idx, term))
	return lines
}

// resultKey 归一化一条 Apply 结果，用于跨运行比对。
func resultKey(res raftlog.Result) string {
	return fmt.Sprintf("%s|%d|%d", res.Status, res.Version, res.CurrentVersion)
}

func digestLines(lines []string) string {
	h := sha256.Sum256([]byte(fmt.Sprint(lines)))
	return hex.EncodeToString(h[:])
}

// memSink 复用 snapshot_test.go 中的实现。

// mustNewFSM 在一次性测试库上构造 FSM。
func mustNewFSM(t *testing.T) (*FSM, *store.Store, string) {
	t.Helper()
	dsn := testutil.NewDB(t, "gcc_prop_test")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("store.Open: %v", err)
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

// takeSnapshot 驱动 FSM 完成快照导出与落盘序列化，返回快照字节流。
func takeSnapshot(t *testing.T, f *FSM) []byte {
	t.Helper()
	fsms, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	t.Cleanup(fsms.Release)
	sink := &memSink{}
	if err := fsms.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	return sink.Bytes()
}

// 验收：多种子随机序列在重复投递、重启重放、快照/Restore 交错下终态与结果逐次一致。
func TestFSMPropertyReplayDeterminism(t *testing.T) {
	const opsCount = 60
	half := opsCount / 2

	for _, seed := range []int64{1, 2, 3} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			specs := genLogs(t, seed, opsCount)
			logs := toRaftLogs(specs)

			// 基准运行：顺序应用全部日志。
			ref, _, refDSN := mustNewFSM(t)
			refResults := make(map[uint64]raftlog.Result, opsCount)
			for _, l := range logs {
				refResults[l.Index] = apply(t, ref, l)
			}
			refDigest := digestLines(snapshotState(t, refDSN))

			// 运行 A：同序列重复投递（Raft 可能向同一 FSM 重复投递已应用日志）。
			fA, _, dsnA := mustNewFSM(t)
			for _, l := range logs {
				first := apply(t, fA, l)
				dup := apply(t, fA, l)
				if first.Status == raftlog.StatusReplayed || first.Status == raftlog.StatusError {
					t.Fatalf("首次应用不应为 %s（index=%d）", first.Status, l.Index)
				}
				if resultKey(dup) != resultKey(first) {
					t.Fatalf("重复投递 index=%d 结果漂移: %v vs %v", l.Index, first, dup)
				}
				if resultKey(first) != resultKey(refResults[l.Index]) {
					t.Fatalf("运行 A index=%d 结果与基准不一致: %v vs %v", l.Index, first, refResults[l.Index])
				}
			}
			if got := digestLines(snapshotState(t, dsnA)); got != refDigest {
				t.Fatalf("运行 A 终态与基准不一致")
			}

			// 运行 B：中途"重启"——新 FSM 实例重放全部日志（≤ last_applied 的必须跳过），
			// 再应用后半段，终态与结果必须与基准一致。
			fB, stB, dsnB := mustNewFSM(t)
			for i := 0; i < half; i++ {
				apply(t, fB, logs[i])
			}
			fB2, err := New(context.Background(), stB, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < half; i++ {
				if res := apply(t, fB2, logs[i]); res.Status != raftlog.StatusReplayed {
					t.Fatalf("重启重放 index=%d = %s, want replayed", logs[i].Index, res.Status)
				}
			}
			for i := half; i < opsCount; i++ {
				res := apply(t, fB2, logs[i])
				if resultKey(res) != resultKey(refResults[logs[i].Index]) {
					t.Fatalf("运行 B index=%d 结果与基准不一致: %v vs %v", logs[i].Index, res, refResults[logs[i].Index])
				}
			}
			if got := digestLines(snapshotState(t, dsnB)); got != refDigest {
				t.Fatalf("运行 B 终态与基准不一致")
			}

			// 运行 C：快照与 Restore 交错——前半段后取快照，导入全新 PG（全量路径），
			// 再应用后半段；随后同一快照二次 Restore 必须命中跳过路径且终态不变。
			fC, _, dsnC := mustNewFSM(t)
			for i := 0; i < half; i++ {
				apply(t, fC, logs[i])
			}
			snap := takeSnapshot(t, fC)

			fD, _, dsnD := mustNewFSM(t)
			if err := fD.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
				t.Fatalf("Restore（全量）: %v", err)
			}
			importedDigest := digestLines(snapshotState(t, dsnD))
			midDigest := digestLines(snapshotState(t, dsnC))
			if importedDigest != midDigest {
				t.Fatalf("快照导入后终态与快照点不一致")
			}
			if err := fD.Restore(io.NopCloser(bytes.NewReader(snap))); err != nil {
				t.Fatalf("Restore（跳过）: %v", err)
			}
			if got := digestLines(snapshotState(t, dsnD)); got != importedDigest {
				t.Fatalf("跳过式 Restore 改变了终态")
			}
			for i := half; i < opsCount; i++ {
				res := apply(t, fD, logs[i])
				if resultKey(res) != resultKey(refResults[logs[i].Index]) {
					t.Fatalf("运行 C index=%d 结果与基准不一致: %v vs %v", logs[i].Index, res, refResults[logs[i].Index])
				}
			}
			if got := digestLines(snapshotState(t, dsnD)); got != refDigest {
				t.Fatalf("运行 C 终态与基准不一致")
			}
		})
	}
}
