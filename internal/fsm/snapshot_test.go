package fsm

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
	"github.com/jninng/go_config_center/internal/testutil"
	ptestutil "github.com/prometheus/client_golang/prometheus/testutil"
)

// seedAndSnapshot 在全新 FSM 上应用 3 条 publish（ns: a×2 版本、b×1 版本）并生成快照字节流。
func seedAndSnapshot(t *testing.T) (*FSM, *store.Store, []byte) {
	t.Helper()
	f, st, _ := mustFSM(t)
	apply(t, f, publishLog(t, 1, 1, t.Name(), "a", "v1", nil))
	apply(t, f, publishLog(t, 2, 1, t.Name(), "a", "v2", nil))
	apply(t, f, publishLog(t, 3, 1, t.Name(), "b", "v1", nil))

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()
	return f, st, sink.Bytes()
}

// memSink 是 raft.SnapshotSink 的内存实现（仅测试用）。
type memSink struct{ bytes.Buffer }

func (m *memSink) ID() string    { return "mem" }
func (m *memSink) Cancel() error { return nil }
func (m *memSink) Close() error  { return nil }

// 验收项：快照文件含合法头（format_version、raft index/term、各表行数、checksum）与三表 COPY 数据。
func TestSnapshotProducesValidStream(t *testing.T) {
	_, _, buf := seedAndSnapshot(t)

	if !bytes.HasPrefix(buf, []byte(snapshotMagic)) {
		t.Fatalf("快照应以 magic %q 开头", snapshotMagic)
	}
	hdr, bodyOff, err := parseSnapshotHeader(buf)
	if err != nil {
		t.Fatalf("parseSnapshotHeader: %v", err)
	}
	if hdr.FormatVersion != snapshotFormatVersion {
		t.Fatalf("format_version = %d, want %d", hdr.FormatVersion, snapshotFormatVersion)
	}
	if hdr.RaftIndex != 3 || hdr.RaftTerm != 1 {
		t.Fatalf("raft index/term = (%d,%d), want (3,1)", hdr.RaftIndex, hdr.RaftTerm)
	}
	if hdr.Tables["biz_configs"].Rows != 2 || hdr.Tables["biz_config_history"].Rows != 3 || hdr.Tables["raft_fsm_meta"].Rows != 1 {
		t.Fatalf("各表行数 = %+v, want configs=2 history=3 meta=1", hdr.Tables)
	}

	body := buf[bodyOff:]
	var want int64
	for _, tbl := range snapshotTables {
		want += hdr.Tables[tbl].Bytes
	}
	if int64(len(body)) != want {
		t.Fatalf("数据体 %d 字节, 头声明合计 %d", len(body), want)
	}
	sum := sha256.Sum256(body)
	if hdr.Checksum != hex.EncodeToString(sum[:]) {
		t.Fatal("checksum 与数据体 sha256 不符")
	}
}

// 验收项：带数据重启走跳过路径——本机进度已追平时 Restore 跳过导入，数据不变。
func TestRestoreSkipsWhenMetaCaughtUp(t *testing.T) {
	f, _, buf := seedAndSnapshot(t)

	before := ptestutil.ToFloat64(metrics.restoreSkipped)
	if err := f.Restore(io.NopCloser(bytes.NewReader(buf))); err != nil {
		t.Fatalf("跳过路径 Restore 不应失败: %v", err)
	}
	if after := ptestutil.ToFloat64(metrics.restoreSkipped); after <= before {
		t.Fatalf("restore_skipped_total 应递增：before=%v after=%v", before, after)
	}
	// 数据体未被导入路径触碰：history 仍为 3 行、进度不变。
	idx, term := f.LastApplied()
	if idx != 3 || term != 1 {
		t.Fatalf("进度 = (%d,%d), want (3,1)", idx, term)
	}
	if n := historyRows(t, f); n != 3 {
		t.Fatalf("history 行数 = %d, want 3（跳过不得改写数据）", n)
	}
}

// 验收项：清空 PG 后重启走全量导入——落后节点从快照完整恢复并可继续 Apply。
func TestRestoreFullImportWhenBehind(t *testing.T) {
	_, _, buf := seedAndSnapshot(t)

	dsn := testutil.NewDB(t, "gcc_fsm_import")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	f, err := New(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.Restore(io.NopCloser(bytes.NewReader(buf))); err != nil {
		t.Fatalf("全量导入 Restore: %v", err)
	}

	entry, err := st.GetEntry(context.Background(), t.Name(), "a")
	if err != nil || entry == nil || entry.Version != 2 || entry.Content != "v2" {
		t.Fatalf("导入后条目 a = %+v err=%v, want v2", entry, err)
	}
	if entry, _ = st.GetEntry(context.Background(), t.Name(), "b"); entry == nil || entry.Version != 1 {
		t.Fatalf("导入后条目 b = %+v", entry)
	}
	idx, term := f.LastApplied()
	if idx != 3 || term != 1 {
		t.Fatalf("导入后内存进度 = (%d,%d), want (3,1)", idx, term)
	}
	idx, term, err = st.LoadMeta(context.Background())
	if err != nil || idx != 3 || term != 1 {
		t.Fatalf("导入后 raft_fsm_meta = (%d,%d,%v), want (3,1)", idx, term, err)
	}

	// 导入后继续 Apply 新日志：序列已修正，History 追加不得撞主键。
	res := apply(t, f, publishLog(t, 4, 1, t.Name(), "a", "v3", nil))
	if res.Status != raftlog.StatusOK || res.Version != 3 {
		t.Fatalf("导入后继续 Publish = %+v, want ok v3", res)
	}
}

// 验收项：checksum 或 magic 不符时 Restore 显式失败，不产生半写状态。
func TestRestoreRejectsCorruption(t *testing.T) {
	makeFreshFSM := func(t *testing.T) *FSM {
		dsn := testutil.NewDB(t, "gcc_fsm_corrupt")
		st, err := store.Open(context.Background(), dsn)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Migrate(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(st.Close)
		f, err := New(context.Background(), st)
		if err != nil {
			t.Fatal(err)
		}
		return f
	}

	_, _, buf := seedAndSnapshot(t)

	t.Run("数据体损坏", func(t *testing.T) {
		f := makeFreshFSM(t)
		corrupt := bytes.Clone(buf)
		_, hdrEnd, err := parseSnapshotHeader(corrupt)
		if err != nil {
			t.Fatal(err)
		}
		corrupt[hdrEnd+(len(corrupt)-hdrEnd)/2] ^= 0xFF // 翻转数据体中部一字节，破坏 sha256 校验
		if err := f.Restore(io.NopCloser(bytes.NewReader(corrupt))); err == nil {
			t.Fatal("checksum 不符应显式失败")
		}
		if n := historyRows(t, f); n != 0 {
			t.Fatalf("失败后 history 行数 = %d, want 0（事务必须整体回滚）", n)
		}
		if !f.Serving() {
			t.Fatal("失败的 Restore 结束后 Serving 门闩应恢复 true")
		}
	})

	t.Run("magic 损坏", func(t *testing.T) {
		f := makeFreshFSM(t)
		corrupt := bytes.Clone(buf)
		corrupt[0] = 'X'
		if err := f.Restore(io.NopCloser(bytes.NewReader(corrupt))); err == nil {
			t.Fatal("magic 不符应显式失败")
		}
	})

	t.Run("截断的数据体", func(t *testing.T) {
		f := makeFreshFSM(t)
		if err := f.Restore(io.NopCloser(bytes.NewReader(buf[:len(buf)-10]))); err == nil {
			t.Fatal("截断流应显式失败")
		}
		if n := historyRows(t, f); n != 0 {
			t.Fatalf("失败后 history 行数 = %d, want 0", n)
		}
	})
}

// 验收项：全量恢复期间 Serving 门闩关闭（读 API 503 的依据），完成后恢复。
func TestRestoreServingLatchDuringImport(t *testing.T) {
	_, _, buf := seedAndSnapshot(t)

	dsn := testutil.NewDB(t, "gcc_fsm_latch")
	st, err := store.Open(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	f, err := New(context.Background(), st)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Serving() {
		t.Fatal("初始状态应 Serving")
	}

	// 在数据体中段阻塞读取，制造可观测的恢复中窗口。
	_, hdrEnd, err := parseSnapshotHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	blockAt := int64(hdrEnd + 16)
	var served atomic.Bool
	r := &blockingReader{src: bytes.NewReader(buf), blockAt: blockAt, entered: &served}

	done := make(chan error, 1)
	go func() { done <- f.Restore(io.NopCloser(r)) }()

	deadline := time.Now().Add(3 * time.Second)
	for !served.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !served.Load() {
		t.Fatal("读取未进入阻塞区")
	}
	// 阻塞期间门闩必须已关闭（导入尚未完成）。
	if f.Serving() {
		t.Fatal("全量导入期间 Serving 应为 false")
	}
	r.release()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Restore: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Restore 未在期限内完成")
	}
	if !f.Serving() {
		t.Fatal("导入完成后 Serving 应恢复 true")
	}
	if n := historyRows(t, f); n != 3 {
		t.Fatalf("导入后 history = %d, want 3", n)
	}
}

// blockingReader 在读到 blockAt 字节后阻塞，直到 release。
type blockingReader struct {
	src       *bytes.Reader
	blockAt   int64
	entered   *atomic.Bool
	releaseCh chan struct{}
	once      atomic.Bool
	n         int64
}

func (b *blockingReader) Read(p []byte) (int, error) {
	if b.n >= b.blockAt {
		b.entered.Store(true)
		if b.once.CompareAndSwap(false, true) {
			b.releaseCh = make(chan struct{})
		}
		<-b.releaseCh
	}
	m, err := b.src.Read(p)
	b.n += int64(m)
	return m, err
}

func (b *blockingReader) release() {
	if ch := b.releaseCh; ch != nil {
		close(ch)
	}
}

func historyRows(t *testing.T, f *FSM) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var n int64
	if err := f.store.Pool().QueryRow(ctx, "SELECT count(*) FROM biz_config_history").Scan(&n); err != nil {
		t.Fatalf("historyRows: %v", err)
	}
	return n
}

// parseSnapshotHeader 断言快照字节流的头；返回头与数据体起始偏移。
func parseSnapshotHeader(buf []byte) (snapshotHeader, int, error) {
	h, br, err := readSnapshotHeader(bytes.NewReader(buf))
	if err != nil {
		return snapshotHeader{}, 0, err
	}
	return h, len(buf) - br.Buffered(), nil
}
