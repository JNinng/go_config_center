// 快照导出/导入（DESIGN §4.4、ADR-0007）：
// 单文件自描述流 = [magic 8B "GCCSNP01"][JSON 头一行][数据体]。
// 本文件负责格式骨架：magic、JSON 头、checksum 记账、跳过式恢复判定与 Serving 门闩；
// 数据体的分节编码与导入原子性由存储后端实现（store.Store.ExportTables/ImportTables）。
package fsm

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/hashicorp/raft"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
)

const (
	snapshotMagic         = "GCCSNP01"
	snapshotFormatVersion = 1
	// snapshotTimeout 限制导出/导入事务时长；量级假设 ≤10 万条时本地 PG 为秒级。
	snapshotTimeout = 120 * time.Second
)

// tableSection 记录快照头中单表分节的行数与字节长度；字节长度让 Restore 无需解析行边界。
type tableSection struct {
	Rows  int64 `json:"rows"`
	Bytes int64 `json:"bytes"`
}

type snapshotHeader struct {
	FormatVersion int                     `json:"format_version"`
	RaftIndex     uint64                  `json:"raft_index"`
	RaftTerm      uint64                  `json:"raft_term"`
	Tables        map[string]tableSection `json:"tables"`
	Checksum      string                  `json:"checksum"` // 数据体 sha256（hex）
}

// pgSnapshot 实现 raft.FSMSnapshot：Persist 把 spool 好数据体写入快照存储的 sink。
type pgSnapshot struct {
	header    snapshotHeader
	spoolPath string
	startedAt time.Time
}

func (p *pgSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write([]byte(snapshotMagic)); err != nil {
		return fmt.Errorf("写入 magic: %w", err)
	}
	hdr, err := json.Marshal(p.header)
	if err != nil {
		return fmt.Errorf("序列化快照头: %w", err)
	}
	if _, err := sink.Write(append(hdr, '\n')); err != nil {
		return fmt.Errorf("写入快照头: %w", err)
	}
	f, err := os.Open(p.spoolPath)
	if err != nil {
		return fmt.Errorf("打开数据体 spool: %w", err)
	}
	_, err = io.Copy(sink, f)
	_ = f.Close()
	if err != nil {
		return fmt.Errorf("写入数据体: %w", err)
	}
	metrics.snapshotDuration.Observe(time.Since(p.startedAt).Seconds())
	metrics.snapshotSize.Set(float64(p.header.totalBytes()))
	return nil
}

func (p *pgSnapshot) Release() {
	_ = os.Remove(p.spoolPath)
}

// Snapshot 经由存储后端在单个一致性读事务内导出三表（ADR-0007：快照点 = 该事务内
// 读到的 raft_fsm_meta）。数据体先 spool 到临时文件并计算 checksum，再由 Persist
// 按 [magic][JSON 头][数据体] 顺序写出。
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	spool, err := os.CreateTemp("", "gcc-snapshot-*.tmp")
	if err != nil {
		return nil, fmt.Errorf("创建数据体 spool: %w", err)
	}
	hasher := sha256.New()
	point, sections, err := f.store.ExportTables(ctx, io.MultiWriter(spool, hasher))
	if err != nil {
		_ = spool.Close()
		_ = os.Remove(spool.Name())
		return nil, err
	}
	size, seekErr := spool.Seek(0, io.SeekCurrent)
	if closeErr := spool.Close(); seekErr == nil {
		seekErr = closeErr
	}
	if seekErr != nil {
		_ = os.Remove(spool.Name())
		return nil, fmt.Errorf("统计数据体长度: %w", seekErr)
	}
	if len(sections) != len(store.SnapshotTables) {
		_ = os.Remove(spool.Name())
		return nil, fmt.Errorf("后端返回分节数 %d 与快照格式不符（期望 %d）",
			len(sections), len(store.SnapshotTables))
	}

	header := snapshotHeader{
		FormatVersion: snapshotFormatVersion,
		RaftIndex:     point.Index,
		RaftTerm:      point.Term,
		Checksum:      hex.EncodeToString(hasher.Sum(nil)),
		Tables:        make(map[string]tableSection, len(store.SnapshotTables)),
	}
	for i, table := range store.SnapshotTables {
		header.Tables[table] = tableSection{Rows: sections[i].Rows, Bytes: sections[i].Bytes}
	}
	if int64(size) != header.totalBytes() {
		_ = os.Remove(spool.Name())
		return nil, fmt.Errorf("数据体 %d 字节与分节声明合计 %d 不符", size, header.totalBytes())
	}

	f.log.Info("快照导出完成",
		"raft_index", header.RaftIndex, "raft_term", header.RaftTerm,
		"tables", fmt.Sprint(header.Tables))
	return &pgSnapshot{header: header, spoolPath: spool.Name(), startedAt: startedAt}, nil
}

// totalBytes 返回快照头声明的数据体合计字节数。
func (h snapshotHeader) totalBytes() int64 {
	var total int64
	for _, tbl := range store.SnapshotTables {
		total += h.Tables[tbl].Bytes
	}
	return total
}

// Restore 先读头与本机进度比对：已追平或超前 → 跳过导入仅消费流；
// 否则置 Serving 门闩为 false，交由存储后端单事务全量导入（行数/checksum/多余字节
// 校验通过才提交，任一不符整体回滚并显式返回错误）。
func (f *FSM) Restore(rc io.ReadCloser) error {
	header, br, err := readSnapshotHeader(rc)
	if err != nil {
		return err
	}

	f.mu.Lock()
	local := f.lastIndex
	f.mu.Unlock()
	if header.RaftIndex <= local {
		_, _ = io.Copy(io.Discard, br)
		metrics.restoreSkipped.Inc()
		f.log.Info("恢复快照：本机已追平或超前，跳过数据导入",
			"snapshot_index", header.RaftIndex, "local_last_applied", local)
		return nil
	}

	f.serving.Store(false)
	defer f.serving.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	sections := make([]store.SnapshotSection, 0, len(store.SnapshotTables))
	for _, table := range store.SnapshotTables {
		sec := header.Tables[table]
		sections = append(sections, store.SnapshotSection{Rows: sec.Rows, Bytes: sec.Bytes})
	}
	if err := f.store.ImportTables(ctx, br, sections, header.Checksum); err != nil {
		return err
	}

	f.mu.Lock()
	f.lastIndex = header.RaftIndex
	f.lastTerm = header.RaftTerm
	f.results = make(map[uint64]raftlog.Result)
	f.mu.Unlock()
	metrics.lastApplied.Set(float64(header.RaftIndex))
	f.log.Info("恢复快照：全量导入完成",
		"raft_index", header.RaftIndex, "raft_term", header.RaftTerm)
	return nil
}

// readSnapshotHeader 校验 magic 与格式版本，解析 JSON 头；
// 返回的 bufio.Reader 已缓冲头之后的数据体首段。
func readSnapshotHeader(r io.Reader) (snapshotHeader, *bufio.Reader, error) {
	br := bufio.NewReader(r)
	var magic [8]byte
	if _, err := io.ReadFull(br, magic[:]); err != nil {
		return snapshotHeader{}, nil, fmt.Errorf("读取 magic: %w", err)
	}
	if string(magic[:]) != snapshotMagic {
		return snapshotHeader{}, nil, fmt.Errorf("magic 不符：期望 %q，得到 %q", snapshotMagic, magic)
	}
	line, err := br.ReadBytes('\n')
	if err != nil {
		return snapshotHeader{}, nil, fmt.Errorf("读取快照头: %w", err)
	}
	var h snapshotHeader
	if err := json.Unmarshal(line, &h); err != nil {
		return snapshotHeader{}, nil, fmt.Errorf("解析快照头: %w", err)
	}
	if h.FormatVersion != snapshotFormatVersion {
		return snapshotHeader{}, nil, fmt.Errorf("不支持的快照格式版本 %d", h.FormatVersion)
	}
	for _, table := range store.SnapshotTables {
		if _, ok := h.Tables[table]; !ok {
			return snapshotHeader{}, nil, fmt.Errorf("快照头缺少分节 %s", table)
		}
	}
	if h.Checksum == "" {
		return snapshotHeader{}, nil, fmt.Errorf("快照头缺少 checksum")
	}
	return h, br, nil
}

// Serving 返回 Serving 门闩（全量恢复期间为 false，读 API 应 503）。
func (f *FSM) Serving() bool {
	return f.serving.Load()
}
