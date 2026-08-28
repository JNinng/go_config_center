// 快照导出/导入（DESIGN §4.4、ADR-0007）：
// 单文件自描述流 = [magic 8B "GCCSNP01"][JSON 头一行][三表 COPY 数据体]。
// Restore 按本机 raft_fsm_meta 进度决定跳过或全量导入；全量导入单事务提交，
// checksum/行数/截断任一不符即整体回滚，不产生半写状态。
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

// snapshotTables 固定数据体中三表的顺序，导出与导入必须一致。
var snapshotTables = []string{"biz_configs", "biz_config_history", "raft_fsm_meta"}

// tableSection 记录单表分节的行数与字节长度；字节长度让 Restore 无需解析行边界。
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

// pgSnapshot 实现 raft.FSMSnapshot：Persist 把 spool 好的数据体写入快照存储的 sink。
type pgSnapshot struct {
	header     snapshotHeader
	spoolPaths []string
	startedAt  time.Time
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
	var total int64
	for _, path := range p.spoolPaths {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("打开分节 spool: %w", err)
		}
		n, err := io.Copy(sink, f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("写入分节: %w", err)
		}
		total += n
	}
	metrics.snapshotDuration.Observe(time.Since(p.startedAt).Seconds())
	metrics.snapshotSize.Set(float64(total))
	return nil
}

func (p *pgSnapshot) Release() {
	for _, path := range p.spoolPaths {
		_ = os.Remove(path)
	}
}

// Snapshot 在单个 REPEATABLE READ 事务内导出三表（ADR-0007：快照点 = 该事务内
// 读到的 raft_fsm_meta）。头中的 checksum 覆盖整个数据体，因此先 spool 到临时文件
// 计算完成后再由 Persist 写出。
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()

	tx, err := f.store.Pool().Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("开启导出事务: %w", err)
	}
	committed := false
	defer store.RollbackUnlessCommitted(ctx, tx, &committed)()

	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, fmt.Errorf("设置隔离级别: %w", err)
	}

	var index, term int64
	if err := tx.QueryRow(ctx,
		`SELECT last_applied_index, last_applied_term FROM raft_fsm_meta WHERE id = 1`).
		Scan(&index, &term); err != nil {
		return nil, fmt.Errorf("读取快照点: %w", err)
	}

	hasher := sha256.New()
	header := snapshotHeader{
		FormatVersion: snapshotFormatVersion,
		RaftIndex:     uint64(index),
		RaftTerm:      uint64(term),
		Tables:        make(map[string]tableSection, len(snapshotTables)),
	}
	var spools []string
	for _, table := range snapshotTables {
		spool, err := os.CreateTemp("", "gcc-snapshot-*.tmp")
		if err != nil {
			releaseSpools(spools)
			return nil, fmt.Errorf("创建分节 spool: %w", err)
		}
		tag, copyErr := tx.Conn().PgConn().CopyTo(ctx,
			io.MultiWriter(spool, hasher), "COPY "+table+" TO STDOUT")
		size, seekErr := spool.Seek(0, io.SeekCurrent)
		_ = spool.Close()
		if copyErr != nil {
			_ = os.Remove(spool.Name())
			releaseSpools(spools)
			return nil, fmt.Errorf("导出 %s: %w", table, copyErr)
		}
		if seekErr != nil {
			_ = os.Remove(spool.Name())
			releaseSpools(spools)
			return nil, fmt.Errorf("统计 %s 分节长度: %w", table, seekErr)
		}
		header.Tables[table] = tableSection{Rows: tag.RowsAffected(), Bytes: size}
		spools = append(spools, spool.Name())
	}
	if err := tx.Commit(ctx); err != nil {
		releaseSpools(spools)
		return nil, fmt.Errorf("提交导出事务: %w", err)
	}
	committed = true
	header.Checksum = hex.EncodeToString(hasher.Sum(nil))

	f.log.Info("快照导出完成",
		"raft_index", header.RaftIndex, "raft_term", header.RaftTerm,
		"tables", fmt.Sprint(header.Tables))
	return &pgSnapshot{header: header, spoolPaths: spools, startedAt: startedAt}, nil
}

// releaseSpools 清理已创建的分节临时文件（导出中断时）。
func releaseSpools(paths []string) {
	for _, p := range paths {
		_ = os.Remove(p)
	}
}

// Restore 先读头与本机进度比对：已追平或超前 → 跳过导入仅消费流；
// 否则置 Serving 门闩为 false，单事务 TRUNCATE + COPY 导入 + 修正序列，
// checksum 与行数校验通过才提交；任一不符整体回滚并显式返回错误。
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

	tx, err := f.store.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启导入事务: %w", err)
	}
	committed := false
	defer store.RollbackUnlessCommitted(ctx, tx, &committed)()

	if _, err := tx.Exec(ctx, `TRUNCATE biz_configs, biz_config_history, raft_fsm_meta`); err != nil {
		return fmt.Errorf("清空三表: %w", err)
	}

	// checksum 覆盖整个数据体：边导入边哈希，提交前比对。
	hasher := sha256.New()
	tee := io.TeeReader(br, hasher)
	for _, table := range snapshotTables {
		section := header.Tables[table]
		tag, err := tx.Conn().PgConn().CopyFrom(ctx,
			io.LimitReader(tee, section.Bytes), "COPY "+table+" FROM STDIN")
		if err != nil {
			return fmt.Errorf("导入 %s: %w", table, err)
		}
		if tag.RowsAffected() != section.Rows {
			return fmt.Errorf("导入 %s 行数不符：导入 %d，头声明 %d",
				table, tag.RowsAffected(), section.Rows)
		}
	}
	if got := hex.EncodeToString(hasher.Sum(nil)); got != header.Checksum {
		return fmt.Errorf("checksum 不符：数据体可能损坏")
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return fmt.Errorf("数据体之后存在多余字节")
	}
	// COPY 携带显式 id 值，序列必须修正到 MAX(id)，否则后续 History 追加撞主键。
	for _, table := range []string{"biz_configs", "biz_config_history"} {
		if _, err := tx.Exec(ctx, fmt.Sprintf(
			`SELECT setval(pg_get_serial_sequence('%s', 'id'),
			 COALESCE((SELECT MAX(id) FROM %s), 0) + 1, false)`, table, table)); err != nil {
			return fmt.Errorf("修正 %s 序列: %w", table, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("提交导入事务: %w", err)
	}
	committed = true

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
	for _, table := range snapshotTables {
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
