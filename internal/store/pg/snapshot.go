// 快照数据体的导出/导入（DESIGN §4.4、ADR-0007）：三表按 store.SnapshotTables 固定顺序
// 的 PG COPY 文本流。导出在单个 REPEATABLE READ 事务内完成，事务内读到的 raft_fsm_meta
// 即快照点；导入单事务 TRUNCATE + COPY，分节行数、整体 checksum、多余字节任一不符即
// 整体回滚，不产生半写状态。快照文件骨架（magic/JSON 头/跳过判定）由 fsm 负责。
package pg

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/jninng/go_config_center/internal/store"
)

// ExportTables 在单个一致性读事务内导出三表：分节按 store.SnapshotTables 顺序连续
// 写入 w，返回该事务读到的 FSM 进度（快照点）与各分节行数/字节长度。
func (s *Store) ExportTables(ctx context.Context, w io.Writer) (store.SnapshotPoint, []store.SnapshotSection, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return store.SnapshotPoint{}, nil, fmt.Errorf("开启导出事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(ctx, tx, &committed)()

	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return store.SnapshotPoint{}, nil, fmt.Errorf("设置隔离级别: %w", err)
	}

	var index, term int64
	if err := tx.QueryRow(ctx,
		`SELECT last_applied_index, last_applied_term FROM raft_fsm_meta WHERE id = 1`).
		Scan(&index, &term); err != nil {
		return store.SnapshotPoint{}, nil, fmt.Errorf("读取快照点: %w", err)
	}
	point := store.SnapshotPoint{Index: uint64(index), Term: uint64(term)}

	sections := make([]store.SnapshotSection, 0, len(store.SnapshotTables))
	for _, table := range store.SnapshotTables {
		cw := &countingWriter{w: w}
		tag, err := tx.Conn().PgConn().CopyTo(ctx, cw, "COPY "+table+" TO STDOUT")
		if err != nil {
			return store.SnapshotPoint{}, nil, fmt.Errorf("导出 %s: %w", table, err)
		}
		sections = append(sections, store.SnapshotSection{Rows: tag.RowsAffected(), Bytes: cw.n})
	}
	if err := tx.Commit(ctx); err != nil {
		return store.SnapshotPoint{}, nil, fmt.Errorf("提交导出事务: %w", err)
	}
	committed = true
	return point, sections, nil
}

// ImportTables 全量导入 ExportTables 产出的数据体：单事务 TRUNCATE 三表 + COPY 导入 +
// 修正序列。各分节实际行数须与 sections 声明一致、数据体 sha256 须等于 checksum、
// 数据体之后不得有多余字节，任一不符整体回滚并返回错误。
func (s *Store) ImportTables(ctx context.Context, r io.Reader, sections []store.SnapshotSection, checksum string) error {
	if len(sections) != len(store.SnapshotTables) {
		return fmt.Errorf("分节数 %d 与快照格式不符（期望 %d）", len(sections), len(store.SnapshotTables))
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("开启导入事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(ctx, tx, &committed)()

	if _, err := tx.Exec(ctx, `TRUNCATE biz_configs, biz_config_history, raft_fsm_meta`); err != nil {
		return fmt.Errorf("清空三表: %w", err)
	}

	// checksum 覆盖整个数据体：边导入边哈希，提交前比对。
	hasher := sha256.New()
	tee := io.TeeReader(r, hasher)
	for i, table := range store.SnapshotTables {
		section := sections[i]
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
	if got := hex.EncodeToString(hasher.Sum(nil)); got != checksum {
		return fmt.Errorf("checksum 不符：数据体可能损坏")
	}
	var probe [1]byte
	if n, err := io.ReadFull(r, probe[:]); n > 0 || !errors.Is(err, io.EOF) {
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
	return nil
}

// countingWriter 透传写入并统计单分节字节数。
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
