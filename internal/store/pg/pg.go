// Package pg 实现 store.Store 的 PostgreSQL 后端（DESIGN §3.1/§3.3）：
// 三表 schema 迁移、FSM 的单事务业务写入、读路径与进度维护。
// pgx 依赖仅允许出现在本包——上游（fsm/api/cmd）只依赖 store.Store 接口（ADR-0011）。
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jninng/go_config_center/internal/raftlog"
	"github.com/jninng/go_config_center/internal/store"
)

// applyTimeout 限制单次 Apply 事务时长，防止 PG 故障卡死 Raft 的 apply 循环；
// 超时则事务回滚、raft_fsm_meta 不推进，进程按 apply-or-die 契约崩溃后重放。
const applyTimeout = 5 * time.Second

const schema = `
CREATE TABLE IF NOT EXISTS biz_configs (
    id          BIGSERIAL PRIMARY KEY,
    namespace   VARCHAR(64)  NOT NULL,
    data_id     VARCHAR(255) NOT NULL,
    data_type   VARCHAR(50)  NOT NULL,
    content     TEXT         NOT NULL,
    version     BIGINT       NOT NULL,
    tags        JSONB,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (namespace, data_id)
);

CREATE TABLE IF NOT EXISTS biz_config_history (
    id          BIGSERIAL PRIMARY KEY,
    namespace   VARCHAR(64)  NOT NULL,
    data_id     VARCHAR(255) NOT NULL,
    version     BIGINT       NOT NULL,
    op          VARCHAR(16)  NOT NULL,
    data_type   VARCHAR(50),
    content     TEXT,
    tags        JSONB,
    actor       VARCHAR(255),
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (namespace, data_id, version)
);

CREATE TABLE IF NOT EXISTS raft_fsm_meta (
    id                  INT  PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_applied_index  BIGINT NOT NULL,
    last_applied_term   BIGINT NOT NULL
);
`

// Store 是 store.Store 的 PostgreSQL 实现。
type Store struct {
	pool *pgxpool.Pool
}

// Open 连接 PostgreSQL（连接池）并返回存储实现。
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("创建 PG 连接池: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("连接 PG: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close 关闭连接池。
func (s *Store) Close() {
	s.pool.Close()
}

// Pool 暴露底层连接池，仅供测试断言与运维排查；业务代码一律经由 store.Store 接口。
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// Ping 探测数据库连通性（/healthz）。
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Migrate 建三表并播种 raft_fsm_meta 单行，幂等。
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, schema); err != nil {
		return fmt.Errorf("迁移 schema: %w", err)
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO raft_fsm_meta (id, last_applied_index, last_applied_term)
		VALUES (1, 0, 0) ON CONFLICT (id) DO NOTHING`)
	if err != nil {
		return fmt.Errorf("播种 raft_fsm_meta: %w", err)
	}
	return nil
}

// LoadMeta 返回 raft_fsm_meta 中的 FSM 进度。
func (s *Store) LoadMeta(ctx context.Context) (index, term uint64, err error) {
	var i, t int64
	err = s.pool.QueryRow(ctx,
		`SELECT last_applied_index, last_applied_term FROM raft_fsm_meta WHERE id = 1`).Scan(&i, &t)
	if err != nil {
		return 0, 0, fmt.Errorf("读取 raft_fsm_meta: %w", err)
	}
	return uint64(i), uint64(t), nil
}

// GetEntry 读当前条目；不存在（或已删除）返回 (nil, nil)。
func (s *Store) GetEntry(ctx context.Context, namespace, dataId string) (*store.Entry, error) {
	var e store.Entry
	var tags []byte
	err := s.pool.QueryRow(ctx, `
		SELECT namespace, data_id, data_type, content, version, tags, created_at, updated_at
		FROM biz_configs WHERE namespace = $1 AND data_id = $2`, namespace, dataId).
		Scan(&e.Namespace, &e.DataId, &e.DataType, &e.Content, &e.Version, &tags, &e.CreatedAt, &e.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取条目: %w", err)
	}
	if tags != nil {
		if err := json.Unmarshal(tags, &e.Tags); err != nil {
			return nil, fmt.Errorf("解析 tags: %w", err)
		}
	}
	return &e, nil
}

// ApplyPublish 在单个 PG 事务内完成 op=publish：
// 读当前版本 → CAS 判定 → upsert biz_configs + 追加 History + 推进 raft_fsm_meta。
// 无论业务结果如何，raft_fsm_meta 都与业务表同事务推进——CAS 失败也是已提交日志。
func (s *Store) ApplyPublish(ctx context.Context, d raftlog.PublishData, index, term uint64) (raftlog.Result, error) {
	tctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()

	tx, err := s.pool.Begin(tctx)
	if err != nil {
		return raftlog.Result{}, fmt.Errorf("开启事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(tctx, tx, &committed)()

	// 当前版本：存活条目优先；行不存在（从未创建或已删除）时取 History 最大版本，
	// 墓碑延续同一版本序列（DESIGN §3.1）。
	var current int64
	var live bool
	err = tx.QueryRow(tctx,
		`SELECT version FROM biz_configs WHERE namespace = $1 AND data_id = $2`,
		d.Namespace, d.DataId).Scan(&current)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		live = false
		err = tx.QueryRow(tctx,
			`SELECT COALESCE(MAX(version), 0) FROM biz_config_history
			 WHERE namespace = $1 AND data_id = $2`,
			d.Namespace, d.DataId).Scan(&current)
		if err != nil {
			return raftlog.Result{}, fmt.Errorf("读取 History 版本: %w", err)
		}
	case err != nil:
		return raftlog.Result{}, fmt.Errorf("读取当前版本: %w", err)
	default:
		live = true
	}

	// CAS（DESIGN §4.1）：省略恒通过；0 = 条目不存在或已删除（create-if-absent，
	// 墓碑状态下 current 为墓碑版本但不参与比较）；N>0 = 当前版本恰为 N（含墓碑版本）。
	if d.ExpectedVersion != nil {
		mismatch := false
		if *d.ExpectedVersion == 0 {
			mismatch = live
		} else {
			mismatch = current != *d.ExpectedVersion
		}
		if mismatch {
			return finishRejected(tctx, tx, &committed, index, term, raftlog.Conflict(current))
		}
	}

	version := current + 1
	tags, err := marshalTags(d.Tags)
	if err != nil {
		return raftlog.Result{}, err
	}

	if _, err = tx.Exec(tctx, `
		INSERT INTO biz_configs (namespace, data_id, data_type, content, version, tags)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (namespace, data_id) DO UPDATE SET
			data_type  = EXCLUDED.data_type,
			content    = EXCLUDED.content,
			version    = EXCLUDED.version,
			tags       = EXCLUDED.tags,
			updated_at = now()`,
		d.Namespace, d.DataId, d.DataType, d.Content, version, tags); err != nil {
		return raftlog.Result{}, fmt.Errorf("upsert biz_configs: %w", err)
	}

	if _, err = tx.Exec(tctx, `
		INSERT INTO biz_config_history (namespace, data_id, version, op, data_type, content, tags, actor)
		VALUES ($1, $2, $3, 'publish', $4, $5, $6::jsonb, $7)`,
		d.Namespace, d.DataId, version, d.DataType, d.Content, tags, d.Actor); err != nil {
		return raftlog.Result{}, fmt.Errorf("追加 History: %w", err)
	}

	if err := advanceMeta(tctx, tx, index, term); err != nil {
		return raftlog.Result{}, err
	}
	if err := tx.Commit(tctx); err != nil {
		return raftlog.Result{}, fmt.Errorf("提交: %w", err)
	}
	committed = true
	return raftlog.OK(version, !live), nil
}

// ApplyDelete 在单个 PG 事务内完成 op=delete（DESIGN §4.2）：删除 biz_configs 行 +
// 追加 content 为 NULL 的 Tombstone（ADR-0010，版本号照常消耗）+ 推进 raft_fsm_meta。
// 条目不存在或已删除返回 not_found（无物可删）；CAS 不匹配返回 conflict——
// 二者都是已提交日志，进度同事务推进，不追加 History。
func (s *Store) ApplyDelete(ctx context.Context, d raftlog.DeleteData, index, term uint64) (raftlog.Result, error) {
	tctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()

	tx, err := s.pool.Begin(tctx)
	if err != nil {
		return raftlog.Result{}, fmt.Errorf("开启事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(tctx, tx, &committed)()

	var current int64
	err = tx.QueryRow(tctx,
		`SELECT version FROM biz_configs WHERE namespace = $1 AND data_id = $2`,
		d.Namespace, d.DataId).Scan(&current)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return finishRejected(tctx, tx, &committed, index, term,
			raftlog.Result{Status: raftlog.StatusNotFound})
	case err != nil:
		return raftlog.Result{}, fmt.Errorf("读取当前版本: %w", err)
	}

	// CAS（DESIGN §4.1）：省略恒通过；0 = create-if-absent，与删除互斥——
	// 走到这里条目必然存活，恒冲突；N>0 = 当前版本恰为 N。
	if d.ExpectedVersion != nil && (*d.ExpectedVersion == 0 || current != *d.ExpectedVersion) {
		return finishRejected(tctx, tx, &committed, index, term, raftlog.Conflict(current))
	}

	if _, err = tx.Exec(tctx,
		`DELETE FROM biz_configs WHERE namespace = $1 AND data_id = $2`,
		d.Namespace, d.DataId); err != nil {
		return raftlog.Result{}, fmt.Errorf("删除 biz_configs: %w", err)
	}
	if _, err = tx.Exec(tctx, `
		INSERT INTO biz_config_history (namespace, data_id, version, op, actor)
		VALUES ($1, $2, $3, 'delete', $4)`,
		d.Namespace, d.DataId, current+1, d.Actor); err != nil {
		return raftlog.Result{}, fmt.Errorf("追加 Tombstone: %w", err)
	}

	if err := advanceMeta(tctx, tx, index, term); err != nil {
		return raftlog.Result{}, err
	}
	if err := tx.Commit(tctx); err != nil {
		return raftlog.Result{}, fmt.Errorf("提交: %w", err)
	}
	committed = true
	return raftlog.OK(current+1, false), nil
}

// ApplyRollback 在单个 PG 事务内完成 op=rollback（DESIGN §4.2）：从 History 解析
// to_version 的内容，作为一次 Publish 发布为新版本——从不物理撤销，版本序列延续。
// 目标解析与发布同事务，消除与并发写的竞态（无 TOCTOU）。
// 无效目标：History 中不存在 → not_found；指向 Tombstone 或 ≥ 当前版本 → invalid_rollback。
// 二者都是已提交日志：不追加 History，进度同事务推进。
func (s *Store) ApplyRollback(ctx context.Context, d raftlog.RollbackData, index, term uint64) (raftlog.Result, error) {
	tctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()

	tx, err := s.pool.Begin(tctx)
	if err != nil {
		return raftlog.Result{}, fmt.Errorf("开启事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(tctx, tx, &committed)()

	// 当前版本：存活条目优先；已删除时为墓碑版本——回滚到墓碑之前的任意版本可重建条目。
	var current int64
	var live bool
	err = tx.QueryRow(tctx,
		`SELECT version FROM biz_configs WHERE namespace = $1 AND data_id = $2`,
		d.Namespace, d.DataId).Scan(&current)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		live = false
		err = tx.QueryRow(tctx,
			`SELECT COALESCE(MAX(version), 0) FROM biz_config_history
			 WHERE namespace = $1 AND data_id = $2`,
			d.Namespace, d.DataId).Scan(&current)
		if err != nil {
			return raftlog.Result{}, fmt.Errorf("读取 History 版本: %w", err)
		}
	case err != nil:
		return raftlog.Result{}, fmt.Errorf("读取当前版本: %w", err)
	default:
		live = true
	}

	// 目标校验发生在 Apply 阶段（DESIGN §4.2）：不存在 → 404；墓碑或 ≥ 当前版本 → 409。
	var op string
	var dataType, content *string
	var tags []byte
	err = tx.QueryRow(tctx, `
		SELECT op, data_type, content, tags FROM biz_config_history
		WHERE namespace = $1 AND data_id = $2 AND version = $3`,
		d.Namespace, d.DataId, d.ToVersion).Scan(&op, &dataType, &content, &tags)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return finishRejected(tctx, tx, &committed, index, term,
			raftlog.Result{Status: raftlog.StatusNotFound, Err: "to_version 在 History 中不存在"})
	case err != nil:
		return raftlog.Result{}, fmt.Errorf("读取回滚目标: %w", err)
	}
	if op == "delete" || d.ToVersion >= current {
		return finishRejected(tctx, tx, &committed, index, term,
			raftlog.Result{Status: raftlog.StatusInvalidRollback,
				Err:            "to_version 指向 Tombstone 或不小于当前版本",
				CurrentVersion: current})
	}

	version := current + 1
	var tagsPtr *string
	if tags != nil {
		s := string(tags)
		tagsPtr = &s
	}
	if _, err = tx.Exec(tctx, `
		INSERT INTO biz_configs (namespace, data_id, data_type, content, version, tags)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (namespace, data_id) DO UPDATE SET
			data_type  = EXCLUDED.data_type,
			content    = EXCLUDED.content,
			version    = EXCLUDED.version,
			tags       = EXCLUDED.tags,
			updated_at = now()`,
		d.Namespace, d.DataId, *dataType, *content, version, tagsPtr); err != nil {
		return raftlog.Result{}, fmt.Errorf("upsert biz_configs: %w", err)
	}
	if _, err = tx.Exec(tctx, `
		INSERT INTO biz_config_history (namespace, data_id, version, op, data_type, content, tags, actor)
		VALUES ($1, $2, $3, 'publish', $4, $5, $6::jsonb, $7)`,
		d.Namespace, d.DataId, version, *dataType, *content, tagsPtr, d.Actor); err != nil {
		return raftlog.Result{}, fmt.Errorf("追加 History: %w", err)
	}

	if err := advanceMeta(tctx, tx, index, term); err != nil {
		return raftlog.Result{}, err
	}
	if err := tx.Commit(tctx); err != nil {
		return raftlog.Result{}, fmt.Errorf("提交: %w", err)
	}
	committed = true
	return raftlog.OK(version, !live), nil
}

// CurrentVersion 返回条目当前版本与存活标志：存活条目为 biz_configs.version，
// 已删除为墓碑版本，从未存在为 (0, false)（DESIGN §3.1：墓碑延续同一版本序列）。
func (s *Store) CurrentVersion(ctx context.Context, namespace, dataId string) (version int64, live bool, err error) {
	err = s.pool.QueryRow(ctx,
		`SELECT version FROM biz_configs WHERE namespace = $1 AND data_id = $2`,
		namespace, dataId).Scan(&version)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		err = s.pool.QueryRow(ctx,
			`SELECT COALESCE(MAX(version), 0) FROM biz_config_history
			 WHERE namespace = $1 AND data_id = $2`,
			namespace, dataId).Scan(&version)
		if err != nil {
			return 0, false, fmt.Errorf("读取 History 版本: %w", err)
		}
		return version, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("读取当前版本: %w", err)
	}
	return version, true, nil
}

// ListHistory 按 version 倒序分页返回 History（DESIGN §5.1），含 Tombstone 记录。
// beforeVersion 非 nil 时仅返回 version 小于它的记录；limit ≤0 取默认值，超过上限收敛。
func (s *Store) ListHistory(ctx context.Context, namespace, dataId string, limit int, beforeVersion *int64) ([]store.HistoryRecord, error) {
	if limit <= 0 {
		limit = store.DefaultHistoryLimit
	}
	if limit > store.MaxHistoryLimit {
		limit = store.MaxHistoryLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT version, op, data_type, content, tags, actor, created_at
		FROM biz_config_history
		WHERE namespace = $1 AND data_id = $2 AND ($3::bigint IS NULL OR version < $3)
		ORDER BY version DESC
		LIMIT $4`, namespace, dataId, beforeVersion, limit)
	if err != nil {
		return nil, fmt.Errorf("查询 History: %w", err)
	}
	defer rows.Close()

	records := make([]store.HistoryRecord, 0)
	for rows.Next() {
		var r store.HistoryRecord
		var tags []byte
		if err := rows.Scan(&r.Version, &r.Op, &r.DataType, &r.Content, &tags, &r.Actor, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("读取 History 记录: %w", err)
		}
		if tags != nil {
			if err := json.Unmarshal(tags, &r.Tags); err != nil {
				return nil, fmt.Errorf("解析 tags: %w", err)
			}
		}
		records = append(records, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历 History: %w", err)
	}
	return records, nil
}

// AdvanceMeta 仅推进 raft_fsm_meta，用于信封损坏或 op 未实现但日志已提交的场景，
// 保证 last_applied_index 无空洞。
func (s *Store) AdvanceMeta(ctx context.Context, index, term uint64) error {
	tctx, cancel := context.WithTimeout(ctx, applyTimeout)
	defer cancel()
	tx, err := s.pool.Begin(tctx)
	if err != nil {
		return fmt.Errorf("开启事务: %w", err)
	}
	committed := false
	defer rollbackUnlessCommitted(tctx, tx, &committed)()
	if err := advanceMeta(tctx, tx, index, term); err != nil {
		return err
	}
	if err := tx.Commit(tctx); err != nil {
		return fmt.Errorf("提交: %w", err)
	}
	committed = true
	return nil
}

// finishRejected 提交一个拒绝性结果的事务：不追加 History，仅推进 raft_fsm_meta。
func finishRejected(ctx context.Context, tx pgx.Tx, committed *bool, index, term uint64, res raftlog.Result) (raftlog.Result, error) {
	if err := advanceMeta(ctx, tx, index, term); err != nil {
		return raftlog.Result{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return raftlog.Result{}, fmt.Errorf("提交（拒绝路径）: %w", err)
	}
	*committed = true
	return res, nil
}

// rollbackUnlessCommitted 返回一个可 defer 的守卫：事务提交成功（*committed 置 true）
// 后不再回滚，避免对已关闭事务的无效回滚。
func rollbackUnlessCommitted(ctx context.Context, tx pgx.Tx, committed *bool) func() {
	return func() {
		if !*committed {
			_ = tx.Rollback(ctx)
		}
	}
}

// advanceMeta 在当前事务内推进 raft_fsm_meta，保证三表进度恒一致。
func advanceMeta(ctx context.Context, tx pgx.Tx, index, term uint64) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO raft_fsm_meta (id, last_applied_index, last_applied_term)
		VALUES (1, $1, $2)
		ON CONFLICT (id) DO UPDATE SET
			last_applied_index = EXCLUDED.last_applied_index,
			last_applied_term  = EXCLUDED.last_applied_term`,
		int64(index), int64(term))
	if err != nil {
		return fmt.Errorf("推进 raft_fsm_meta: %w", err)
	}
	return nil
}

func marshalTags(tags map[string]string) (*string, error) {
	if tags == nil {
		return nil, nil
	}
	b, err := json.Marshal(tags)
	if err != nil {
		return nil, fmt.Errorf("序列化 tags: %w", err)
	}
	s := string(b)
	return &s, nil
}
