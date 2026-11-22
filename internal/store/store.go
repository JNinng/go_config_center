// Package store 定义存储后端契约（DESIGN §3.1/§3.3/§4.4）：FSM 的单事务业务写入、
// 本机读路径、进度维护与快照数据体的导出/导入。内置 PostgreSQL 实现
//（internal/store/pg）；pgx 依赖不得出现在本包及上游（fsm/api/cmd）——
// 新后端（如 bbolt）实现 Store 接口即可接入（ADR-0011）。
package store

import (
	"context"
	"io"
	"time"

	"github.com/jninng/go_config_center/internal/raftlog"
)

// Store 是存储后端契约。实现约束：
//   - Apply 三方法必须在单个事务内完成业务写入与进度推进（无进度空洞）；
//     写入失败返回错误并回滚（上层按 apply-or-die 契约崩溃重放）。
//   - Apply 方法的 ts 参数是日志的业务时间戳（Raft 日志 AppendedAt：Leader 追加时
//     盖章，随日志复制与持久化，各节点应用同一条日志得到同一值）。行级
//     created_at/updated_at 一律取 ts，禁止以本机时钟或数据库默认值盖章——
//     否则节点加入/追平时时间字段与 Leader 漂移（DESIGN §3.1）。
//   - ExportTables/ImportTables 的数据体字节由实现自定义编码，但分节顺序
//     与命名须遵守 SnapshotTables（快照格式 v1 契约）。
type Store interface {
	// Migrate 初始化存储结构（表/桶），幂等。
	Migrate(ctx context.Context) error

	// LoadMeta 返回 FSM 已应用进度（raft_fsm_meta）。
	LoadMeta(ctx context.Context) (index, term uint64, err error)

	// AdvanceMeta 仅推进进度，用于信封损坏或 op 未实现但日志已提交的场景，
	// 保证 last_applied_index 无空洞。
	AdvanceMeta(ctx context.Context, index, term uint64) error

	// GetEntry 读当前条目；不存在（或已删除）返回 (nil, nil)。
	GetEntry(ctx context.Context, namespace, dataId string) (*Entry, error)

	// CurrentVersion 返回条目当前版本与存活标志：存活条目为当前版本，
	// 已删除为墓碑版本，从未存在为 (0, false)。
	CurrentVersion(ctx context.Context, namespace, dataId string) (version int64, live bool, err error)

	// ListHistory 按 version 倒序分页返回 History，含 Tombstone 记录；
	// beforeVersion 非 nil 时仅返回 version 小于它的记录。
	ListHistory(ctx context.Context, namespace, dataId string, limit int, beforeVersion *int64) ([]HistoryRecord, error)

	// ListEntries 按 data_id 字典序分页返回 Namespace 下存活条目的元数据（不含 content，
	// DESIGN §5.1）；afterID 非 nil 时仅返回 data_id 大于它的条目（keyset 分页）。
	// 已删除条目无当前行，天然不出现。
	ListEntries(ctx context.Context, namespace string, limit int, afterID *string) ([]EntryMeta, error)

	// ListNamespaces 返回仍有存活条目的 Namespace 去重清单（字典序，DESIGN §5.1）。
	ListNamespaces(ctx context.Context) ([]string, error)

	// ApplyPublish 应用 op=publish：CAS 判定 → upsert 当前条目 + 追加 History +
	// 推进步度。CAS 失败等拒绝结果也是已提交日志，须推进进度。
	ApplyPublish(ctx context.Context, d raftlog.PublishData, ts time.Time, index, term uint64) (raftlog.Result, error)

	// ApplyDelete 应用 op=delete：删除当前条目 + 追加 content 为 NULL 的 Tombstone
	//（版本号照常消耗）+ 推进步度。条目不存在或 CAS 不匹配为拒绝结果。
	ApplyDelete(ctx context.Context, d raftlog.DeleteData, ts time.Time, index, term uint64) (raftlog.Result, error)

	// ApplyDeleteNamespace 应用 op=delete_namespace：单事务删除该 Namespace 下全部
	// 存活条目，各追加一条 Tombstone（版本序列照常延续）+ 推进步度。
	// 无存活条目时为空操作（Entries=0，幂等）；结果携带被删条目清单（供 Watch 唤醒）。
	ApplyDeleteNamespace(ctx context.Context, d raftlog.DeleteNamespaceData, ts time.Time, index, term uint64) (raftlog.Result, error)

	// ApplyRollback 应用 op=rollback：以 to_version 从 History 取内容发布为新版本，
	// 从不物理撤销；目标解析与发布同事务（无 TOCTOU）。
	// 无效目标（History 不存在 / 指向 Tombstone / ≥ 当前版本）为拒绝结果。
	ApplyRollback(ctx context.Context, d raftlog.RollbackData, ts time.Time, index, term uint64) (raftlog.Result, error)

	// ExportTables 在单个一致性读事务内导出全部数据体：分节按 SnapshotTables 顺序
	// 连续写入 w，返回该事务读到的 FSM 进度（即快照点）与各分节行数/字节长度。
	ExportTables(ctx context.Context, w io.Writer) (SnapshotPoint, []SnapshotSection, error)

	// ImportTables 全量导入 ExportTables 产出的数据体，单事务清空重建：
	// 各分节实际行数须与 sections 一致、数据体 sha256 须等于 checksum、
	// 数据体之后不得有多余字节，任一不符整体回滚并返回错误。
	ImportTables(ctx context.Context, r io.Reader, sections []SnapshotSection, checksum string) error

	// Ping 探测存储连通性（/healthz）。
	Ping(ctx context.Context) error

	// Close 释放底层资源。
	Close()
}

// SnapshotTables 是快照数据体的分节顺序与命名——快照格式（v1）契约的一部分，
// ExportTables/ImportTables 必须按此顺序产出与消费。
var SnapshotTables = []string{"biz_configs", "biz_config_history", "raft_fsm_meta"}

// SnapshotPoint 是导出事务内读到的 FSM 进度，即快照点。
type SnapshotPoint struct {
	Index uint64
	Term  uint64
}

// SnapshotSection 声明快照单个分节的行数与字节长度；字节长度让导入方无需解析行边界。
type SnapshotSection struct {
	Rows  int64
	Bytes int64
}

// Entry 是一条 Config Entry 的当前态（本机物化视图）。
type Entry struct {
	Namespace string
	DataId    string
	DataType  string
	Content   string
	Version   int64
	Tags      map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// HistoryRecord 是 History 中的一条不可变版本记录；DataType/Content 为 nil 即 Tombstone。
type HistoryRecord struct {
	Version   int64
	Op        string
	DataType  *string
	Content   *string
	Tags      map[string]string
	Actor     *string
	CreatedAt time.Time
}

// EntryMeta 是 Config Entry 的列表元数据视图（不含 content，DESIGN §5.1）——
// 条目列表分页用，避免列表页拖入整页内容体。
type EntryMeta struct {
	Namespace string
	DataId    string
	DataType  string
	Version   int64
	Tags      map[string]string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// DefaultHistoryLimit 与 MaxHistoryLimit 是 History 查询的默认与上限页大小（DESIGN §5.1）；
// DefaultListLimit 与 MaxListLimit 是条目列表查询的默认与上限页大小。
const (
	DefaultHistoryLimit = 50
	MaxHistoryLimit     = 1000
	DefaultListLimit    = 100
	MaxListLimit        = 1000
)

// ClampListLimit 将条目列表页大小收敛到生效值：≤0 取默认，超过上限收敛。
// API 层计算分页游标（是否满页）必须使用同一收敛结果，避免以未收敛的
// 原始 limit 误判满页而丢 next_after_id。
func ClampListLimit(limit int) int {
	if limit <= 0 {
		return DefaultListLimit
	}
	if limit > MaxListLimit {
		return MaxListLimit
	}
	return limit
}
