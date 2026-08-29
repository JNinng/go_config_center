// Package raftlog 定义 Raft 日志信封（DESIGN §3.2）与 FSM Apply 结果（DESIGN §3.3）。
package raftlog

import (
	"encoding/json"
	"errors"
	"fmt"
)

// FormatVersion 是日志信封的格式版本号，升级编解码时递增并兼容读取。
const FormatVersion = 1

// Op 是日志信封中的操作类型。
type Op string

const (
	OpPublish         Op = "publish"
	OpDelete          Op = "delete"
	OpRollback        Op = "rollback"
	OpDeleteNamespace Op = "delete_namespace"
)

// 与 PostgreSQL schema（DESIGN §3.1）和量级假设（DESIGN §1.3）一致的长度上限。
const (
	MaxNamespaceLen = 64
	MaxDataIdLen    = 255
	MaxContentBytes = 1 << 20
)

// Command 是一条 Raft 日志的 JSON 信封：{"v":1,"op":"...","data":{...}}。
type Command struct {
	V    int             `json:"v"`
	Op   Op              `json:"op"`
	Data json.RawMessage `json:"data"`
}

// PublishData 是 op=publish 的 data 载荷。ExpectedVersion 为 nil 表示省略（LWW）。
type PublishData struct {
	Namespace       string            `json:"namespace"`
	DataId          string            `json:"data_id"`
	DataType        string            `json:"data_type"`
	Content         string            `json:"content"`
	Tags            map[string]string `json:"tags,omitempty"`
	ExpectedVersion *int64            `json:"expected_version,omitempty"`
	Actor           string            `json:"actor,omitempty"`
}

func NewPublish(d PublishData) (Command, error) {
	if err := validatePublish(d); err != nil {
		return Command{}, err
	}
	data, err := json.Marshal(d)
	if err != nil {
		return Command{}, err
	}
	return Command{V: FormatVersion, Op: OpPublish, Data: data}, nil
}

func validatePublish(d PublishData) error {
	if err := validateIdentity(d.Namespace, d.DataId); err != nil {
		return err
	}
	switch d.DataType {
	case "JSON", "YAML", "TEXT":
	default:
		return fmt.Errorf("data_type 必须是 JSON/YAML/TEXT，得到 %q", d.DataType)
	}
	if len(d.Content) > MaxContentBytes {
		return fmt.Errorf("content 超过 %d 字节", MaxContentBytes)
	}
	if d.ExpectedVersion != nil && *d.ExpectedVersion < 0 {
		return errors.New("expected_version 不能为负")
	}
	return nil
}

// Publish 解码并校验 op=publish 的 data 载荷。
func (c Command) Publish() (PublishData, error) {
	var d PublishData
	if err := json.Unmarshal(c.Data, &d); err != nil {
		return PublishData{}, fmt.Errorf("解析 publish data: %w", err)
	}
	if err := validatePublish(d); err != nil {
		return PublishData{}, err
	}
	return d, nil
}

// DeleteData 是 op=delete 的 data 载荷。ExpectedVersion 为 nil 表示省略（LWW）。
type DeleteData struct {
	Namespace       string `json:"namespace"`
	DataId          string `json:"data_id"`
	ExpectedVersion *int64 `json:"expected_version,omitempty"`
	Actor           string `json:"actor,omitempty"`
}

func NewDelete(d DeleteData) (Command, error) {
	if err := validateDelete(d); err != nil {
		return Command{}, err
	}
	data, err := json.Marshal(d)
	if err != nil {
		return Command{}, err
	}
	return Command{V: FormatVersion, Op: OpDelete, Data: data}, nil
}

func validateDelete(d DeleteData) error {
	if err := validateIdentity(d.Namespace, d.DataId); err != nil {
		return err
	}
	if d.ExpectedVersion != nil && *d.ExpectedVersion < 0 {
		return errors.New("expected_version 不能为负")
	}
	return nil
}

// Delete 解码并校验 op=delete 的 data 载荷。
func (c Command) Delete() (DeleteData, error) {
	var d DeleteData
	if err := json.Unmarshal(c.Data, &d); err != nil {
		return DeleteData{}, fmt.Errorf("解析 delete data: %w", err)
	}
	if err := validateDelete(d); err != nil {
		return DeleteData{}, err
	}
	return d, nil
}

// RollbackData 是 op=rollback 的 data 载荷（DESIGN §3.2）。
// to_version 的解析在 FSM Apply 阶段进行，API 层不预取，消除与并发写的竞态。
type RollbackData struct {
	Namespace string `json:"namespace"`
	DataId    string `json:"data_id"`
	ToVersion int64  `json:"to_version"`
	Actor     string `json:"actor,omitempty"`
}

func NewRollback(d RollbackData) (Command, error) {
	if err := validateRollback(d); err != nil {
		return Command{}, err
	}
	data, err := json.Marshal(d)
	if err != nil {
		return Command{}, err
	}
	return Command{V: FormatVersion, Op: OpRollback, Data: data}, nil
}

func validateRollback(d RollbackData) error {
	if err := validateIdentity(d.Namespace, d.DataId); err != nil {
		return err
	}
	if d.ToVersion <= 0 {
		return fmt.Errorf("to_version 必须为正整数，得到 %d", d.ToVersion)
	}
	return nil
}

// Rollback 解码并校验 op=rollback 的 data 载荷。
func (c Command) Rollback() (RollbackData, error) {
	var d RollbackData
	if err := json.Unmarshal(c.Data, &d); err != nil {
		return RollbackData{}, fmt.Errorf("解析 rollback data: %w", err)
	}
	if err := validateRollback(d); err != nil {
		return RollbackData{}, err
	}
	return d, nil
}

// DeleteNamespaceData 是 op=delete_namespace 的 data 载荷：原子删除该 Namespace
// 下全部存活 Config Entry（各写一条 Tombstone，History 保留可逐条回滚，DESIGN §5.1）。
type DeleteNamespaceData struct {
	Namespace string `json:"namespace"`
	Actor     string `json:"actor,omitempty"`
}

func NewDeleteNamespace(d DeleteNamespaceData) (Command, error) {
	if err := validateNamespace(d.Namespace); err != nil {
		return Command{}, err
	}
	data, err := json.Marshal(d)
	if err != nil {
		return Command{}, err
	}
	return Command{V: FormatVersion, Op: OpDeleteNamespace, Data: data}, nil
}

// DeleteNamespace 解码并校验 op=delete_namespace 的 data 载荷。
func (c Command) DeleteNamespace() (DeleteNamespaceData, error) {
	var d DeleteNamespaceData
	if err := json.Unmarshal(c.Data, &d); err != nil {
		return DeleteNamespaceData{}, fmt.Errorf("解析 delete_namespace data: %w", err)
	}
	if err := validateNamespace(d.Namespace); err != nil {
		return DeleteNamespaceData{}, err
	}
	return d, nil
}

func validateIdentity(namespace, dataId string) error {
	if err := validateNamespace(namespace); err != nil {
		return err
	}
	if dataId == "" {
		return errors.New("data_id 不能为空")
	}
	if len(dataId) > MaxDataIdLen {
		return fmt.Errorf("data_id 长度超过 %d", MaxDataIdLen)
	}
	return nil
}

func validateNamespace(namespace string) error {
	if namespace == "" {
		return errors.New("namespace 不能为空")
	}
	if len(namespace) > MaxNamespaceLen {
		return fmt.Errorf("namespace 长度超过 %d", MaxNamespaceLen)
	}
	return nil
}

func Encode(c Command) ([]byte, error) {
	return json.Marshal(c)
}

func Decode(b []byte) (Command, error) {
	var c Command
	if err := json.Unmarshal(b, &c); err != nil {
		return Command{}, fmt.Errorf("解析日志信封: %w", err)
	}
	if c.V != FormatVersion {
		return Command{}, fmt.Errorf("不支持的日志格式版本 v=%d", c.V)
	}
	switch c.Op {
	case OpPublish, OpDelete, OpRollback, OpDeleteNamespace:
	default:
		return Command{}, fmt.Errorf("未知 op %q", c.Op)
	}
	if len(c.Data) == 0 {
		return Command{}, errors.New("信封缺少 data")
	}
	return c, nil
}

// Status 是一条已提交日志 Apply 后的业务结果（DESIGN §3.3）。
type Status string

const (
	StatusOK              Status = "ok"
	StatusVersionConflict Status = "version_conflict" // CAS 不匹配
	StatusNotFound        Status = "not_found"        // 条目从未存在
	StatusInvalidRollback Status = "invalid_rollback" // to_version 无效
	StatusReplayed        Status = "replayed"         // index ≤ last_applied，跳过重放
	StatusError           Status = "error"            // 内部错误
)

// DeletedEntry 是 op=delete_namespace 被删条目的一条记录（data_id 与墓碑版本），
// 供 FSM 逐条唤醒 Watch；其他 op 恒为 nil。
type DeletedEntry struct {
	DataId  string
	Version int64
}

// Result 由 FSM.Apply 返回，经 ApplyFuture 回传给 API 层映射为 200/409/404。
type Result struct {
	Status         Status
	Version        int64         // ok 时新分配的 Version
	CurrentVersion int64         // version_conflict 时条目当前 Version
	Created        bool          // ok 时是否为新建（此前无存活条目）
	Entries        int64         // ok 且 op=delete_namespace 时删除的条目数
	Deleted        []DeletedEntry // ok 且 op=delete_namespace 时的被删条目清单
	Err            string        // 非 ok 时的补充说明
	Index          uint64        // replayed 时命中的日志 index，供上层定位 lineage 错位
}

func OK(version int64, created bool) Result {
	return Result{Status: StatusOK, Version: version, Created: created}
}

func Conflict(current int64) Result {
	return Result{Status: StatusVersionConflict, CurrentVersion: current}
}
