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
	OpPublish  Op = "publish"
	OpDelete   Op = "delete"
	OpRollback Op = "rollback"
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
	if d.Namespace == "" {
		return errors.New("namespace 不能为空")
	}
	if len(d.Namespace) > MaxNamespaceLen {
		return fmt.Errorf("namespace 长度超过 %d", MaxNamespaceLen)
	}
	if d.DataId == "" {
		return errors.New("data_id 不能为空")
	}
	if len(d.DataId) > MaxDataIdLen {
		return fmt.Errorf("data_id 长度超过 %d", MaxDataIdLen)
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
	case OpPublish, OpDelete, OpRollback:
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

// Result 由 FSM.Apply 返回，经 ApplyFuture 回传给 API 层映射为 200/409/404。
type Result struct {
	Status         Status
	Version        int64 // ok 时新分配的 Version
	CurrentVersion int64 // version_conflict 时条目当前 Version
	Created        bool  // ok 时是否为新建（此前无存活条目）
	Err            string
}

func OK(version int64, created bool) Result {
	return Result{Status: StatusOK, Version: version, Created: created}
}

func Conflict(current int64) Result {
	return Result{Status: StatusVersionConflict, CurrentVersion: current}
}
