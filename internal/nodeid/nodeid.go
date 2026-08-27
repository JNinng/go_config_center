// Package nodeid 维护持久 Node ID（ADR-0009）：生命周期内不随地址变化。
package nodeid

import (
	crand "crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadOrCreate 返回 Node ID：优先采用配置显式指定的值；
// 否则读取 <dir>/node-id，不存在则生成 UUID 并持久化。
func LoadOrCreate(dir, configured string) (string, error) {
	if configured != "" {
		return configured, nil
	}
	path := filepath.Join(dir, "node-id")
	if b, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(b)); id != "" {
			return id, nil
		}
	}
	id := newUUID()
	if err := os.WriteFile(path, []byte(id+"\n"), 0o600); err != nil {
		return "", fmt.Errorf("持久化 node-id: %w", err)
	}
	return id, nil
}

func newUUID() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
