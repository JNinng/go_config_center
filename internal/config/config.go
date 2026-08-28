// Package config 加载并校验 config.yaml（DESIGN §6.1）。
package config

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
)

type Raft struct {
	SnapshotThreshold uint64 `yaml:"snapshot_threshold"`
	SnapshotRetention int    `yaml:"snapshot_retention"`
	TrailingLogs      uint64 `yaml:"trailing_logs"`
}

type Config struct {
	NodeID        string   `yaml:"node_id"`
	HTTPAddr      string   `yaml:"http_addr"`
	RaftAddr      string   `yaml:"raft_addr"`
	DataDir       string   `yaml:"data_dir"`
	AdminToken    string   `yaml:"admin_token"`
	PgDSN         string   `yaml:"pg_dsn"`
	HTTPAdvertise string   `yaml:"http_advertise"`
	Seeds         []string `yaml:"seeds"`
	Raft          Raft     `yaml:"raft"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(b, c); err != nil {
		return nil, fmt.Errorf("解析配置文件: %w", err)
	}
	applyDefaults(c)
	if err := validate(c); err != nil {
		return nil, err
	}
	return c, nil
}

func applyDefaults(c *Config) {
	if c.HTTPAddr == "" {
		c.HTTPAddr = ":8080"
	}
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	if c.Raft.SnapshotThreshold == 0 {
		c.Raft.SnapshotThreshold = 10000
	}
	if c.Raft.SnapshotRetention == 0 {
		c.Raft.SnapshotRetention = 2
	}
	if c.Raft.TrailingLogs == 0 {
		c.Raft.TrailingLogs = 10240
	}
	// HTTP 通告地址供 307 重定向与 Join 响应使用：缺省按 raft_addr 的主机 +
	// http_addr 的端口推导（同机部署即可用；跨机时显式配置）。
	if c.HTTPAdvertise == "" {
		host := c.RaftAddr
		if h, _, err := net.SplitHostPort(c.RaftAddr); err == nil {
			host = h
		}
		if host == "" {
			host = "127.0.0.1"
		}
		_, port, err := net.SplitHostPort(c.HTTPAddr)
		if err != nil || port == "" {
			port = "8080"
		}
		c.HTTPAdvertise = fmt.Sprintf("http://%s:%s", host, port)
	}
}

func validate(c *Config) error {
	if c.RaftAddr == "" {
		return fmt.Errorf("raft_addr 未配置（成员间复制流量地址）")
	}
	if c.PgDSN == "" {
		return fmt.Errorf("pg_dsn 未配置")
	}
	if c.AdminToken == "" {
		return fmt.Errorf("admin_token 未配置（写接口认证）")
	}
	return nil
}
