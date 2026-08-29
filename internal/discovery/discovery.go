// Package discovery 实现发现代理（DESIGN §4.5、ADR-0008）：
// 空 Raft 状态的节点启动后扫描 Seed List（排除自身），经 /internal/discovery
// 寻找 Leader 并调用 Join API；Seed 全部不可达时指数退避重试——永不自举、不退出。
// 加入成功后进入低频刷新，持续跟踪 Leader 的 HTTP 地址（Leader 更替后仍可 307）。
package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	scanTimeout    = 2 * time.Second
	joinTimeout    = 10 * time.Second
	maxBackoff     = 30 * time.Second
	defaultRefresh = 5 * time.Second
)

// Options 是发现代理的运行参数。
type Options struct {
	NodeID        string
	RaftAddr      string
	HTTPAdvertise string
	Seeds         []string // 对端节点的 HTTP 基地址（如 http://127.0.0.1:8080）
	AdminToken    string
	// RefreshPeriod 是加入成功后跟踪 Leader 地址的刷新周期，缺省 5s（测试可缩短）。
	RefreshPeriod time.Duration
	// OnLeader 在本节点获知 Leader 地址时被调用（含加入成功后与刷新期间）。
	// raftAddr 供调用方校验缓存新鲜度（Leader 更换后旧 HTTP 地址立即失效）。
	OnLeader func(leaderRaftAddr, leaderHTTPAddr string)
}

type discoveryInfo struct {
	NodeID         string `json:"node_id"`
	Role           string `json:"role"`
	LeaderAddr     string `json:"leader_addr"`
	LeaderHTTPAddr string `json:"leader_http_addr"`
}

// Run 阻塞运行发现代理直至 ctx 取消。alreadyMember 表示本地已有 Raft 状态
// （成员表里已有自己，重启自愈场景）：跳过初始 Join，仅做刷新。
func Run(ctx context.Context, log *slog.Logger, opts Options, alreadyMember bool) {
	client := &http.Client{Timeout: scanTimeout}
	backoff := time.Second

	joined := alreadyMember
	if !joined {
		log.Info("发现代理启动：扫描 Seed List 寻找 Leader",
			"node_id", opts.NodeID, "seeds", opts.Seeds)
	}

	refresh := opts.RefreshPeriod
	if refresh == 0 {
		refresh = defaultRefresh
	}

	for {
		if ctx.Err() != nil {
			return
		}
		leaderRaft, leaderHTTP, found := scanAndJoin(ctx, log, client, opts, joined)
		if found {
			if opts.OnLeader != nil {
				opts.OnLeader(leaderRaft, leaderHTTP)
			}
			if !joined {
				joined = true
				log.Info("已加入集群，进入 Leader 跟踪刷新", "leader_http_addr", leaderHTTP)
			}
			sleepCtx(ctx, refresh)
			continue
		}
		if joined {
			// 刷新失败不算事件，保持低频。
			sleepCtx(ctx, refresh)
			continue
		}
		log.Warn("Seed 全部不可达，退避重试（永不自举、不退出）",
			"backoff", backoff.String())
		sleepCtx(ctx, backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// scanAndJoin 扫描 seeds：发现 Leader（或经 Follower 缓存得知 Leader 地址）时
// 尝试 Join，返回 Leader 的 Raft/HTTP 地址。joined=true 时只做地址跟踪，不再发 Join。
func scanAndJoin(ctx context.Context, log *slog.Logger, client *http.Client, opts Options, joined bool) (string, string, bool) {
	for _, seed := range opts.Seeds {
		if isSelfAddr(seed, opts.HTTPAdvertise, opts.RaftAddr) {
			continue
		}
		info, err := fetchDiscovery(ctx, client, seed)
		if err != nil {
			log.Debug("Seed 不可达", "seed", seed, "err", err)
			continue
		}
		if info.LeaderHTTPAddr == "" || info.LeaderAddr == "" {
			continue
		}
		if info.Role != "leader" && info.Role != "follower" {
			continue
		}
		// Leader seed 直接 Join；Follower seed 从其缓存取 Leader HTTP 地址再试（DESIGN §4.5）。
		if opts.OnLeader != nil {
			opts.OnLeader(info.LeaderAddr, info.LeaderHTTPAddr)
		}
		if joined {
			return info.LeaderAddr, info.LeaderHTTPAddr, true
		}
		if err := sendJoin(ctx, client, opts, info.LeaderHTTPAddr); err != nil {
			log.Warn("Join 失败", "leader", info.LeaderHTTPAddr, "err", err)
			continue
		}
		return info.LeaderAddr, info.LeaderHTTPAddr, true
	}
	return "", "", false
}

// fetchDiscovery 调用对端 /internal/discovery。
func fetchDiscovery(ctx context.Context, client *http.Client, seed string) (discoveryInfo, error) {
	var info discoveryInfo
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, seed+"/internal/discovery", nil)
	if err != nil {
		return info, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return info, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("discovery 返回 %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return info, fmt.Errorf("解析 discovery 响应: %w", err)
	}
	return info, nil
}

// sendJoin 向 Leader 发起加入（POST /v1/cluster/join，Bearer 认证）。
func sendJoin(ctx context.Context, client *http.Client, opts Options, leaderHTTP string) error {
	body, err := json.Marshal(map[string]string{
		"node_id":   opts.NodeID,
		"raft_addr": opts.RaftAddr,
		"http_addr": opts.HTTPAdvertise,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, leaderHTTP+"/v1/cluster/join", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+opts.AdminToken)
	req.Header.Set("Content-Type", "application/json")

	joinCtx, cancel := context.WithTimeout(ctx, joinTimeout)
	defer cancel()
	resp, err := (&http.Client{Timeout: joinTimeout}).Do(req.WithContext(joinCtx))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		return nil
	default:
		return fmt.Errorf("join 返回 %d", resp.StatusCode)
	}
}

// normalizeAddr 把地址/URL 统一为 host:port（空主机视为 127.0.0.1）。
func normalizeAddr(addr string) string {
	if u, err := url.Parse(addr); err == nil && u.Host != "" {
		addr = u.Host
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// isSelfAddr 判断 seed 是否指向自身（通告地址或 Raft 地址的主机端口）。
func isSelfAddr(seed, httpAdvertise, raftAddr string) bool {
	return normalizeAddr(seed) == normalizeAddr(httpAdvertise) ||
		normalizeAddr(seed) == normalizeAddr(raftAddr)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
