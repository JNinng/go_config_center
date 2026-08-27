// Package testutil 提供 PG 集成测试脚手架：一次性测试库与断言查询。
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// AdminDSN 返回管理员 DSN（GCC_TEST_PG_DSN）；为空表示 PG 集成测试不可用。
func AdminDSN() string {
	return os.Getenv("GCC_TEST_PG_DSN")
}

// NewDB 创建一次性测试库并注册测试结束清理，返回指向该库的 DSN。
// 未设置 GCC_TEST_PG_DSN 时跳过测试。
func NewDB(t *testing.T, prefix string) string {
	t.Helper()
	adminDSN := AdminDSN()
	if adminDSN == "" {
		t.Skip("GCC_TEST_PG_DSN 未设置，跳过 PG 集成测试")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Skipf("连接管理员 DSN 失败: %v", err)
	}
	dbName := fmt.Sprintf("%s_%d_%d", prefix, os.Getpid(), time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		t.Skipf("创建测试库失败: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+dbName+" WITH (FORCE)")
		_ = admin.Close(ctx)
	})

	// pgx 的 ConnString() 返回解析时的原串，不反映后续修改，因此显式拼接。
	cfg, err := pgx.ParseConfig(adminDSN)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	sslmode := "disable"
	if cfg.TLSConfig != nil {
		sslmode = "require"
	}
	return fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, dbName, sslmode)
}

// QueryInt 在指定库上执行返回单个整数的查询（测试断言用）。
func QueryInt(t *testing.T, dsn, sql string, args ...any) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("assert connect: %v", err)
	}
	defer conn.Close(ctx)
	var n int64
	if err := conn.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		t.Fatalf("assert query: %v", err)
	}
	return n
}
