// Package console 提供控制台的静态资源服务（ADR-0012）：构建产物 go:embed 内嵌，
// 运行期零外部依赖。dist/ 由 scripts/build-console.sh 从 web/console 产出，整体不入库；
// 仓库内 dist 仅含 .keep（保证 embed 目录存在）。未构建时直接 go build 仍可通过，
// 访问任意路径得 placeholder 占位页——"未构建"是可观测状态而非故障。
package console

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

//go:embed placeholder.html
var placeholderHTML []byte

// Handler 返回控制台 handler：未构建（dist 无 index.html）时全部路径返回占位页；
// 已构建则 SPA fallback——dist 内存在的文件原样服务，其余 GET 路径回退 index.html，
// 由前端路由接管。
func Handler() (http.Handler, error) {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil, fmt.Errorf("定位内嵌 dist: %w", err)
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return placeholderHandler(), nil
	}
	return spaHandler(sub), nil
}

// placeholderHandler 以占位页响应全部请求（未构建兜底）。
func placeholderHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(placeholderHTML)
	})
}

// spaHandler 服务 dist 内文件，未命中路径回退 index.html（SPA fallback）。
func spaHandler(sub fs.FS) http.Handler {
	files := http.FileServerFS(sub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if name == "" || name == "." {
			name = "index.html"
		}
		if _, err := fs.Stat(sub, name); err != nil {
			r.URL.Path = "/"
		}
		files.ServeHTTP(w, r)
	})
}
