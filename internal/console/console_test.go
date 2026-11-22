package console

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
)

func mustHandler(t *testing.T) http.Handler {
	t.Helper()
	h, err := Handler()
	if err != nil {
		t.Fatalf("Handler: %v", err)
	}
	return h
}

func body(t *testing.T, h http.Handler, path string) (int, string, string) {
	t.Helper()
	ts := httptest.NewServer(h)
	defer ts.Close()
	resp, err := http.Get(ts.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(b)
}

// 未构建（dist 无 index.html）：任意路径返回占位页。
func TestPlaceholderMode(t *testing.T) {
	code, ct, got := body(t, placeholderHandler(), "/anything")
	if code != http.StatusOK || !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("占位模式 = %d %q, want 200 text/html", code, ct)
	}
	if !strings.Contains(got, "控制台未构建") {
		t.Fatalf("占位页内容缺失: %q", got)
	}
}

// spaHandler：存在的文件原样服务（含正确 Content-Type），未命中路径回退 index.html。
func TestSpaHandler(t *testing.T) {
	sub := fstest.MapFS{
		"index.html":    {Data: []byte("<html><div id=\"app\">spa</div></html>")},
		"assets/app.js": {Data: []byte("console.log(1)")},
	}
	h := spaHandler(sub)

	code, ct, got := body(t, h, "/")
	if code != http.StatusOK || !strings.HasPrefix(ct, "text/html") || !strings.Contains(got, `id="app"`) {
		t.Fatalf("GET / = %d %q %q", code, ct, got)
	}

	code, ct, got = body(t, h, "/assets/app.js")
	if code != http.StatusOK || !strings.Contains(ct, "javascript") || got != "console.log(1)" {
		t.Fatalf("GET /assets/app.js = %d %q %q, want 原样服务", code, ct, got)
	}

	code, _, got = body(t, h, "/some/spa/route")
	if code != http.StatusOK || !strings.Contains(got, `id="app"`) {
		t.Fatalf("SPA fallback = %d %q, want 回退 index.html", code, got)
	}
}

// Handler 装配内嵌 dist：无论构建产物还是占位页，根路径均服务含挂载点的 HTML 入口。
func TestHandlerServesIndex(t *testing.T) {
	code, ct, got := body(t, mustHandler(t), "/")
	if code != http.StatusOK || !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("GET / = %d %q, want 200 text/html", code, ct)
	}
	if !strings.Contains(got, `id="app"`) {
		t.Fatalf("入口页缺挂载点: %q", got)
	}
}
