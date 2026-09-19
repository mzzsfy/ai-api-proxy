// Package adminweb 内嵌管理 GUI(单文件零依赖:原生 HTML/JS/fetch;无构建链)
package adminweb

import (
	_ "embed"
	"net/http"
)

//go:embed index.html
var indexHTML []byte

// Handler 管理页(SPA 单页;/admin 与 /admin/ 均可)
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		_, _ = w.Write(indexHTML)
	})
}
