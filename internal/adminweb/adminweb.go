// Package adminweb 内嵌管理 GUI(Vite 构建产物;源码在 web/)
package adminweb

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler 管理页 SPA(/admin 与 /admin/ 均可;hash 路由,资源按文件名分发)
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		panic(err)
	}
	fileServer := http.FileServer(http.FS(sub))
	// FileServer 以 r.URL.Path 在 dist 根查文件, 剥离 /admin 前缀
	rewritePath := func(r *http.Request) {
		r.URL.Path = strings.TrimPrefix(r.URL.Path, "/admin")
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rewritePath(r)
		if r.URL.Path != "/" {
			if _, err := fs.Stat(sub, strings.TrimPrefix(r.URL.Path, "/")); err != nil {
				// 未命中的路径回退到入口(hash 路由不依赖此路径,防御性处理)
				r.URL.Path = "/"
			} else if strings.HasPrefix(r.URL.Path, "/assets/") {
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			}
		}
		if r.URL.Path == "/" {
			w.Header().Set("Cache-Control", "no-cache")
		}
		fileServer.ServeHTTP(w, r)
	})
}
