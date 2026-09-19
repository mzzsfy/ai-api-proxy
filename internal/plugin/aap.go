package plugin

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
)

// MaxPackageSize .aap 包大小上限(防zip炸弹)
const MaxPackageSize = 8 * 1024 * 1024

// ParseAAP 解析 .aap(zip)字节流为包
func ParseAAP(data []byte) (*Package, error) {
	if len(data) > MaxPackageSize {
		return nil, fmt.Errorf("package too large: %d bytes", len(data))
	}
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open aap zip: %w", err)
	}
	pkg := &Package{Files: map[string][]byte{}}
	var manifestRaw []byte
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		name := path.Clean(f.Name)
		if strings.HasPrefix(name, "..") || path.IsAbs(name) {
			return nil, fmt.Errorf("unsafe entry %q", f.Name)
		}
		rc, err := f.Open()
		if err != nil {
			return nil, fmt.Errorf("open entry %s: %w", name, err)
		}
		content, err := io.ReadAll(io.LimitReader(rc, MaxPackageSize))
		_ = rc.Close()
		if err != nil {
			return nil, fmt.Errorf("read entry %s: %w", name, err)
		}
		if name == "manifest.json" {
			manifestRaw = content
			continue
		}
		pkg.Files[name] = content
	}
	if manifestRaw == nil {
		return nil, fmt.Errorf("manifest.json missing")
	}
	m := &Manifest{}
	if err := json.Unmarshal(manifestRaw, m); err != nil {
		return nil, fmt.Errorf("parse manifest.json: %w", err)
	}
	pkg.Manifest = m
	if err := pkg.Validate(); err != nil {
		return nil, err
	}
	return pkg, nil
}

// BuildAAP 打包 manifest + files 为 .aap 字节(导出/模板共用)
func BuildAAP(m *Manifest, files map[string][]byte) ([]byte, error) {
	manifestRaw, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		return nil, err
	}
	if _, err := mf.Write(manifestRaw); err != nil {
		return nil, err
	}
	for name, src := range files {
		f, err := zw.Create(name)
		if err != nil {
			return nil, err
		}
		if _, err := f.Write(src); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
