package plugin

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
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

// PackDir 打包包目录为 .aap 字节(唯一 .aap 生产路径);manifest.json 必在,其余文件按路径序入包
func PackDir(dir string) ([]byte, error) {
	m := &Manifest{}
	manifestRaw, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(manifestRaw, m); err != nil {
		return nil, fmt.Errorf("parse manifest.json: %w", err)
	}
	files, err := walkPackageFiles(dir)
	if err != nil {
		return nil, err
	}
	raw, err := BuildAAP(m, files)
	if err != nil {
		return nil, err
	}
	// 自校验:产出必须是可安装的合法包
	if _, err := ParseAAP(raw); err != nil {
		return nil, fmt.Errorf("packed package invalid: %w", err)
	}
	return raw, nil
}

// readManifest 读包目录 manifest.json
func readManifest(dir string) ([]byte, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("read manifest.json: %w", err)
	}
	return raw, nil
}

// walkPackageFiles 收集除 manifest.json 外的全部常规文件(路径分隔符归一为 /)
func walkPackageFiles(dir string) (map[string][]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(rel)
		if name == "manifest.json" {
			return nil
		}
		content, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		files[name] = content
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", dir, err)
	}
	return files, nil
}

// BuildAAP 打包 manifest + files 为 .aap 字节(导出/模板/目录打包共用)
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
