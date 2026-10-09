package plugin

import (
	"context"
	"testing"
)

func TestInstall_SourceRoundtrip(t *testing.T) {
	// Given 订阅来源安装 When 读包来源 Then source 保留;local 安装 source 规范化为 local
	r := NewRegistry(testDB(t))
	ctx := context.Background()
	data := buildAAP(t, goodManifest, map[string]string{
		"protocol.js":         protoSrc,
		"filters/identity.js": filterSrc,
	})
	if err := r.Install(ctx, data, "subscription/repo-a"); err != nil {
		t.Fatal(err)
	}
	if src, ok := r.PackageSource("demo"); !ok || src != "subscription/repo-a" {
		t.Fatalf("source roundtrip: %q ok=%v", src, ok)
	}
	if p, _ := r.GetPackage("demo"); p.Source != "subscription/repo-a" {
		t.Fatalf("package source field: %q", p.Source)
	}
	// 空串安装另一包(仅 filter 部件):落库规范化为 local
	m2 := `{"manifestVersion":1,"name":"demo2","version":"1.0.0","parts":{"filters":[{"name":"identity"}]}}`
	if err := r.Install(ctx, buildAAP(t, m2, map[string]string{"filters/identity.js": filterSrc}), ""); err != nil {
		t.Fatal(err)
	}
	if src, ok := r.PackageSource("demo2"); !ok || src != "local" {
		t.Fatalf("empty source must normalize to local: %q ok=%v", src, ok)
	}
}

func TestInstall_SourceConflictBothDirections(t *testing.T) {
	// Given local 包已存在 When 订阅 source 同名安装 Then 拒绝;反向(订阅包被 local 覆盖)同样拒绝
	r := NewRegistry(testDB(t))
	ctx := context.Background()
	data := buildAAP(t, goodManifest, map[string]string{
		"protocol.js":         protoSrc,
		"filters/identity.js": filterSrc,
	})
	if err := r.Install(ctx, data, ""); err != nil {
		t.Fatal(err)
	}
	if err := r.Install(ctx, data, "subscription/repo-a"); err == nil {
		t.Fatal("subscription over local must be rejected")
	}
	// 清场后反向:订阅包存在,local 同名安装被拒
	r2 := NewRegistry(testDB(t))
	if err := r2.Install(ctx, data, "subscription/repo-a"); err != nil {
		t.Fatal(err)
	}
	if err := r2.Install(ctx, data, ""); err == nil {
		t.Fatal("local over subscription must be rejected")
	}
	// 同 source 升级放行
	if err := r2.Install(ctx, data, "subscription/repo-a"); err != nil {
		t.Fatalf("same-source upgrade must pass: %v", err)
	}
}

func TestPackageSource_Unknown(t *testing.T) {
	// Given 空注册中心 When 查不存在包 Then ok=false
	r := NewRegistry(testDB(t))
	if _, ok := r.PackageSource("nope"); ok {
		t.Fatal("unknown package must report ok=false")
	}
}
