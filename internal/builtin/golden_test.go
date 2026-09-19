package builtin

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"ai-api-proxy/internal/pipeline"
	"ai-api-proxy/internal/plugin"
)

// ─── 黄金对照:js-openai-full 包(JS 版)与内置 Go 版同输入同产物 ───

// goldenCtx 构造一致上下文
func goldenCtx() *pipeline.PipelineContext {
	ctx := pipeline.NewContext("r", pipeline.UpstreamInfo{Name: "u", Models: []string{"m"}}, pipeline.Vars{Model: "m", EntryStream: true})
	ctx.Target = pipeline.Target{ID: "1/t1", Name: "t1", BaseURL: "https://api.x.com/", SecretsRef: "u/t1"}
	return ctx
}

const goldenKey = "sk-golden"

// loadJSPackage 从 js-openai-full 插件库(与主库同级的 plugins-repo)实例化协议;库缺失则跳过
func loadJSPackage(t *testing.T) pipeline.Protocol {
	t.Helper()
	root := findRepoRoot(t)
	dir := filepath.Join(root, "plugins", "js-openai-full")
	if _, err := os.Stat(dir); err != nil {
		// 插件已拆分独立库(../plugins-repo), 优先从那里取
		sibling := filepath.Join(root, "..", "plugins-repo", "plugins", "js-openai-full")
		if _, err2 := os.Stat(sibling); err2 != nil {
			t.Skip("js-openai-full plugin repo not found (split repo); skipping golden JS-vs-Go comparison")
		}
		dir = sibling
	}
	manifest, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	protoSrc, err := os.ReadFile(filepath.Join(dir, "protocol.js"))
	if err != nil {
		t.Fatal(err)
	}
	aap := zipBytes(t, manifest, map[string]string{"protocol.js": string(protoSrc)})
	pkg, err := plugin.ParseAAP(aap)
	if err != nil {
		t.Fatal(err)
	}
	secrets := func(target, key string) (string, bool) {
		if key == "api_key" {
			return goldenKey, true
		}
		return "", false
	}
	secretValues := map[string]map[string]string{"t1": {"api_key": goldenKey}}
	p, err := plugin.NewProtocol(pkg, nil, secrets, func(string) map[string]string { return secretValues["t1"] }, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.(interface{ ClosePools() }).ClosePools)
	return p
}

// zipBytes 内存打包 .aap
func zipBytes(t *testing.T, manifest []byte, files map[string]string) []byte {
	t.Helper()
	buf := &bytes.Buffer{}
	zw := zip.NewWriter(buf)
	mf, err := zw.Create("manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = mf.Write(manifest)
	for name, src := range files {
		f, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.Write([]byte(src))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// findRepoRoot 向上找 go.mod
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for range 6 {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		dir = filepath.Dir(dir)
	}
	t.Fatal("repo root not found")
	return ""
}

// TestGolden_JSvsBuiltin_BuildRequest JS 版与内置版 BuildRequest 产物一致
func TestGolden_JSvsBuiltin_BuildRequest(t *testing.T) {
	goProto := &Protocol{TargetSecrets: func(target, key string) (string, bool) { return goldenKey, true }}
	jsProto := loadJSPackage(t)
	cases := []struct {
		name  string
		pivot string
	}{
		{"plain", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`},
		{"anthropic system", `{"model":"m","system":"be nice","messages":[{"role":"user","content":"hi"}]}`},
		{"stop_sequences", `{"model":"m","messages":[],"stop_sequences":["END"]}`},
		{"x_top_k", `{"model":"m","messages":[],"x_top_k":5}`},
		{"x_stripped", `{"model":"m","messages":[],"x_block":{"index":0},"x_custom":1}`},
		{"full combo", `{"model":"m","system":"s","messages":[],"stop_sequences":["E"],"x_top_k":3,"x_trash":0}`},
		{"non json", `not-json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goReq, err := goProto.BuildRequest(goldenCtx(), []byte(tc.pivot))
			if err != nil {
				t.Fatal(err)
			}
			jsReq, err := jsProto.BuildRequest(goldenCtx(), []byte(tc.pivot))
			if err != nil {
				t.Fatal(err)
			}
			if goReq.URL != jsReq.URL || goReq.Method != jsReq.Method || goReq.Stream != jsReq.Stream {
				t.Fatalf("carrier mismatch:\n go=%+v\n js=%+v", goReq, jsReq)
			}
			if goReq.Headers["Authorization"] != jsReq.Headers["Authorization"] ||
				goReq.Headers["Content-Type"] != jsReq.Headers["Content-Type"] {
				t.Fatalf("headers mismatch:\n go=%v\n js=%v", goReq.Headers, jsReq.Headers)
			}
			// 语义等价(JSON 规范化比较,键序无关)
			if !jsonEqual(goReq.Body, jsReq.Body) {
				t.Fatalf("body mismatch:\n go=%s\n js=%s", goReq.Body, jsReq.Body)
			}
		})
	}
}

// TestGolden_JSvsBuiltin_Events 信封解包与 [DONE] 跳帧一致
func TestGolden_JSvsBuiltin_Events(t *testing.T) {
	goProto := New()
	jsProto := loadJSPackage(t)
	frames := []string{
		`{"event":"","data":"{\"id\":1,\"delta\":{\"content\":\"x\"}}"}`,
		`{"event":"ping","data":"keepalive"}`,
		`{"event":"","data":"[DONE]"}`,
	}
	for _, f := range frames {
		got, gerr := goProto.MapEvent(goldenCtx(), []byte(f))
		jv, jerr := jsProto.MapEvent(goldenCtx(), []byte(f))
		if (gerr == nil) != (jerr == nil) {
			t.Fatalf("err mismatch on %s: go=%v js=%v", f, gerr, jerr)
		}
		if string(got) != string(jv) {
			t.Fatalf("event mismatch on %s: go=%s js=%s", f, got, jv)
		}
	}
}

// TestGolden_JSvsBuiltin_MapResponse 非流式透传一致
func TestGolden_JSvsBuiltin_MapResponse(t *testing.T) {
	goProto := New()
	jsProto := loadJSPackage(t)
	body := []byte(`{"id":"c1","object":"chat.completion"}`)
	gb, gerr := goProto.MapResponse(goldenCtx(), body)
	jb, jerr := jsProto.MapResponse(goldenCtx(), body)
	if gerr != nil || jerr != nil || string(gb) != string(jb) {
		t.Fatalf("response mismatch: go=(%s,%v) js=(%s,%v)", gb, gerr, jb, jerr)
	}
}

// jsonEqual 语义等价(双端产物均 JSON 时规范化比较;均非 JSON 时字符串比较)
func jsonEqual(a, b []byte) bool {
	var va, vb any
	aok := json.Unmarshal(a, &va) == nil
	bok := json.Unmarshal(b, &vb) == nil
	if aok && bok {
		ea, _ := json.Marshal(va)
		eb, _ := json.Marshal(vb)
		return string(ea) == string(eb)
	}
	return string(a) == string(b)
}
