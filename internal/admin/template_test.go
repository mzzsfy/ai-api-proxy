package admin

import (
	"net/http/httptest"
	"testing"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

func TestTemplateParsesWithHooks(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/packages/template", nil)
	(&Deps{}).PackageTemplate(w, r)
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	pkg, err := plugin.ParseAAP(w.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"protocol.js", "filter.js", "settings.js", "tasks/heartbeat.js"} {
		if _, ok := pkg.Files[want]; !ok {
			t.Fatalf("missing %s", want)
		}
	}
	if pkg.Manifest.Parts.Hooks == nil || len(pkg.Manifest.Parts.Hooks.Tasks) != 1 {
		t.Fatal("hooks task missing")
	}
	if pkg.Manifest.MetaTitle() != "示例包" {
		t.Fatalf("title: %q", pkg.Manifest.MetaTitle())
	}
}
