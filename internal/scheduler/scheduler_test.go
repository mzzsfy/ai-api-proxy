package scheduler

import (
	"sync"
	"testing"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// fakeSource 任务声明来源桩
type fakeSource struct {
	pkgs     map[string]*plugin.Package
	disabled map[string]bool
}

func (f *fakeSource) ListPackages() []string {
	out := make([]string, 0, len(f.pkgs))
	for n := range f.pkgs {
		out = append(out, n)
	}
	return out
}

func (f *fakeSource) GetPackage(name string) (*plugin.Package, error) {
	return f.pkgs[name], nil
}

func (f *fakeSource) IsEnabled(name string) bool { return !f.disabled[name] }

func hooksPkg(name string, tasks ...plugin.HooksTask) *plugin.Package {
	if tasks == nil {
		tasks = []plugin.HooksTask{}
	}
	pkg := &plugin.Package{
		Manifest: &plugin.Manifest{ManifestVersion: plugin.ManifestVersion, Name: name, Version: "0.1.0"},
	}
	pkg.Manifest.Parts.Hooks = &plugin.HooksPart{Entry: "h.js", Tasks: tasks}
	return pkg
}

// waitRunners 等待在途 runner 全部进入阻塞点
func (s *Scheduler) waitRunners(mu *sync.Mutex, active *int) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := *active
		mu.Unlock()
		if n > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestScheduler_TickFiresDueTask(t *testing.T) {
	// Given 每分钟任务 When tick 至到期分钟 Then runner 收到包名与触发时刻
	src := &fakeSource{pkgs: map[string]*plugin.Package{"checkin": hooksPkg("checkin",
		plugin.HooksTask{Name: "signIn", Cron: "* * * * *"})}}
	var mu sync.Mutex
	var got []string
	s := New(src, func(pkgName string, task plugin.HooksTask, at time.Time) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, pkgName+"/"+task.Name)
		return nil
	}, nil)
	s.Refresh()
	s.tick(time.Date(2025, 1, 1, 9, 30, 0, 0, time.Local))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 1 {
			if got[0] != "checkin/signIn" {
				t.Fatalf("run: %v", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("task not run: %v", got)
}

func TestScheduler_SkipsWhenPreviousRunning(t *testing.T) {
	// Given 上一轮阻塞未结束 When 再 tick Then 本轮跳过且前轮仍在跑
	src := &fakeSource{pkgs: map[string]*plugin.Package{"p": hooksPkg("p",
		plugin.HooksTask{Name: "t", Cron: "* * * * *"})}}
	release := make(chan struct{})
	var mu sync.Mutex
	active := 0
	s := New(src, func(pkgName string, task plugin.HooksTask, at time.Time) error {
		mu.Lock()
		active++
		mu.Unlock()
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	}, nil)
	s.Refresh()
	s.tick(time.Date(2025, 1, 1, 9, 30, 0, 0, time.Local))
	s.waitRunners(&mu, &active)
	s.tick(time.Date(2025, 1, 1, 9, 31, 0, 0, time.Local))
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if active != 1 {
		t.Fatalf("concurrent runs: %d", active)
	}
	mu.Unlock()
	close(release)
}

func TestScheduler_DisabledNotRegistered(t *testing.T) {
	// Given 包禁用 When Refresh Then 任务表为空
	src := &fakeSource{
		pkgs:     map[string]*plugin.Package{"p": hooksPkg("p", plugin.HooksTask{Name: "t", Cron: "* * * * *"})},
		disabled: map[string]bool{"p": true},
	}
	s := New(src, func(string, plugin.HooksTask, time.Time) error { return nil }, nil)
	s.Refresh()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.scheds) != 0 {
		t.Fatalf("disabled package registered: %v", s.scheds)
	}
}

func TestScheduler_TasksFromEnabledPackagesRegistered(t *testing.T) {
	// Given 启用包 a(含任务)与无任务包 b When Refresh Then 仅 a 注册
	src := &fakeSource{pkgs: map[string]*plugin.Package{
		"a": hooksPkg("a", plugin.HooksTask{Name: "t", Cron: "*/5 * * * *"}),
		"b": hooksPkg("b"),
	}}
	s := New(src, func(string, plugin.HooksTask, time.Time) error { return nil }, nil)
	s.Refresh()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.scheds) != 1 {
		t.Fatalf("scheds: %v", s.scheds)
	}
	if _, ok := s.scheds["a/t"]; !ok {
		t.Fatalf("missing a/t: %v", s.scheds)
	}
}

func TestScheduler_RunStopsCleanly(t *testing.T) {
	// Given Run 阻塞 When Stop Then 主循环退出
	src := &fakeSource{pkgs: map[string]*plugin.Package{}}
	s := New(src, func(string, plugin.HooksTask, time.Time) error { return nil }, nil)
	done := make(chan struct{})
	go func() { s.Run(); close(done) }()
	time.Sleep(30 * time.Millisecond)
	s.Stop()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
}
