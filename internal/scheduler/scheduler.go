// Package scheduler 宿主定时调度:分钟级 tick,执行已启用包的 hooks 任务
package scheduler

import (
	"log"
	"sync"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// Workers 并发 worker 数(防单包脚本阻塞全局)
const Workers = 2

// TickInterval tick 间隔(对齐分钟)
const TickInterval = 60 * time.Second

// Runner 单任务执行器(宿主注入:构建 HooksRuntime 并 RunTask)
type Runner func(pkgName string, task plugin.HooksTask, at time.Time) error

// PackageSource 任务声明来源(插件注册中心最小面)
type PackageSource interface {
	ListPackages() []string
	GetPackage(name string) (*plugin.Package, error)
	IsEnabled(name string) bool
}

// Scheduler 分钟级调度器
type Scheduler struct {
	src     PackageSource
	run     Runner
	now     func() time.Time
	scheds  map[string]cronEntry // "pkg/task" → 入口
	mu      sync.Mutex
	busy    map[string]bool
	sem     chan struct{}
	stop    chan struct{}
	stopped sync.WaitGroup
}

type cronEntry struct {
	pkgName string
	task    plugin.HooksTask
	sched   interface{ Next(time.Time) time.Time }
}

// New 构造;Run 前 Refresh 一次
func New(src PackageSource, run Runner, now func() time.Time) *Scheduler {
	if now == nil {
		now = time.Now
	}
	s := &Scheduler{
		src:    src,
		run:    run,
		now:    now,
		scheds: map[string]cronEntry{},
		busy:   map[string]bool{},
		sem:    make(chan struct{}, Workers),
		stop:   make(chan struct{}),
	}
	s.stopped.Add(1)
	return s
}

// Refresh 重建任务表(凡已启用包均注册;与是否被上游引用无关)
func (s *Scheduler) Refresh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scheds = map[string]cronEntry{}
	for _, name := range s.src.ListPackages() {
		if !s.src.IsEnabled(name) {
			continue
		}
		pkg, err := s.src.GetPackage(name)
		if err != nil || pkg.Manifest.Parts.Hooks == nil {
			continue
		}
		for _, tk := range pkg.Manifest.Parts.Hooks.Tasks {
			sched, err := plugin.CronSchedule(tk.Cron)
			if err != nil {
				log.Printf("scheduler: package %s task %s: %v", name, tk.Name, err)
				continue
			}
			s.scheds[name+"/"+tk.Name] = cronEntry{pkgName: name, task: tk, sched: sched}
		}
	}
}

// Run 主循环(阻塞;每 tick 对齐分钟);时钟注入点
func (s *Scheduler) Run() {
	defer s.stopped.Done()
	s.Refresh()
	for {
		now := s.now()
		next := now.Truncate(TickInterval).Add(TickInterval)
		select {
		case <-s.stop:
			return
		case <-time.After(next.Sub(now)):
		}
		s.tick(next)
	}
}

// Stop 请求退出(Run 返回后资源释放)
func (s *Scheduler) Stop() { close(s.stop) }

// Wait 等待主循环退出
func (s *Scheduler) Wait() { s.stopped.Wait() }

// tick 触发到期任务(同包同任务串行:上一轮未结束跳过;misfire 不补偿——只看最近一次触发点)
func (s *Scheduler) tick(due time.Time) {
	window := due.Add(-TickInterval)
	type dueTask struct {
		key string
		e   cronEntry
	}
	var dueTasks []dueTask
	s.mu.Lock()
	for key, e := range s.scheds {
		if s.busy[key] {
			log.Printf("scheduler: %s skipped (previous run in progress)", key)
			continue
		}
		n := e.sched.Next(window)
		if n.After(due) {
			continue
		}
		s.busy[key] = true
		dueTasks = append(dueTasks, dueTask{key: key, e: e})
	}
	s.mu.Unlock()
	for _, d := range dueTasks {
		d := d
		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.busy, d.key)
				s.mu.Unlock()
			}()
			s.sem <- struct{}{}
			defer func() { <-s.sem }()
			if err := s.run(d.e.pkgName, d.e.task, due); err != nil {
				log.Printf("scheduler: %s: %v", d.key, err)
			}
		}()
	}
}
