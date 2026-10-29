package scheduler

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// 场景一：Given 每分钟任务已执行结束，When 下一分钟 tick，Then 同一任务再次执行。
// 可证伪目标：遗漏执行结束后的 busy 清理会阻止下一轮 runner 调用。
func TestScheduler_Round3_CompletedTaskRunsOnNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type invocation struct {
			name string
			at   time.Time
		}
		calls := make(chan invocation, 2)
		src := &fakeSource{pkgs: map[string]*plugin.Package{
			"p": hooksPkg("p", plugin.HooksTask{Name: "repeat", Cron: "* * * * *"}),
		}}
		s := New(src, func(pkgName string, task plugin.HooksTask, at time.Time, _ string) error {
			calls <- invocation{name: pkgName + "/" + task.Name, at: at}
			return nil
		}, nil)
		s.Refresh()
		first := time.Date(2025, time.January, 1, 9, 30, 0, 0, time.UTC)
		s.tick(first)
		synctest.Wait()
		select {
		case got := <-calls:
			if got.name != "p/repeat" || !got.at.Equal(first) {
				t.Fatalf("first invocation = %+v, want p/repeat at %v", got, first)
			}
		default:
			t.Fatal("first tick did not execute the task")
		}

		next := first.Add(time.Minute)
		s.tick(next)
		synctest.Wait()
		select {
		case got := <-calls:
			if got.name != "p/repeat" || !got.at.Equal(next) {
				t.Fatalf("next invocation = %+v, want p/repeat at %v", got, next)
			}
		default:
			t.Fatal("completed task did not execute on the next tick")
		}
		if len(calls) != 0 {
			t.Fatalf("unexpected extra invocations: %d", len(calls))
		}
	})
}

// 场景二：Given 多个不同到期任务且 runner 阻塞，When tick 后逐个释放，Then 同时执行不超过两个且排队任务随后全部执行。
// 可证伪目标：增大 worker 容量会突破行为上限，减少容量或丢弃排队任务会缺少预期调用。
func TestScheduler_Round3_WorkerLimitAndQueuedTasksRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tasks := []plugin.HooksTask{
			{Name: "a", Cron: "* * * * *"},
			{Name: "b", Cron: "* * * * *"},
			{Name: "c", Cron: "* * * * *"},
			{Name: "d", Cron: "* * * * *"},
		}
		release := make(chan struct{})
		started := make(chan string, len(tasks))
		var mu sync.Mutex
		active, peak, completed := 0, 0, 0
		src := &fakeSource{pkgs: map[string]*plugin.Package{"p": hooksPkg("p", tasks...)}}
		s := New(src, func(pkgName string, task plugin.HooksTask, _ time.Time, _ string) error {
			mu.Lock()
			active++
			if active > peak {
				peak = active
			}
			mu.Unlock()
			started <- pkgName + "/" + task.Name
			<-release
			mu.Lock()
			active--
			completed++
			mu.Unlock()
			return nil
		}, nil)
		defer func() {
			close(release)
			synctest.Wait()
		}()
		s.Refresh()
		s.tick(time.Date(2025, time.January, 1, 9, 30, 0, 0, time.UTC))
		synctest.Wait()
		if got := len(started); got != 2 {
			t.Fatalf("initial runner calls = %d, want 2 while other tasks queue", got)
		}
		seen := make(map[string]bool, len(tasks))
		record := func() {
			t.Helper()
			select {
			case name := <-started:
				if seen[name] {
					t.Fatalf("task executed more than once: %s", name)
				}
				seen[name] = true
			default:
				t.Fatal("queued task did not start after a worker was released")
			}
		}
		record()
		record()
		for range len(tasks) - 2 {
			select {
			case release <- struct{}{}:
			default:
				t.Fatal("no running worker available to release")
			}
			synctest.Wait()
			if got := len(started); got != 1 {
				t.Fatalf("new runner calls after releasing one worker = %d, want 1", got)
			}
			record()
		}
		for _, name := range []string{"p/a", "p/b", "p/c", "p/d"} {
			if !seen[name] {
				t.Errorf("task did not execute: %s", name)
			}
		}
		mu.Lock()
		gotActive, gotPeak, gotCompleted := active, peak, completed
		mu.Unlock()
		if gotActive != 2 || gotPeak != 2 || gotCompleted != 2 {
			t.Fatalf("active/peak/completed = %d/%d/%d, want 2/2/2", gotActive, gotPeak, gotCompleted)
		}
	})
}

// 场景三：Given 任务的下次触发时刻在下一分钟，When 当前分钟 tick，Then 不执行，直到下一分钟才执行。
// 可证伪目标：将到期判断放宽到 due.Add(TickInterval) 会造成 runner 提前收到调用。
func TestScheduler_Round3_FutureMinuteDoesNotRunEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(chan time.Time, 2)
		src := &fakeSource{pkgs: map[string]*plugin.Package{
			"p": hooksPkg("p", plugin.HooksTask{Name: "future", Cron: "31 9 * * *"}),
		}}
		s := New(src, func(_ string, _ plugin.HooksTask, at time.Time, _ string) error {
			calls <- at
			return nil
		}, nil)
		s.Refresh()
		s.tick(time.Date(2025, time.January, 1, 9, 30, 0, 0, time.UTC))
		synctest.Wait()
		select {
		case at := <-calls:
			t.Fatalf("future task executed early at %v", at)
		default:
		}

		due := time.Date(2025, time.January, 1, 9, 31, 0, 0, time.UTC)
		s.tick(due)
		synctest.Wait()
		select {
		case at := <-calls:
			if !at.Equal(due) {
				t.Fatalf("execution time = %v, want %v", at, due)
			}
		default:
			t.Fatal("task did not execute when its scheduled minute arrived")
		}
		if len(calls) != 0 {
			t.Fatalf("unexpected extra invocations: %d", len(calls))
		}
	})
}

// 场景四：Given Next 任务同时携带有效 cron 并存在普通 cron 对照任务，When 到期 tick，Then 仅普通任务进入 runner。
// 可证伪目标：遗漏 Next 分支跳过会使自调度任务进入 cron 执行。
func TestScheduler_Round3_NextTaskWithValidCronDoesNotRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := make(chan string, 2)
		src := &fakeSource{pkgs: map[string]*plugin.Package{
			"p": hooksPkg("p",
				plugin.HooksTask{Name: "selfScheduled", Next: true, Cron: "* * * * *"},
				plugin.HooksTask{Name: "cron", Cron: "* * * * *"},
			),
		}}
		s := New(src, func(pkgName string, task plugin.HooksTask, _ time.Time, _ string) error {
			calls <- pkgName + "/" + task.Name
			return nil
		}, nil)
		s.Refresh()
		s.tick(time.Date(2025, time.January, 1, 9, 30, 0, 0, time.UTC))
		synctest.Wait()
		if got := len(calls); got != 1 {
			t.Fatalf("cron runner calls = %d, want only the ordinary cron task", got)
		}
		if got := <-calls; got != "p/cron" {
			t.Fatalf("cron runner executed %q, want p/cron", got)
		}
	})
}
