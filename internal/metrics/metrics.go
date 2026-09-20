// Package metrics 内存计数与分钟快照;读即重置快照
package metrics

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// MinuteFormat 分钟键格式(metrics_minutely.minute 主键口径)
const MinuteFormat = "2006-01-02T15:04"

// TargetKey 目标维度键 "{upstream}/{target}"
func TargetKey(upstream, target string) string { return upstream + "/" + target }

// UpstreamCounters 单上游计数
type UpstreamCounters struct {
	Requests   int64 `json:"requests"`
	Errors     int64 `json:"errors"`
	MaxConc    int64 `json:"max_concurrent"`
	concurrent int64
}

// Recorder 请求计数器;Cancel 不计入 errors(feat/metrics)
type Recorder struct {
	mu       sync.Mutex
	requests int64
	errors   int64
	current  int64
	maxConc  int64
	byUp     map[string]*UpstreamCounters
	byTarget map[string]*UpstreamCounters
}

// NewRecorder 构造
func NewRecorder() *Recorder {
	return &Recorder{
		byUp:     make(map[string]*UpstreamCounters),
		byTarget: make(map[string]*UpstreamCounters),
	}
}

// EnterRequest 请求进入:总并发+1;返回退出函数(幂等由调用方保证单次调用)
func (r *Recorder) EnterRequest() func() {
	r.mu.Lock()
	r.requests++
	r.current++
	if r.current > r.maxConc {
		r.maxConc = r.current
	}
	r.mu.Unlock()
	return func() { r.exit() }
}

func (r *Recorder) exit() {
	r.mu.Lock()
	r.current--
	r.mu.Unlock()
}

// IncError 错误计数(Cancel 调用方不调本方法)
func (r *Recorder) IncError() {
	r.mu.Lock()
	r.errors++
	r.mu.Unlock()
}

// IncUpstream 上游维度计数;failed 仅 5xx/传输类计错误
func (r *Recorder) IncUpstream(upstream string, failed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.byUp[upstream]
	if c == nil {
		c = &UpstreamCounters{}
		r.byUp[upstream] = c
	}
	c.Requests++
	if failed {
		c.Errors++
	}
}

// EnterTarget 目标请求开始;返回结束函数,failed 标记本次目标尝试失败
func (r *Recorder) EnterTarget(upstream, target string) func(failed bool) {
	key := TargetKey(upstream, target)
	r.mu.Lock()
	c := r.byTarget[key]
	if c == nil {
		c = &UpstreamCounters{}
		r.byTarget[key] = c
	}
	c.Requests++
	c.concurrent++
	if c.concurrent > c.MaxConc {
		c.MaxConc = c.concurrent
	}
	r.mu.Unlock()
	return func(failed bool) {
		r.mu.Lock()
		c.concurrent--
		if failed {
			c.Errors++
		}
		r.mu.Unlock()
	}
}

// Snapshot 读即重置:取走全部计数并归零
func (r *Recorder) Snapshot() (requests, errors, maxConc int64, byUp, byTarget map[string]*UpstreamCounters) {
	r.mu.Lock()
	defer r.mu.Unlock()
	requests, errors, maxConc = r.requests, r.errors, r.maxConc
	r.requests, r.errors, r.maxConc = 0, 0, 0
	byUp, byTarget = r.byUp, r.byTarget
	r.byUp = make(map[string]*UpstreamCounters)
	r.byTarget = make(map[string]*UpstreamCounters)
	return
}

// SnapshotRow 快照转一行持久化记录
type SnapshotRow struct {
	Minute     string
	Requests   int64
	Errors     int64
	MaxConc    int64
	ByUpstream map[string]*UpstreamCounters
	ByTarget   map[string]*UpstreamCounters
}

// Take 快照并封装为行;失败丢弃该分钟(不合并)由持久层决定
func (r *Recorder) Take(now time.Time) SnapshotRow {
	reqs, errs, conc, byUp, byTarget := r.Snapshot()
	return SnapshotRow{
		Minute:     now.Format(MinuteFormat),
		Requests:   reqs,
		Errors:     errs,
		MaxConc:    conc,
		ByUpstream: byUp,
		ByTarget:   byTarget,
	}
}

// UpstreamJSON 序列化 by_upstream
func (row SnapshotRow) UpstreamJSON() (string, error) {
	b, err := json.Marshal(row.ByUpstream)
	if err != nil {
		return "", fmt.Errorf("marshal by_upstream: %w", err)
	}
	return string(b), nil
}

// TargetJSON 序列化 by_target
func (row SnapshotRow) TargetJSON() (string, error) {
	b, err := json.Marshal(row.ByTarget)
	if err != nil {
		return "", fmt.Errorf("marshal by_target: %w", err)
	}
	return string(b), nil
}

// SnapshotLive 非重置实时读
func (r *Recorder) SnapshotLive() (requests, errors, concurrent int64, byUp, byTarget map[string]*UpstreamCounters) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.requests, r.errors, r.current, copyCounters(r.byUp), copyCounters(r.byTarget)
}

// copyCounters 计数表浅拷贝(值拷贝)
func copyCounters(src map[string]*UpstreamCounters) map[string]*UpstreamCounters {
	out := make(map[string]*UpstreamCounters, len(src))
	for k, v := range src {
		vc := *v
		out[k] = &vc
	}
	return out
}
