// RSS 看门狗:进程级 OOM 兜底(goja 无内存配额 API)。软限=丢全部 handler 池+拒新 502+回落重建;硬限=软限×1.5 进程退出(仅独立进程部署启用)
package server

import (
	"context"
	"os"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/mzzsfy/ai-api-proxy/internal/plugin"
)

// watchdogPoll 看门狗轮询周期
const watchdogPoll = 5 * time.Second

// Watchdog RSS 阈值看门狗(阈值按物理内存绝对字节数,config 侧换算;0 = 关闭)
type Watchdog struct {
	Engines   *plugin.HandlerEngines
	SoftBytes uint64
	// HardBytes = 软限×1.5;>0 且独立进程部署 = 到限进程退出
	HardBytes   uint64
	Independent bool
	readRSS     func() uint64
	tripped     atomic.Bool
	// poll 轮询周期(缺省 watchdogPoll;测试可调小)
	poll time.Duration
}

// NewWatchdog 构造(readRSS 注入可测;totalMemory 缺省 sysTotalMemory)
func NewWatchdog(engines *plugin.HandlerEngines, softBytes, hardBytes uint64, independent bool, readRSS func() uint64) *Watchdog {
	return &Watchdog{Engines: engines, SoftBytes: softBytes, HardBytes: hardBytes, Independent: independent, readRSS: readRSS, poll: watchdogPoll}
}

// Run 轮询(阻塞;ctx 取消退出)
func (w *Watchdog) Run(ctx context.Context) {
	if w.SoftBytes == 0 || w.readRSS == nil {
		return
	}
	if w.poll <= 0 {
		w.poll = watchdogPoll
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(w.poll):
		}
		rss := w.readRSS()
		switch {
		case rss == 0:
			// 读取失败:跳过,不动 tripped(真实状态未知不误判回落)
		case w.HardBytes > 0 && w.Independent && rss >= w.HardBytes:
			// 硬限:进程退出交部署层 supervisor 拉起(库形态嵌入宿主不启用)
			slog.Error("watchdog: RSS over hard limit, exiting", "rss_bytes", rss, "hard_limit_bytes", w.HardBytes)
			exitProcess(1)
		case rss >= w.SoftBytes:
			if !w.tripped.Swap(true) {
				slog.Warn("watchdog: RSS over soft limit, dropping all handler pools and rejecting new requests", "rss_bytes", rss, "soft_limit_bytes", w.SoftBytes)
				w.Engines.SetRejectNew(true)
				w.Engines.CloseAll() // 闲置全弃;在借不 Interrupt,随归还即弃
			}
		default:
			if w.tripped.Swap(false) {
				slog.Info("watchdog: RSS back under soft limit, handler pools reopen", "rss_bytes", rss, "soft_limit_bytes", w.SoftBytes)
				w.Engines.SetRejectNew(false)
			}
		}
	}
}

// exitProcess 独立进程部署的退出动作(变量注入测试替身)
var exitProcess = os.Exit
