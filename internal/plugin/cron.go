package plugin

import (
	"fmt"

	cron "github.com/robfig/cron/v3"
)

// CronSchedule 解析 5 字段 cron 表达式(仅表达式语义,调度循环自持)
func CronSchedule(expr string) (cron.Schedule, error) {
	if expr == "" {
		return nil, fmt.Errorf("cron required")
	}
	return cron.ParseStandard(expr)
}

// TaskTimeoutDefault 任务超时缺省
const TaskTimeoutDefault = 30 * 1000

// TaskTimeoutMax 任务超时上限
const TaskTimeoutMax = 5 * 60 * 1000

// OnLoadTimeout onLoad 超时
const OnLoadTimeout = 5 * 1000

// HttpBodyLimit ctx.http 响应体上限
const HttpBodyLimit = 1024 * 1024
