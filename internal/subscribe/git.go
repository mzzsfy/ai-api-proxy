package subscribe

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// gitTimeout 单次 git 命令超时上限(黑洞远端防挂死;持全局锁时挂起会阻塞全部订阅)
const gitTimeout = 5 * time.Minute

// writeErrorTimeout 失败态落库的独立超时(与请求 ctx 解耦)
const writeErrorTimeout = 10 * time.Second

// git 执行 git CLI(dir 空 = 全局命令;ctx + 超时);错误附输出(git 语义最可靠,项目零 git 库依赖)
func git(ctx context.Context, dir string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, gitTimeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
