package subscribe

import (
	"fmt"
	"os/exec"
	"strings"
)

// git 执行 git CLI(dir 空 = 全局命令);错误附输出(git 语义最可靠,项目零 git 库依赖)
func git(dir string, args ...string) (string, error) {
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}
