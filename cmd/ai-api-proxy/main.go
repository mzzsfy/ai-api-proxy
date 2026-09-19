// ai-api-proxy 入口:只做装配;退出信号由 server.Run 内部处理
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/mzzsfy/ai-api-proxy/internal/server"
)

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()
	cfg, err := server.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}
	if err := server.Run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "server: %v\n", err)
		os.Exit(1)
	}
}
