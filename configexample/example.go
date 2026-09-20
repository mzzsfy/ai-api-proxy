// Package configexample 内嵌示例配置,供启动时首次释放(缺失才写,不覆盖)
package configexample

import _ "embed"

//go:embed config.example.yaml
var Example []byte
