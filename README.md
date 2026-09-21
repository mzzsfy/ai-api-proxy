# ai-api-proxy

插件化 LLM API 反向代理:把私有协议(opencode/zcode 等)的模型网关转换为标准 chat/message API 供主流客户端使用。

**执行模型**:单目标直发,恰一次上游请求——失败即终局透传(上游状态原样返回),无重试/无熔断(重试策略由调用方实现)。多目标仅作手动启停管理。

## 快速开始

```bash
cp config.example.yaml config.yaml   # 配置 api_keys 与上游
go run ./cmd/ai-api-proxy            # 默认 :8080
```

- 反代入口:`POST /v1/chat/completions`(Bearer)、`POST /v1/messages`(x-api-key)、`GET /v1/models`
- 管理 GUI:`http://127.0.0.1:8080/admin`(口令见启动日志或 config)
- 健康检查:`GET /healthz`

## 核心概念

- **插件包(.aap,zip)**:三层插槽的可选实现集合——protocol(协议适配,主包必须)/ filters(请求修改,串行)/ targets 预设
- **上游 = 包的实例化**:主包 + 附加包(filters)+ 实例配置(模型列表/目标/参数/启停),经管理 GUI 或 API 创建
- **声明式单协议**:协议部件在 manifest 声明唯一协议槽(`openai-completions`|`anthropic-messages`)与可处理形态;
  入口协议或形态不符时主体直接 400 拒绝,不做转换、不做兜底。管道权威格式 = 声明协议格式,无中间格式
- **快速路径**:openai 入口 ∧ 内置协议 ∧ 空 filter 链 → 字节透传(不重序列化)
- **runtime 池**:JS hook 粒度借还(池 = GOMAXPROCS,env `API_PROXY_POOL_SIZE` 逃生),排队 5s 超时 → 503

## 配置

`config.yaml` > 环境变量(`API_PROXY_` 前缀) > 默认值。字段:

| 字段 | 说明 | 默认 |
|------|------|------|
| listen | 监听地址 | `:8080` |
| data_dir | SQLite 数据目录 | `./data` |
| api_keys | 反代入口 Bearer key(必填,空拒绝启动) | - |
| admin_user / admin_pass_bcrypt | 管理账户;口令空则启动生成随机口令打印一次 | admin / 随机 |
| plugins_dir | 普通插件目录(裸 .aap 或含 manifest.json 的包目录)启动时自动导入(幂等) | `./plugins` |
| builtin_dir | 内置插件目录(随分发的预置包;只补缺,不覆盖用户同名包) | `./builtin-plugins` |
| pack_dir | 非空 = 只打包不服务:把上述两级目录打成 `<包名>.aap` 落到此目录 | 空(服务模式) |
| transports | 命名传输实例(direct/http_proxy/socks5),target 按名引用 | direct |

## 插件开发

- 类型与宿主面:`sdk/ai-api-proxy.d.ts`;本地测试宿主:`sdk/test.js`(与 Go host 语义一致)
- 起步模板:管理 GUI → 包 → 下载模板包;或 `GET /packages/template`
- 示例包:`sdk/examples/gemini`(四 hook 全实现 + 编写说明)
- 测试:`node --test plugins/*/test/*.test.js`(插件库根执行)
- 管理包:GUI 在线编辑(保存热生效)/ 导入(文件或 URL)/ 导出 / 卸载
- 目录热载:`config.plugins_dir` 内 `.aap` 或包目录启动时自动导入(幂等);`config.pack_dir` 非空则只打包不服务

## 开发

```bash
go test -race ./...    # Go 全量
go vet ./...
```

目录:`internal/`(server 装配根/gateway/convert/pipeline/plugin/upstream/builtin/metrics/admin/adminweb/store/transport)、`sdk/`(镜像内插件 SDK 快照)。

## 部署

Docker 镜像由 CI 推送 GHCR(main 分支)。配置文件缺失时启动自动释放内嵌示例配置到该路径(已存在不覆盖)。**单目录挂载**:宿主一个目录映射到容器 `/data`,全部状态都在里面:

```
/data/
├── config/config.yaml   # 配置(缺失时启动自动释放示例)
├── lib/                 # 数据(SQLite/会话)
└── plugins/             # 插件包热载目录(可选)
```

```bash
mkdir -p /srv/ai-api-proxy/{config,lib,plugins}
# 不预置 config.yaml 也可:首次启动自动释放示例配置
# 编辑 config: data_dir: /data/lib  plugins_dir: /data/plugins

docker run -d --name ai-api-proxy \
  -p 8080:8080 \
  -e TZ=Asia/Shanghai \
  -v /srv/ai-api-proxy:/data \
  ghcr.io/mzzsfy/ai-api-proxy:latest
```

