# ai-api-proxy

插件化 LLM API 反向代理:把私有协议(opencode/zcode 等)的模型网关转换为标准 chat/message API 供主流客户端使用。

**执行模型**:单路直发,恰一次上游请求——失败即终局透传(上游状态原样返回),无重试/无切换/无熔断(重试策略由调用方实现)。

## 快速开始

```bash
cp config.example.yaml config.yaml   # 配置 api_keys 与上游
go run ./cmd/ai-api-proxy            # 默认 :8080
```

- 反代入口:`POST /v1/chat/completions`(Bearer)、`POST /v1/messages`(x-api-key)、`GET /v1/models`
- 管理 GUI:`http://127.0.0.1:8080/admin`(口令见启动日志或 config)
- 健康检查:`GET /healthz`

## 核心概念

- **渠道**:上游接入配置的核心实体(管理台展示为"渠道";存储与管理 API 仍使用 `model_groups` / `/admin/api/groups` 命名),承载协议插件、渠道参数覆盖与启用状态;传输实例在全局 `transports` 配置,渠道参数以 `transport` 键引用。请求仍通过标准 `model` 字段选择模型
- **渠道模型**:`{name, enabled}` 路由成员(管理 API 为 `/admin/api/groups/{id}/members`),只定义该渠道可路由的模型名与启用状态,不持有插件或参数。请求的 `model` 可传 `渠道前缀/模型名` 全路径(定向该渠道),或传模型短名;短名在多个启用渠道重名时跨渠道轮询。`/v1/models` 返回名格式由 `models_listing` 决定
- **参数解析**:插件 settings 声明的 `default` 为底值,包级插件参数覆盖默认值,渠道参数覆盖前两者(渠道 > 插件参数 > settings default);声明类型/枚举在合并后校验,`required` 仅请求期检查(缺失 → 502)
- **插件包(.aap,zip)**:三层插槽的可选实现集合——protocol(协议适配,渠道必须绑定)/ filters(请求修改,串行);
  参数槽由包内族文件的 settings 片段统一声明(同名槽拒装);密钥独立成包级 keys(util.key 直读,轮换立即生效)
- **声明式单协议**:协议部件在 manifest 声明唯一协议槽(`openai-completions`|`anthropic-messages`)与可处理形态;
  入口协议或形态不符时路由直接 400 拒绝(逐行注明拒因),不做转换、不做兜底。管道权威格式 = 声明协议格式,无中间格式
- **快速路径**:openai 入口 ∧ 内置协议工厂 ∧ 空 filter 链 → 字节直发(不重序列化)
- **runtime 池**:JS hook 粒度借还(池 = GOMAXPROCS,env `API_PROXY_POOL_SIZE` 逃生),排队 5s 超时 → 503
- **仓库订阅**:管理 GUI 登记私有 git 仓库(HTTPS + PAT),手动同步导入仓库内包目录(monorepo 填子路径,单包仓库填 `.`);
  包带 `subscription/<名>` 来源标记,与本地上传互斥覆盖;删除订阅联动卸载其包并清理克隆工作区(`data/subscriptions/`)

## 配置

`config.yaml` > 环境变量(`API_PROXY_` 前缀) > 默认值。字段:

| 字段 | 说明 | 默认 |
|------|------|------|
| listen | 监听地址 | `:8080` |
| data_dir | SQLite 数据目录 | `./data` |
| api_keys | 反代入口 Bearer key(必填,空拒绝启动) | - |
| admin_user / admin_pass_bcrypt | 管理账户;口令支持明文或 bcrypt 哈希(`$2a$`/`$2b$`/`$2x$`/`$2y$` 开头按哈希解析,以此开头的明文口令不可用),空则启动生成随机口令打印一次 | admin / 随机 |
| plugins_dir | 普通插件目录(裸 .aap 或含 manifest.json 的包目录)启动时自动导入(幂等) | `./plugins` |
| builtin_dir | 内置插件目录(随分发的预置包;只补缺,不覆盖用户同名包) | `./builtin-plugins` |
| history_retention_days | 请求历史保留天数(独立库 `history.db`;管理 GUI"请求历史"页;<0 永久保留,0 视为未配置) | 7 |
| log_level | 日志级别(log/slog,按级别过滤;非法值拒绝启动) | info |
| models_listing | `/v1/models` 列表模式:full(全路径)| short(短名,同名轮询)| both(全路径 ∪ 短名) | full |
| pack_dir | 非空 = 只打包不服务:把上述两级目录打成 `<包名>.aap` 落到此目录 | 空(服务模式) |
| transports | 命名传输实例,请求载体 transport 槽按名引用;url scheme 判型:direct(字面值)/ `http(s)://` / `socks5://` / `aap://`(代理节点)| `ipp+<kind>://`(出口 IP 供给方,见 internal/ipprovider) | direct |

## 插件开发

- 类型与宿主面:`sdk/ai-api-proxy.d.ts`;本地测试宿主:`sdk/test.js`(与 Go host 语义一致)
- 起步模板:管理 GUI → 包 → 下载模板包;或 `GET /packages/template`
- 示例包:`sdk/examples/gemini`(v2:factory 形态 + settings 片段声明 + util.key)
- 测试:`node --test plugins/*/test/*.test.js`(插件库根执行)
- 管理包:导入(文件或 URL)/ 导出 / 卸载
- 目录热载:`config.plugins_dir` 内 `.aap` 或包目录启动时自动导入(幂等);`config.pack_dir` 非空则只打包不服务

## 开发

```bash
go test -race ./...    # Go 全量
go vet ./...
```

目录:`internal/`(server 装配根/gateway/convert/pipeline/plugin/upstream/builtin/metrics/history/admin/adminweb/store/transport)、`web/`(管理台前端源码)、`sdk/`(镜像内插件 SDK 快照)。

### 管理台前端(web/)

Vue3 + TypeScript + naive-ui + vite,构建产物输出 `internal/adminweb/dist` 由 `go:embed` 嵌入单二进制,`go build` 前需先构建前端:

```bash
cd web && bun install && bun run build   # 或 pnpm install && pnpm build
```

开发模式:`cd web && bun dev`(代理 `/admin/api` 到本地 18080),访问 `http://localhost:5173`。

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

