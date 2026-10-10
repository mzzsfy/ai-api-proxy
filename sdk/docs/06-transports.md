# 06 · 传输配置:transports 命名实例

宿主的全部出站走"命名传输实例":`config.yaml` 的 `transports` 列表声明实例,协议部件经参数槽、handler 经 `ctx.http` 的 `transport` 字段按名引用。本文面向部署/配置者与包作者;示例配置见 `configexample/config.example.yaml`。

## 一、配置方式

```yaml
transports:                    # 列表;每项 = 一个命名实例
  - name: my-proxy             # 必填;实例名,上游按名引用
    url: "..."                 # 判型见下表
    options: {}                # 可选;仅 aap 允许 affinity_ttl,其余 scheme 必须为空
```

### url scheme 判型表

| url 形态 | 类型 | 说明 |
|---|---|---|
| `direct` | 直连 | 字面值,精确匹配(大小写敏感) |
| `http://…` / `https://…` | HTTP 代理 | 标准库 Proxy 包装,支持 `user:pass@` |
| `socks5://…` | SOCKS5 代理 | 支持 `user:pass@` 认证 |
| `aap://host:port?token=…` | 插件化代理节点 | aap 协议,会话亲和;见下文 |
| `ipp+<kind>://<参数>` | IP 供给方 | kind 须已注册,见下文 |

规则:

- 非 `direct` 的 url 必须含 `://` 且 scheme 后非空,否则拒绝启动
- `aap://` 的 url 必须含 `token=` 且 token 长度 1..255
- `ipp+<kind>://` 的 kind 须已注册(未注册 = 缺构建支持),否则拒绝启动
- options 白名单:仅 `aap://` 允许携带 `affinity_ttl`;其他 scheme 带 options、或 aap 带未知键,均拒绝启动
- `direct` 实例无需声明——运行时保证名为 `direct` 的默认实例必然存在;显式声明同形体亦合法
- 实例名不可重复;启动期全部构建,任一失败整体拒绝启动并级联关闭已建供给方

## 二、direct / http / socks5

普通代理传输,无供给方语义(无会话亲和、无轮换、无租约):

```yaml
transports:
  - name: cn-relay
    url: "http://user:pass@host:port"
  - name: socks-relay
    url: "socks5://user:pass@host:port"
```

- http(s)/socks5 均支持 url 内嵌认证;TLS 指纹拨号包装
- 出站请求缺省补齐浏览器头组(UA 等);插件可传 `X-Aap-Browser-Defaults: off` 关闭除 UA 外的缺省头(UA 任何路径必注入)

## 三、aap:插件化代理节点

`aap://host:port?token=xxx` 对接 aap 协议节点(SOCKS5 式定长二进制握手 + TUNNEL,rep=0 后为裸字节双向流;协议权威规范另见 docs/ai-api-proxy/feat/aap-protocol.md)。

```yaml
transports:
  - name: aap-out
    url: "aap://node.example:7000?token=YOUR_TOKEN"
    options:
      affinity_ttl: 86400     # 可选;亲和 TTL 秒;省略或 0 = 默认 86400;负值拒绝启动
```

### 会话亲和与 affinity_ttl

- 亲和键 = `sha256(apiKey + NUL + model)` 前 16 字节;model 为空退化为仅 apiKey 素材。同一键的请求尽量落在同一出口
- TTL 随每次 TUNNEL 请求下发给节点(秒);节点侧据此维持 api key + 模型 → 出口 的粘性绑定,到期回收
- 每 TUNNEL 都重申 TTL,活跃会话持续续期

### 连接与握手行为

- 每请求一条连接:握手 + TUNNEL 成功(rep=0)后该连接即目标字节流,不做连接复用
- 握手/应答时限 5 秒(不含数据阶段);token 以单字节长度前缀传输,故上限 255 字节
- 握手 auth 失败连续累计,达上限后本地将该节点标记不可用并进入冷却期;冷却到期自动重探,握手成功即恢复并清零计数

### lease 与轮换语义

- Acquire 是轻对象,连接语义全部在 Dial;Release 幂等,宿主保证每请求必归还(成功路径随响应体 Close 归还)
- 节点 rep 码:0 = 已连通;1 = 出口不可用/目标连通失败;2 = 节点并发上限;3 = 请求含节点不支持的能力
- 恰一次边界:请求字节一经发往上游绝不重发。仅"连接未建立(零字节触达)"的失败可换出口重试,固定预算 1;TLS 握手启动或请求写出后的任何错误终局透传
- rep=1 且请求未写出:携带失败 lease_id 进 exclude 集换出口重试一次;rep=1/2 终局映射 503,rep=3 映射 502
- aap 供给方能力声明恒为:可轮换 + 支持亲和

### 管理面失效(evict)

- aap 传输实例支持管理台"失效命令"(协议 A3):scope=lease 按 lease_id 失效单绑定,scope=egress 按出口地址失效;幂等,目标不存在亦成功
- 插件可经全局 `util.evict(transport, scope, value)` 上报失效

### 上游侧要求

节点须实现 aap 协议:握手(校验 token)→ TUNNEL(ttl/session_key/exclude/目标地址;rep 应答含 lease_id 与 expires)→ 裸字节双向流;可选实现 A3 evict。

## 四、ipp+ 供给方传输

> ⚠ 现状警示:`Validate` 当前只允许 aap 传输携带 options(见第六节"仅 aap 传输支持 options"),因此**下列 ipp+ 示例配置经 config.yaml 启动会被拒绝**。ipp+ 的 options 仅代码构造路径(`TransportDef.ProviderCfg.Options`)可用;待校验放宽后本节示例即恢复正常用法。

`ipp+<kind>://<参数>` 接入出口 IP 供给方。kind 经注册表静态注册,当前内置:

| kind | 注册名 | 出口形态 | 能力 |
|---|---|---|---|
| `clash` | `ipp_clash` | 既有 clash/mihomo 混合端口,单出口 | 不可轮换,不支持亲和 |
| `remote` | `ipp_remote` | 远程供给 API v1,代理池 | 以服务端 lease 级能力为准 |

全部供给方为无条件注册(无构建标签裁剪);新增 kind = 实现 Provider 契约并 `Register("ipp_<kind>", …)`。

供给方状态目录:`data_dir/ipp/<实例名>/`,按实例隔离。

### ipp+clash

```yaml
transports:
  - name: clash-out
    url: "ipp+clash://clash"     # // 后参数段当前仅占位,实际配置全在 options
    options:
      type: http_proxy           # http_proxy(混合端口 http 形态)| socks5
      url: "http://127.0.0.1:6654"
```

- 单出口直连既有 clash 混合端口,无池无轮换:出口 IP 不可见(Dial 失败不重试,直接透传错误)
- 不支持会话亲和(Hint 被忽略)

### ipp+remote

```yaml
transports:
  - name: remote-out
    url: "ipp+remote://remote"   # // 后参数段当前仅占位,实际配置全在 options
    options:
      base: "https://provider.example"   # 必填;必须 https(allow_insecure: true 可放行 http;api_key 与流量将明文传输,仅测试)
      api_key: "sk-..."                  # 必填;Bearer 凭据
      can_rotate_ip: true                # 静态能力声明;与服务端不一致时以服务端为准(记日志)
      session_affinity: true
```

- 协议:`POST /acquire`(body 带 hex session_key;返回 lease_id / proxy{type,url} / egress_ip / capabilities)→ 经返回的 socks5/http 代理拨号 → `POST /release` 归还 → `GET /stats` 观测
- acquire 网络/5xx 按无出口处理(503);401/403 报鉴权失败
- Release 异步通知供给方(不阻塞响应路径,SSE 长流不受 RPC 超时牵连);失败仅记日志,由服务端 TTL 兜底
- 会话亲和:session_key 以 hex 文本下发,服务端尽量同出口;是否生效以服务端能力为准

### 供给方路径公共语义

- 每请求 Acquire → 经 lease 拨号;恰一次边界与 aap 相同(零字节触达可换出口重试预算 1,前提是供给方声明可轮换)
- 无出口(ErrNoExits)映射 503

## 五、上游如何引用传输实例

- 协议部件:组参数槽 `transport`(字符串)= 实例名;请求的 `Transport` 字段由适配器按参数槽填入,空 = `direct`
- handler 部件:`ctx.http.request/open` 的 `transport` 字段 = 实例名;省略 = 组参数解析结果的传输槽
- 引用了不存在的实例名:请求失败(502,错误含 transport 定位字段)
- 供给方类实例(aap/ipp+)下,亲和素材 = apiKey + model(handler 可经 `sessionKey` 自定义素材)

## 六、常见错误对照

| 报错 | 原因 |
|---|---|
| `transports[N]: name required` | 实例缺 name |
| `transports[<name>]: url 须为 direct 或带 scheme 的完整地址` | url 非 `direct` 字面值且缺 `://` 或 scheme 后为空 |
| `transports[<name>]: unknown url scheme "<s>"` | scheme 不在判型表内 |
| `transports[<name>]: aap url 须含 token` | `aap://` 缺 `token=` |
| `transports[<name>]: aap token 长度须 1..255` | token 为空或超 255 字节 |
| `transports[<name>]: provider "<kind>" not registered(缺构建标签或未注册)` | `ipp+<kind>` 未注册 |
| `transports[<name>]: 仅 aap 传输支持 options` | 非 aap 传输带 options |
| `transports[<name>]: unknown aap option "<k>"` | aap options 出现 affinity_ttl 以外的键 |
| `aap <name>: affinity_ttl 须 ≥0` | 负值 TTL(构建期) |
| `ipp_clash <name>: type 须为 http_proxy|socks5` | type 非法 |
| `ipp_clash <name>: url required` | options 缺代理地址 |
| `ipp_remote <name>: base 必须 https(测试场景显式 allow_insecure)` | base 非 https |
| `ipp_remote <name>: api_key required` / `base required` | remote options 缺必填 |
| `transport "<n>" not found` | 上游引用了未声明的实例名(请求期,502) |
| options 未知字段报错 | options 经供给方结构体严格解码,未知键拒绝 |

注意:options 的供给方级校验(clash/remote 的 type/url/base 等)发生在实例构建期——启动即失败,不是运行期。但 config.yaml 路径下这些错误**当前不可达**:更早的 `Validate` 已因"仅 aap 传输支持 options"拒绝带 options 的 ipp+ 传输;下表 ipp_clash/ipp_remote 各条仅在绕过 `Validate` 的代码构造路径出现。

## 七、实现索引

文中行为断言的代码依据(file:锚点,相对仓库根):

- internal/server/config.go:24 `TransportCfg` 结构(name/url/options)
- internal/server/config.go:195-206 transports 校验入口(name 必填、url、options 三段)
- internal/server/config.go:210-244 `validateTransportURL`:direct 精确匹配、scheme 白名单、aap token 必含与 1..255、ipp+<kind> 注册校验
- internal/server/config.go:247-267 `validateTransportOptions`:仅 aap 允许 affinity_ttl,其余 scheme 必须为空
- internal/transport/transport.go:57-60 `direct` 默认实例必然存在
- internal/transport/transport.go:172-236 `build`:各 scheme 构造;aap/ipp+ 走 ipprovider.Create;ipp+ 参数段拆 kind,`//` 后内容原样传入 ProviderCfg.URL
- internal/transport/transport.go:255-261 `sessionKey`:sha256(apiKey+NUL+model) 前 16 字节;model 空退化
- internal/transport/transport.go:274-294 浏览器缺省头组与 `X-Aap-Browser-Defaults: off`(UA 豁免)
- internal/transport/transport.go:362-454 `ipTransport.RoundTrip/RoundTripRaw`:恰一次边界、重试预算 1、rep=1 exclude 重试、rep 终局映射(503/502)、租约随响应体归还
- internal/transport/transport.go:89-99 `Evict`:非供给方传输返回 false(映射 400)
- ipprovider/aap.go:60-77 握手 5s 时限、auth 失败上限 3 次、60s 冷却、默认 TTL 86400
- ipprovider/aap.go:121-141 token ≤255(握手 u8 长度上限)
- ipprovider/aap.go:155-157 aap 能力恒为可轮换 + 亲和
- ipprovider/aap.go:290-379 aapLease.Dial:每请求握手+TUNNEL、TTL 下发、exclude、rep 应答
- ipprovider/aap.go:168-189 A3 evict 命令(幂等)
- ipprovider/contract.go:3-5 恰一次边界(契约级)
- ipprovider/contract.go:52-74 Excluder/RepError/Evictor 契约
- ipprovider/registry.go:29-48 Register/Create:静态注册表,未知 kind 报 ErrUnknownKind
- ipprovider/clash.go:11-13 `ipp_clash` 注册;:42-44 能力(不可轮换、无亲和);:69 出口 IP 不可见
- ipprovider/remote.go:19-21 `ipp_remote` 注册;:24-55 options(base https 强制、api_key、静态能力);:221-238 Release 异步归还
- ipprovider/options.go:17-31 options 严格解码(DisallowUnknownFields)
- internal/pipeline/executor.go:75-85 传输槽解析:空 = `direct`,名不存在 = 请求失败
- internal/pipeline/contract.go:110 `Transport` 字段语义
- internal/gateway/handler.go:91-100 组参数槽 `transport` 与 SessionKey(model)
- configexample/config.example.yaml:20-48 官方示例配置
- cmd/ai-api-proxy/main.go:20 启动期调用 Validate
