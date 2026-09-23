# 04 · 自定义代理协议(protocol 部件)

protocol 部件把**入口协议**翻译成**上游协议**。设计是"声明式单协议":一个部件服务且仅服务 manifest 声明的唯一协议槽,入口请求与槽位不符由宿主直接 400,部件不做任何探测或转换分支。

## 一、两个协议槽

| 槽位 | 入口格式 | 上游对接目标 |
|---|---|---|
| `openai-completions` | OpenAI Chat Completions 请求/响应 | 任何兼容 OpenAI 格式的上游,或经你翻译的其他上游 |
| `anthropic-messages` | Anthropic Messages 请求/响应 | Anthropic 或兼容上游 |

槽位只是**入口格式**的标识——上游实际是什么 API 完全由你的 buildRequest 决定。例:`gemini` 示例包声明 openai-completions 槽,内部把请求翻译成 Google GenerateContent。

## 二、四个钩子

```js
// @ts-check
/// <reference path="../ai-api-proxy.d.ts" />
module.exports = {
  // 必实现:入口原文 → 上游请求(同步)
  buildRequest: function (ctx, entry) {    var body = JSON.parse(entry);
    return {
      url: ctx.target.baseUrl + "/v1/chat/completions",
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer " + util.secret("api_key"),   // ref 必须在 manifest secretRefs 里
      },
      body: JSON.stringify(toUpstream(body)),
      stream: ctx.vars.entryStream,      // 分发意图,必须 ∈ manifest form 声明
    };
  },

  // streaming 形态声明时必实现:单帧 → 声明协议事件对象数组的 JSON 字符串
  mapEvent: function (ctx, event) {
    // event = {"event": "...", "data": "..."} 信封字符串(host 分帧)
    var f = JSON.parse(event);
    if (f.data === "[DONE]") return null;          // null/"[]"/"" = 跳帧
    return JSON.stringify([toOpenAIChunk(JSON.parse(f.data))]);   // 注意是数组
  },

  // non_streaming 形态声明时必实现:上游响应体 → 声明协议响应 JSON 字符串
  mapResponse: function (ctx, body) {
    return JSON.stringify(toOpenAIResponse(JSON.parse(body)));
  },

  // 可选:上游错误终局映射(一次;不实现 = 原样透传)
  mapError: function (ctx, status, body) {
    return JSON.stringify({ error: { message: extract(JSON.parse(body)) } });
  },
};
```

契约要点:

- `buildRequest(ctx, entry)`:entry 是**入口原文字节**(与声明协议同格式,未做任何转换);返回 `{url, method, headers, body, stream}`。宿主执行该请求——恰一次、失败即终局、无重试(重试语义归下游客户端)
- `mapEvent(ctx, event)`:host 按 SSE 分帧,入参恒为 `{"event","data"}` 信封;返回**事件对象数组的 JSON 字符串**(一帧可产多事件);收尾帧宿主负责——openai 槽自动补 `data: [DONE]`,anthropic 槽无收尾;单帧失败降级原样透传并告警,流不中断
- `mapResponse(ctx, body)`:上游完整响应体 → 声明协议响应体
- 管道顺序:buildRequest → 上游 → (流式:mapEvent 逐帧 / 非流式:mapResponse)→ filters 逆序(mapChunk/mapResponse)

## 三、manifest 声明

```jsonc
"parts": {
  "protocol": {
    "protocol": "openai-completions",            // 唯一槽位;文件路径约定 protocol.js(manifest 不写 entry)
    "features": ["tools", "vision"],             // 能力标记(入口请求携带未声明能力 → 400)
    "secretRefs": ["api_key"],                   // util.secret 白名单
    "configSchema": { ... }                      // 部件实例参数(可选)
  }
}
```

**形态由实现推导,manifest 不声明**:`mapEvent` 导出 = 支持流式;`mapResponse` 导出 = 支持非流式;两者都没有 → 拒装。入口请求的流形态不被支持、协议槽不符、携带未声明 features,运行期宿主一律 400,不做转换。

## 四、实例参数(configSchema)与 factory 形态

同一部件被多个上游引用时,每个上游可有不同参数(如 apiVersion)。两种写法:

```js
// factory 形态:宿主用上游实例参数调用一次,返回 hooks
module.exports = function (config) {
  var ver = config.apiVersion || "v1beta";
  return {
    buildRequest: function (ctx, entry) {
      return { url: ctx.target.baseUrl + "/" + ver + "/models:generateContent", /* ... */ };
    },
    // ...
  };
};
```

```jsonc
// manifest 里声明参数 schema(用户建上游时在 GUI 填写)
"configSchema": {
  "type": "object",
  "properties": {
    "apiVersion": { "type": "string", "description": "API 版本路径段(默认 v1beta)" }
  }
}
```

- factory 入参 `config` = 用户填的参数对象,另含宿主注入的 `config.protocol`(本部件声明的槽位)
- **不要在 configSchema 里声明 `protocol` 键**——参数剥离会把宿主注入值删掉
- 无参部件用对象导出形态即可;`ctx.target` 里有 baseUrl/models 等上游事实

## 五、protocol 部件与 hooks 的边界

| | protocol | hooks |
|---|---|---|
| 执行时机 | 每个请求热路径 | 定时/加载/凭据操作 |
| 网络 | 无(host 执行唯一上游请求) | 有(ctx.http.run 限出站) |
| 参数 | configSchema(经 config/Upstream.Params 注入) | settings 声明(经 ctx.settings 注入) |
| 凭据 | util.key() 只读当前注入键 data | ctx.keys.set/merge 写当前键;keySubmit 创建条目 |

protocol/filter 里不要刷 token——那是 hooks 刷新任务 + keys 的事;buildRequest 里 `util.key()` 返回本次请求注入键的 data(请求级 round-robin 选键),刷新任务写完下个请求自然生效。

## 六、本地测试与示例

```js
const { loadPackage, mockCtx } = require("<repo>/sdk/test.js");
const hooks = loadPackage(__dirname + "/..", { config: { apiVersion: "v1" }, secrets: { api_key: "sk-test" } });
const req = hooks.buildRequest(mockCtx());
assert.equal(req.url, "https://upstream.test/v1/...");
```

完整参考实现:`examples/gemini`(四钩子全实现 + factory 形态 + [编写说明](../examples/gemini/README.md))。
