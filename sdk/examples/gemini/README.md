# gemini — 协议转换包(示例)

把 openai chat-completions 形态的 pivot 翻译成 Google Gemini `GenerateContent` 上游调用,再把响应翻译回来。
**这是写协议转换包的最小完整示例**:四个 hook 全部实现,无私有指纹、无业务耦合,可整体照搬结构。

上游协议:`POST {baseUrl}/{apiVersion}/models/{model}:generateContent`(非流式)或 `:streamGenerateContent?alt=sse`(流式)。

## 一、先理解 pivot 契约

宿主在入口编解码与上游部件之间夹了一层**中间形态 pivot**。协议包只需处理 pivot,不必关心客户端用的是 openai 还是 anthropic 入口。

pivot 请求 = openai chat-completions 请求的超集(多了 `x_` 扩展字段)。
pivot 响应 = openai chat-completions 响应形态。

方向的对应关系:

```
客户端请求 → [入口编解码] → pivot → [本包 buildRequest] → Gemini 请求
Gemini 响应 → [本包 map*] → pivot → [入口编解码] → 客户端响应
```

**关键**:`mapEvent` / `mapResponse` 的产物必须是 **pivot 形态**,不是"openai 入口的形态"。
本包产物刻意构造为 openai chunk/message 语义,因为宿主 openai 入口对 pivot chunk 是原样透传的。

## 二、四个 hook 各自职责

### `buildRequest(ctx, pivot) → {url, method, headers, body, stream}`

把 pivot 请求翻译为上游 HTTP 请求。

本包处理的翻译:

| pivot 字段 | Gemini 字段 | 说明 |
|---|---|---|
| `messages[].content` | `contents[].parts[].text` | 字符串与 text 块数组都归一到单文本 |
| `role: "system"/"developer"` | `systemInstruction` | 顶出消息列表(上游无 system role) |
| `role: "assistant"` | `role: "model"` | 角色改名 |
| `max_completion_tokens` ?? `max_tokens` | `generationConfig.maxOutputTokens` | 优先取新字段名 |
| `temperature` / `top_p` | `generationConfig.temperature` / `topP` | 同名改形 |
| `stop` | `generationConfig.stopSequences` | 字符串归一为数组 |

三个必须注意的点:

1. **连续同角色消息必须合并**。Gemini 强制 `user`/`model` 交替,连续同角色会 400。
   本包做法是把内容追加到上一条的 `parts`。
2. **空文本消息跳过**。上游拒绝空 part。
3. **错误要抛 `Error`,不要返回畸形请求**。宿主会把部件抛错转成 502 并注明原因,
   比让上游回一个语义模糊的 400 更好定位。本包在 `baseUrl` 为空、pivot 非法、
   无可翻译内容三种情况下抛错。

`stream` 字段取自 `ctx.vars.entryStream`(客户端是否要流式),决定 URL 用哪个 action。

鉴权用 `util.secret("api_key")` 取密钥,`secretRefs` 在 manifest 里声明。**密钥不要拼进 URL**。

### `mapEvent(ctx, event) → string | null`

流式:把上游一个 SSE 帧翻译为**零个或一个** pivot chunk。返回 `null` 表示跳过该帧。

宿主保证上游流式响应按 SSE 帧喂进来,`event` 是**原始整帧文本**(含 `data: ` 前缀),
所以本包做 `JSON.parse(JSON.parse(event).data)` —— 外层解 SSE 包装,内层解 JSON 载荷。

**跳帧规则**:无文本、无终止原因、无用量时返回 `null`。否则一个"空 delta 空 finish_reason"的帧
会污染下游的流式解析。

**流内错误帧必须处理**:HTTP 200 启动后上游仍可能中途失败,SSE 里出现 `{error: {...}}`。
此时返回 `{x_error: {...}}` 终止本次流。若直接跳帧,错误会伪装成正常收尾,客户端无从察觉。

```js
if (g.error) {
  return JSON.stringify({ x_error: { type: ..., message: ... } });
}
```

### `mapResponse(ctx, body) → string`

非流式:把上游整体响应翻译为 pivot 响应(openai `chat.completion` 信封)。

与 `mapEvent` 共享同一批字段翻译逻辑,差别只是整体 vs 增量。

### `mapError(ctx, status, body) → string`

上游返回非 2xx 时调用,把上游错误体翻译为 pivot 错误形态。

**核心原则:原样反映上游真实信息,不做语义翻译。**

本包的映射:

| pivot | 来源 |
|---|---|
| `error.type` | 上游 `error.status` 字符串**原样**(如 `RESOURCE_EXHAUSTED`);上游未给时回落 HTTP 状态字符串 |
| `error.message` | 上游 `error.message` |
| `error.status` | 上游 `error.code` 数字;未给时回落 HTTP 状态 |
| `error.details` | 上游 `error.details` **原样搬运整个数组**,不挑第一条、不改键名 |

**为什么不做枚举翻译**:初版曾把上游枚举翻成 openai 枚举(`RESOURCE_EXHAUSTED` → `rate_limit_error`),
这是错的。`RESOURCE_EXHAUSTED` 明确区分"配额耗尽"与"请求太快",翻成 `rate_limit_error` 反而**丢信息**——
客户端无法判断该退避 30 秒还是等一天。宿主的 openai 错误输出同样是原样透传 `type`,
包内翻译违背既有约定。

**空体的特殊处理**:配额/限速拒绝常在 SSE 开始前掐断连接,此时 `body` 为空。
空体无法承载任何上游信息,此时至少给出 HTTP 状态与 `type`,避免下游拿到空错误体无从判断。
这是本包唯一"补信息"的路径,且不编造 `code`。

## 三、两个容易踩的真实坑(实测)

### 1. usage 字段缺键会静默丢键

上游在候选被截断/为空时**省略 `candidatesTokenCount`**。若直接取该字段,得到 `undefined`,
经 `JSON.stringify` 后**键会整个消失**——下游读到 `null` 而非 0。

实测帧:
```json
"usageMetadata":{"promptTokenCount":3,"totalTokenCount":16,"thoughtsTokenCount":13}
```
`total(16) = prompt(3) + thoughts(13)`,没有 `candidatesTokenCount`。

本包取定 `completion_tokens = candidatesTokenCount + thoughtsTokenCount`,缺键按 0 计。
理由:上游 `total` 已含思考量,若不计入 completion,则 `total != prompt + completion`,
破坏 openai 客户端依赖的配额不变量。

### 2. 思考模型的 maxOutputTokens 包含思考消耗

3.x 系列是思考模型,`thoughtsTokenCount` **计入** `maxOutputTokens`。
下游若给过小的 `max_tokens`(如 32),思考会吃光预算,正文零输出、
`finishReason` 为 `MAX_TOKENS`。这是上游行为,包不做代偿。

## 四、manifest 要点

```json
{
  "parts": {
    "protocol": {
      "entry": "protocol.js",
      "form": ["streaming", "non_streaming"],
      "features": [],
      "secretRefs": ["api_key"]
    }
  }
}
```

- `form` 声明本包支持的请求形态,宿主据此协商路由
- `features` 声明扩展能力(如 `tools`/`vision`)。**本包只支持纯文本,故为空数组**——
  客户端发工具调用请求时宿主会直接回 400,而不是让部件产出畸形上游请求
- `secretRefs` 声明需要注入的密钥名,部件内用 `util.secret(name)` 取

## 五、测试写法

```js
const test = require("node:test");
const assert = require("node:assert");
const { loadPackage, mockCtx } = require("../../../sdk/test.js");

const hooks = loadPackage(__dirname + "/..");
const CTX = mockCtx({ target: { baseUrl: "https://example.com" }, vars: { model: "gemini-3.6-flash", entryStream: false } });

test("buildRequest: system 提为 systemInstruction,连续同角色合并", () => {
  const req = hooks.buildRequest(CTX, JSON.stringify({
    messages: [{ role: "system", content: "be brief" }, { role: "user", content: "hi" }]
  }));
  const body = JSON.parse(req.body);
  assert.equal(body.systemInstruction.parts[0].text, "be brief");
  assert.equal(body.contents.length, 1);
});
```

运行:`node --test plugins/gemini/test/protocol.test.js`

测试应覆盖三类:
- **形态翻译**(字段对不对)
- **真实上游帧**(从线上抓的帧原样作为用例,尤其是边界帧:空候选、截断、错误帧)
- **异常路径**(pivot 非法、空内容、非 JSON 错误体)

## 六、可照搬与需替换的部分

可直接照搬的结构:
- 四 hook 的划分与调用约定
- `null` 跳帧规则、`x_error` 流内错误处理
- 错误原样透传原则
- 测试文件骨架

必须替换的部分:
- `buildRequest` 里的字段映射表(每个上游协议不同)
- URL 与鉴权形态
- `mapEvent` / `mapResponse` 的响应解包路径

参考其他实现:同目录的 `opencode`(含客户端伪装头)、`zcode`(anthropic 端点 + 身份指纹 + `cache_control` 注入)。

## 七、真连验证

本包在真实 Gemini 上游验证过(含流式/非流式 × openai/anthropic 入口)。
主库侧 E2E:`internal/server/e2e_real_gemini_test.go`,env 门控,无 `GEMINI_TEST_KEY` 时整组跳过。