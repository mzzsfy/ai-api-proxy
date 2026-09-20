# ai-api-proxy 插件 SDK

JS 部件开发工具集。部件契约与宿主面以 `ai-api-proxy.d.ts` 为准;本地测试宿主 `test.js` 与 Go host 绑定语义一致。

## 部件形态

- protocol(主包恰一):**声明式单协议**。manifest 用 `parts.protocol.protocol` 声明唯一协议槽
  (`"openai-completions"` | `"anthropic-messages"`),`forms` 声明形态子集(`streaming` | `non_streaming`)。
  入口协议与槽位不符时**主体直接 400 拒绝**,不做任何转换;`forms` 缺少入口形态时同样 400。
- 钩子与声明的对称性在包校验期强制:`streaming` 必须实现 `mapEvent`,`non_streaming` 必须实现 `mapResponse`;
  实现了某钩子却未声明对应形态同样拒绝。
- `buildRequest(ctx, entry)` 必实现;入参是**入口原文字节**(与声明协议同格式),返回 `stream` 承载双声明分发意图。
- `mapEvent(ctx, event)` 返回**声明协议事件对象数组的 JSON 字符串**;`null`/`"[]"`/`""` 跳帧。
  host 按声明协议收尾(openai 补 `data: [DONE]`,anthropic 无收尾帧)。
- factory 形态 `(config) => hooks` 可读 `config.protocol` 拿到本部件声明的协议槽;
  不要在 `configSchema` 中声明 `protocol` 键——key 剥离会把 host 注入值删掉。
- filter(附加包 0..n):`mapRequest`/`mapChunk`/`mapResponse` 按需
- 全同步,禁 Promise/require/npm;宿主执行网络(恰一次,失败即终局,无重试)

## 本地测试

```js
const { loadPackage, mockCtx } = require("<repo>/sdk/test.js");

const hooks = loadPackage(__dirname + "/..", { config: {}, secrets: { api_key: "sk-test" } });
const req = hooks.buildRequest(mockCtx());
```

`loadPackage(dir, {config, secrets, entry?})` 按目录内 manifest 自动定位部件入口,免去读文件样板;
需直接给源码时用 `loadPart(src, config, secrets)`。

运行:`node --test <包>/test/*.test.js`;SDK 自测:`node --test sdk/test.js`

## 唯一源

本目录是 SDK 的唯一源(类型定义 + 测试宿主 + 说明),随宿主演进。
**外部库(如插件库 plugins-repo)需使用时,从本仓库拉取本目录**,不在外部库内另行维护副本。

## 类型守卫

部件加 `// @ts-check` + d.ts reference + JSDoc 标注,验证:

```
cd plugins && npx -y -p typescript tsc -p tsconfig.json
```

零错误 = 部件与 d.ts 契约一致;新增包应纳入 tsconfig 的 include。

参考:`examples/gemini`(协议转换最小完整示例,四 hook 全实现 + [编写说明](examples/gemini/README.md))
