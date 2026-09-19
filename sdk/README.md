# ai-api-proxy 插件 SDK

JS 部件开发工具集。部件契约与宿主面以 `ai-api-proxy.d.ts` 为准;本地测试宿主 `test.js` 与 Go host 绑定语义一致。

## 部件形态

- protocol(主包恰一):`buildRequest` 必实现;`mapEvent`(声明 streaming)/`mapResponse`(声明 non_streaming)/`mapError` 可选
- filter(附加包 0..n):`mapRequest`/`mapChunk`/`mapResponse` 按需
- 导出:hooks 对象(无参)或 factory `(config) => hooks`
- 全同步,禁 Promise/require/npm;宿主执行网络(恰一次,失败即终局,无重试)

## 本地测试

```js
const { loadPart, mockCtx } = require("<repo>/sdk/test.js");
const hooks = loadPart(src, config, { api_key: "sk-test" });
const req = hooks.buildRequest(mockCtx());
```

运行:`node --test <包>/test/*.test.js`

## 类型守卫

部件加 `// @ts-check` + d.ts reference + JSDoc 标注(参考 plugins/ 两包),验证:

```
cd plugins && npx -y -p typescript tsc -p tsconfig.json
```

零错误 = 部件与 d.ts 契约一致;新增包应纳入 plugins/tsconfig.json 的 include(默认 **/*.js)。

参考包:`plugins/rewrite-model`(factory 形态 filter + configSchema + 单测)、`plugins/js-openai-full`(完整协议 + 黄金对照)
