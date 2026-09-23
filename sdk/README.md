# ai-api-proxy 插件 SDK

JS 部件开发工具集。部件契约与宿主面以 `ai-api-proxy.d.ts` 为准;本地测试宿主 `test.js` 与 Go host 绑定语义一致。

## 开发文档(docs/)

| 文档 | 内容 |
|---|---|
| [01 快速上手与包结构](docs/01-getting-started.md) | 怎么开始、包文件结构、manifest 全字段、打包安装、本地测试、全局注入面 |
| [02 hooks:时机与任务](docs/02-hooks-tasks.md) | init.js/定时任务(cron 与 next 自调度)执行时机、超时、ctx 面、可选项速查 |
| [03 参数配置与 keys](docs/03-settings-keys.md) | settings 声明构建器与覆盖链、凭据(keys)生命周期与五钩子、手动管理兜底 |
| [04 自定义代理协议](docs/04-protocol.md) | protocol 部件:两槽位、四钩子契约、对称性校验、configSchema/factory |

## 插件包文件结构

.aap 包 = zip。无单入口,文件按约定路径寻址、按需存在,manifest 不写 entry:

```
my-package/
├── manifest.json          # 必需;包声明
├── protocol.js            # protocol 部件(主包恰一)
├── filters/
│   └── <name>.js          # filter 部件(0..n,name 即路径)
├── init.js                # 可选;加载钩子
├── keys.js                # 可选;凭据五钩子
├── settings.js            # 可选;hooks 参数声明
├── tasks/
│   └── <name>.js          # 定时任务体(路径即任务名)
└── lib/
    └── *.js               # 共享常量,包内 require
```

require 语义、crontab 行共用 entry 的例外、校验红线见 [01 文档](docs/01-getting-started.md)。

## 部件形态速览

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
- hooks(可选部件,多文件布局):`init.js`(加载钩子)/ `tasks/<name>.js`(定时任务,cron 行与 next 自调度行互斥)/ `keys.js`(凭据五钩子)/ `settings.js`(参数声明)。全可选,详见 02/03 文档
- 全同步,禁 Promise/require npm/宿主进程;宿主执行网络(恰一次,失败即终局,无重试);hooks 出站走 `ctx.http.run`

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
