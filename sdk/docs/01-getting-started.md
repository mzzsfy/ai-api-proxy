# 01 · 快速上手与包结构

开发一个 ai-api-proxy 插件包(.aap = zip)所需的全部结构说明。类型定义见 `../ai-api-proxy.d.ts`;执行语义的深度说明见 [02-hooks-tasks](02-hooks-tasks.md) 与 [03-settings-keys](03-settings-keys.md)。

## 一、选部件类型

| 类型 | 何时用 | 必需文件 | 网络 |
|---|---|---|---|
| protocol | 对接一类上游 API,映射逻辑宿主可编舞(buildRequest/单次出站) | protocol.js + manifest | 不需要(host 执行上游请求) |
| handler | 出站时机/次数/续接/流处理需要代码控制(多次出站、token 轮换、伪流式) | handler.js + manifest(`parts.handler` 与 protocol 互斥) | 有,ctx.http.request/open(经代理包装) |
| filter | 请求进上游前的改写/记录(0..n 个,串行) | filter 实现 js + manifest | 不需要 |
| hooks | 定时任务(刷 token/签到/报表)、加载钩子 | tasks/*.js 等族文件 + manifest | 有,经 ctx.http(限出站) |

可同包组合(handler 与 protocol 互斥除外);仅含 hooks 的包合法(纯签到包不参与请求链)。

## 二、包文件结构

无单入口。每个文件独立、按需存在:

```
my-package/
├── manifest.json          # 必需;包声明
├── protocol.js            # protocol 部件(路径约定:protocol.js;与 handler 互斥)
├── handler.js             # handler 部件(路径约定:handler.js;与 protocol 互斥)
├── filters/
│   └── <name>.js          # filter 部件(路径约定:filters/<name>.js)
├── login/                 # 可选;凭据采集目录(与宿主默认表单两态互斥)
│   ├── index.js           #   登录逻辑(module.exports = (ctx) => data)
│   └── ui/index.html      #   采集页面(与 index.js 绑定成对,缺一拒装)
├── inspect.js             # 可选;账号展示变换(纯函数)
├── settings.js            # 可选;hooks 包共享参数声明(纯声明,无逻辑)
├── init.js                # 可选;onLoad 加载钩子
├── tasks/
│   └── <task-name>.js     # 任务体(路径约定:tasks/<name>.js)
└── lib/
    └── consts.js          # 可选;共享常量,被 require
```

规则:
- 全部部件按路径约定寻址,manifest 不写 entry;唯一例外:多个 crontab 行共用一个实现时,任务行可显式写 `entry` 指向同一文件,运行时用 `ctx.task` 区分是哪一行
- `require("./lib/consts.js")` 包内相对路径:扩展名可省、目录自动找 index.js、同一次任务执行内幂等单例、循环 require 拒装
- 共享代码与声明都可 require;不允许 npm 依赖

## 三、manifest.json

**原则:约定路径能表达的绝不写进 manifest。** 部件文件位置全部由约定决定,manifest 只描述推导不出来的事实。

```jsonc
{
  "manifestVersion": 1,
  "name": "my-package",              // 全局唯一;禁止 "upstream:" 前缀(保留)
  "version": "0.1.0",

  // ── 展示元数据(全部可选) ──
  "title": "我的包",                  // 管理台显示名;缺省回退 name
  "description": "一句话说明",        // ≤256 字符
  "author": "you",
  "homepage": "https://...",
  "license": "MIT",

  "parts": {
    // protocol:主包至多一个;文件固定 protocol.js(约定);与 handler 互斥
    "protocol": {
      "protocol": "openai-completions",          // 槽位:"openai-completions" | "anthropic-messages"
      "features": ["tools"]                      // 上游能力标记
    },
    // handler:与 protocol 互斥;值 = 入口协议名(同 protocol 槽位取值);文件固定 handler.js(约定)
    "handler": "openai-completions",
    // filters:0..n;文件约定 filters/<name>.js(manifest 不写路径)
    "filters": [
      { "name": "log-request" }
    ],
    // hooks:可选;定时任务声明(crontab 心智模型:一行 = 一个时刻 + 一个命令)
    "hooks": {
      "tasks": [
        { "name": "refreshToken", "cron": "0 */12 * * *", "timeoutMs": 30000 },
        { "name": "sync", "next": true, "timeoutMs": 30000 }   // 自调度;与 cron 互斥
      ]
    }
  }
}
```

### 路径约定(manifest 不写 entry)

| 部件 | 文件路径 | 说明 |
|---|---|---|
| protocol | `protocol.js` | 固定 |
| handler | `handler.js` | 固定 |
| filter | `filters/<name>.js` | name 即路径 |
| hooks 任务 | `tasks/<name>.js` | 仅当多行 crontab 共用一个实现时才显式写 `entry` 覆盖 |

### 实现即声明

协议部件支持哪些流形态(流式/非流式)由导出推导:`mapEvent` = 支持流式,`mapResponse` = 支持非流式,都没有拒装。manifest 不写 form。

校验红线(拒装):name/version 缺失、`upstream:` 前缀、cron 行与 next 行同现、任务名重复、约定路径文件缺失、cron 表达式非法、title >64 字符、protocol 与 handler 同声明、handler.js 导出非函数、handler.js/login/index.js 导出 settings、login/ 目录 index.js 与 ui/index.html 缺一、包内出现 keys.js(已废弃)。

## 四、开发循环

1. 建目录写 manifest + 部件文件
2. 本地测试(不启宿主):

```js
// test/pkg.test.js
const { loadPackage, mockCtx } = require("<repo>/sdk/test.js");
const hooks = loadPackage(__dirname + "/..", { config: { base_url: "https://upstream.test" }, keys: { api_key: "sk-test" } });
```

   运行:`node --test test/*.test.js`。类型守卫:部件文件头加 `// @ts-check` + `/// <reference path="<repo>/sdk/ai-api-proxy.d.ts" />`(按文件相对 sdk 的实际层级写),`npx -y -p typescript tsc --noEmit` 零错误即契约一致。
3. 打包:把 manifest.json 与所有被引用文件按上述相对路径 zip(扩展名 .aap);或直接从管理台「下载模板包」起步改
4. 安装:管理台「包」页导入 .aap;后续升级 = 导入同 name 新版本(热加载,revision+1)
5. hooks 包在管理台有「配置」入口(参数/任务开关)与「keys」弹层(凭据),见 03 文档

## 五、全局注入面(所有 JS 文件可用)

| 注入 | 用途 |
|---|---|
| `util` | 工具集:b64/b64url/sha256/hmac/uuid/now/isoNow/template/json/deepMerge/deepClone/get/set/pick/omit/inspect/evict(传输租约/出口逐出)/key()(当前注入键 data 只读) |
| `log` | info/warn/error,进宿主日志 |
| `storage` | 包级 KV(get/set/delete,单值 ≤64KB,ns=包名);**写入语义按执行者分叉**:handler 执行 = 事务缓冲(return 归并,失败丢弃;例外:unlock 同步写权威层,见 05);任务/init/login 等其余执行 = 直写持久层(不回滚) |
| `setting` | 任意族文件求值期注入(参数声明构建器,见 03 文档);handler.js/login/index.js 除外(导出 settings 拒装) |
| `require` / `module` / `exports` | CommonJS,包内相对解析 |

禁:Promise/async(钩子全同步)、npm、宿主进程访问。

## 六、文档地图

- [02-hooks-tasks](02-hooks-tasks.md):init/任务/next 什么时候跑、超时、可选项
- [03-settings-keys](03-settings-keys.md):参数配置声明与覆盖、凭据子系统(login/ 目录、inspect、键池)
- [04-protocol](04-protocol.md):protocol 部件四钩子契约
- [05-handler](05-handler.md):handler 部件全生命周期直线代码(ctx/reply/http/槽锁/超时)
- 类型契约:`../ai-api-proxy.d.ts`;完整示例:`../examples/gemini`
