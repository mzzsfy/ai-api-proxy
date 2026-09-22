# 01 · 快速上手与包结构

开发一个 ai-api-proxy 插件包(.aap = zip)所需的全部结构说明。类型定义见 `../ai-api-proxy.d.ts`;执行语义的深度说明见 [02-hooks-tasks](02-hooks-tasks.md) 与 [03-settings-keys](03-settings-keys.md)。

## 一、选部件类型

| 类型 | 何时用 | 必需文件 | 网络 |
|---|---|---|---|
| protocol | 对接一类上游 API(openai-completions / anthropic-messages 两槽位之一) | protocol.js + manifest | 不需要(host 执行上游请求) |
| filter | 请求进上游前的改写/记录(0..n 个,串行) | filter 实现 js + manifest | 不需要 |
| hooks | 定时任务(刷 token/签到/报表)、加载钩子、凭据管理 | tasks/*.js 等族文件 + manifest | 有,经 ctx.http(限出站) |

三类可同包组合;仅含 hooks 的包合法(纯签到包不参与请求链)。

## 二、包文件结构

无单入口。每个文件独立、按需存在:

```
my-package/
├── manifest.json          # 必需;包声明
├── protocol.js            # protocol 部件(entry 在 manifest 声明)
├── filter.js              # filter 部件(entry 在 manifest 声明)
├── settings.js            # 可选;hooks 包共享参数声明(纯声明,无逻辑)
├── init.js                # 可选;onLoad 加载钩子
├── keys.js                # 可选;凭据钩子(归一化/详情/添加表单)
├── tasks/
│   └── <task-name>.js     # 任务体;manifest 未写 entry 时按此约定路径找
└── lib/
    └── consts.js          # 可选;共享常量,被 require
```

规则:
- `tasks/<name>.js` 是约定路径:manifest 任务行不写 entry 时,宿主按 `tasks/<该行name>.js` 找文件
- 多个 crontab 行可指向同一个 entry 文件,运行时用 `ctx.task` 区分是哪一行
- `require("./lib/consts.js")` 包内相对路径:扩展名可省、目录自动找 index.js、同一次任务执行内幂等单例、循环 require 拒装
- 共享代码与声明都可 require;不允许 npm 依赖

## 三、manifest.json

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

  // ── 包级 key 键名提示(可选;仅 GUI 预填名,不校验) ──
  "keySchema": { "token": "string" },

  "parts": {
    // protocol:主包至多一个
    "protocol": {
      "protocol": "openai-completions",          // 槽位:"openai-completions" | "anthropic-messages"
      "entry": "protocol.js",
      "form": ["streaming", "non_streaming"],    // 声明了哪个形态就必须实现对应钩子
      "features": ["tools"],
      "secretRefs": ["api_key"],                 // 允许 util.secret 读的目标凭据名
      "configSchema": { ... }                    // 部件实例参数 schema(用户建上游时填,经 config 注入)
    },
    // filters:0..n
    "filters": [
      { "name": "log-request", "entry": "filter.js", "configSchema": { ... } }
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

校验红线(拒装):name/version 缺失、`upstream:` 前缀、cron 行与 next 行同现、任务名重复、缺 entry 文件、cron 表达式非法、title >64 字符。

## 四、开发循环

1. 建目录写 manifest + 部件文件
2. 本地测试(不启宿主):

```js
// test/pkg.test.js
const { loadPackage, mockCtx } = require("<repo>/sdk/test.js");
const hooks = loadPackage(__dirname + "/..", { config: {}, secrets: { api_key: "sk-test" } });
```

   运行:`node --test test/*.test.js`。类型守卫:文件头加 `// @ts-check` + `/// <reference path="ai-api-proxy.d.ts" />`,`npx -y -p typescript tsc --noEmit` 零错误即契约一致。
3. 打包:把 manifest.json 与所有被引用文件按上述相对路径 zip(扩展名 .aap);或直接从管理台「下载模板包」起步改
4. 安装:管理台「包」页导入 .aap;后续升级 = 导入同 name 新版本(热加载,revision+1)
5. hooks 包在管理台有「配置」入口(参数/任务开关)与「keys」弹层(凭据),见 03 文档

## 五、全局注入面(所有 JS 文件可用)

| 注入 | 用途 |
|---|---|
| `util` | 工具集:b64/sha256/hmac/uuid/now/template/deepMerge/get/set/pick/omit/inspect/secret(ref)/key(name 只读)/evict |
| `log` | info/warn/error,进宿主日志 |
| `storage` | 包级 KV(get/set/delete,单值 ≤64KB,ns=包名) |
| `setting` | 仅 hooks 族文件求值期 + keyForm 运行期;参数声明构建器(见 03 文档) |
| `require` / `module` / `exports` | CommonJS,包内相对解析 |

禁:Promise/async(钩子全同步)、npm、宿主进程访问。

## 六、文档地图

- [02-hooks-tasks](02-hooks-tasks.md):init/任务/next 什么时候跑、超时、可选项
- [03-settings-keys](03-settings-keys.md):参数配置声明与覆盖、凭据(keys)管理全流程
- 类型契约:`../ai-api-proxy.d.ts`;完整示例:`../examples/gemini`
