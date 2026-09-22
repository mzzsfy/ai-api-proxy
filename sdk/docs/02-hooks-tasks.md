# 02 · hooks 部件:时机、任务与可选项

hooks 部件 = init.js(加载钩子)+ tasks/*.js(定时任务)+ keys.js(凭据钩子,见 [03](03-settings-keys.md))+ settings.js(参数声明,见 [03](03-settings-keys.md))。全部文件可选——只写你需要的。

## 一、执行时机总表

| 文件/导出 | 触发时机 | 超时 | 失败后果 |
|---|---|---|---|
| init.js `onLoad` | 导入/启用/热升级后,新 revision 生效时调用一次 | 5s | 仅 log error,不阻断加载 |
| 任务行 `handler` | 该行 cron 时刻(或 next 链调度) | timeoutMs(缺省 30s,上限 5min) | 该次任务失败,记日志;cron 行下轮照常 |
| 任务行 `next` | handler 跑完后宿主立即调用,决定下次时刻 | 5s | next 抛错 = 链终止 |
| keys.js 五钩子 | 管理台 keys 操作时(见 03) | 归一化/读取 5s;表单流程 10s | 操作失败回报 GUI |

热升级语义:新 revision 先跑自己的 onLoad,旧 revision 等在途引用归零后销毁;onLoad 里 `ctx.keys.previous(name)` 可读旧包凭据。

## 二、init.js(可选)

```js
// @ts-check
/// <reference path="../ai-api-proxy.d.ts" />
module.exports = {
  onLoad: function (ctx) {
    // 环境判断、初始化 storage 标记……均为尽力而为
    // 注意:onLoad 的 ctx.keys 无 merge(只读);需要写 keys 请放任务里
  },
};
```

- 失败不阻断加载,所以不要把"必须成功"的逻辑放这里
- 修改参数(settings)不会重跑 onLoad;下一轮任务读到新值即可

## 三、定时任务:cron 行(推荐起点)

**tasks[] 就是你的 crontab**:manifest 每行 = 一个时刻 + 一个命令文件。

```jsonc
"hooks": { "tasks": [
  { "name": "report-am",  "cron": "0 8 * * *" },                       // 缺省 entry → tasks/report-am.js
  { "name": "report-pm",  "cron": "0 20 * * *", "entry": "tasks/report.js" },
  { "name": "refreshToken", "cron": "0 */12 * * *", "timeoutMs": 30000 }
]}
```

```js
// tasks/report.js —— 多行共用本文件时用 ctx.task 区分
module.exports = {
  handler: function (ctx) {
    log.info("ran by", ctx.task);          // "report-am" | "report-pm"
    var cfg = ctx.settings;                 // 参数快照(声明 ⊕ 用户覆盖)
    ctx.http.run({ url: "https://example/api", method: "POST", body: "{}" });
  },
};
```

要点:
- 一行缺省 entry = `tasks/<name>.js`;想让两行共用实现,显式写同一个 entry
- cron 为标准 5 字段表达式(分 时 日 月 周)
- 时刻与启停归用户:管理台可覆盖 cron、可停某一行;你的声明只是缺省值。代码里不要自己解析 cron
- `ctx.cron.runAt` = 本次触发时刻字符串
- timeoutMs ≤ 0 按缺省,超 5min 按上限

## 四、自调度任务:next 行

结束时刻不由表达式决定、而由"上一轮结果"决定时用(如"每 30 分钟轮询直到成功"):

```jsonc
{ "name": "sync", "next": true }
```

```js
// tasks/sync.js —— next 行必须同时导出 handler 与 next
var state = 0;
module.exports = {
  handler: function (ctx) { state = ctx.http.run({ url: "https://example/poll" }).status; },
  next: function (ctx) {
    if (state === 200) return null;                 // 收摊:链终止
    return util.now() + 30 * 60 * 1000;             // 30 分钟后再跑
  },
};
```

- `next(ctx)` 返回 **unix 毫秒数字**(下次触发)或 `null`(停链);返回 ≤ now = 立即再触发
- 安装/启用/升级后**立即首跑**,此后每轮 handler → next 循环
- cron 行与 next 行互斥:一行只能是一种;宿主重启后 next 链自动重建并首跑
- 禁用任务行 = 停链;重新启用 = 首跑重建

## 五、ctx 面(任务/init/keys 钩子共用)

| 字段 | 说明 | 可用性 |
|---|---|---|
| `ctx.http.run(req)` | 出站请求;`{url, method?, headers?, body?, timeoutMs?}` → `{status, headers, body}`;body >1MB 截断;timeoutMs 缺省 min(30s, 外层剩余) | 全部 hooks 钩子 |
| `ctx.keys` | `get(name)` / `previous(name)` / `list()` 只读;`merge(values)` **仅任务执行与 keySubmit 挂载**(其余钩子上无此方法——阉割即权限);键级合并:给定键覆盖,未提及保留 | 全部(merge 见窗口) |
| `storage` | 包级 KV `get/set/delete`;**事务语义**:执行期写进私有缓冲,本次执行成功才归并持久化,抛错/超时全部丢弃 | 全部 |
| `ctx.settings` | 参数快照(只读),见 03 | 全部 |
| `ctx.task` | 本次任务行名 | 仅任务 |
| `ctx.cron.runAt` | 触发时刻 | 仅任务 |

约束:**handler/next/onLoad/keys 钩子全部同步**。返回 Promise = 同步性违规报错;耗时长出站操作靠 `ctx.http.run` 的 timeoutMs 控制,不要 setTimeout。

## 六、可选项速查

| 想做的事 | 需要写什么 | 不写会怎样 |
|---|---|---|
| 只要定时任务 | tasks/*.js + manifest tasks 行 | — |
| 加载时初始化 | init.js onLoad | 不跑任何加载逻辑(无害) |
| 凭据校验/脱敏显示/采集表单 | keys.js 对应钩子 | GUI 用通用 JSON 编辑(见 03) |
| 包参数 | settings 声明片段(03) | GUI 无参数区,任务读 ctx.settings 全空 |
| 多行区分 | handler 里读 ctx.task | 无法区分是哪一行触发 |
| 共享常量 | lib/*.js + require | 每文件各写一份 |
