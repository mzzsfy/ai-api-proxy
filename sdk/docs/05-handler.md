# 05 · handler 部件:请求全生命周期直线代码

handler 部件与 protocol 部件**互斥**(同包同声明两者拒装):protocol 是"声明式映射"(buildRequest/mapEvent 宿主编舞),handler 是"命令式直线函数"——**一个 JS 函数持有整个请求生命周期**,出站时机、次数、流处理、续接、凭据管理全部内联,宿主只给阻塞原语和触发点。

何时选 handler:需要多次出站(登录引导→生成→续接)、token 运行期轮换、非 SSE 上游拆流、或任何"宿主编舞表达不了"的出站控制逻辑。代价是每请求更重(protocol 是池化热路径纯映射)——不需要出站控制的包仍用 protocol。

## 一、manifest 与文件

```jsonc
{
  "manifestVersion": 1,
  "name": "acme",
  "version": "1.0.0",
  "parts": {
    "handler": "openai-completions"    // 值 = 入口协议名(同 parts.protocol.protocol 的槽位取值)
  }
}
```

| 文件 | 必需 | 说明 |
|---|---|---|
| `handler.js` | 是 | `module.exports = function (ctx) {...}` 直线主函数;可选另导出 `prepare` |
| `login/` | 否 | 凭据采集目录(与宿主默认表单两态互斥),见 [03](03-settings-keys.md) |
| `inspect.js` | 否 | 账号展示变换(纯函数),见 [03](03-settings-keys.md) |
| `filters/`、`tasks/`、`init.js` | 否 | 既有契约不变;filters 照常串在 handler 之前 |
| `settings.js` | 否 | hooks 参数声明;handler 主函数不消费 settings 声明(仅 tasks/init 经 ctx.settings 读;handler 的 `ctx.settings` 是超时覆写、`ctx.params` 是上游参数) |

- `parts.handler` 的值决定入口请求解析、协议协商与 features 语义(与 protocol 声明同源)
- 拒装红线:同时声明 protocol 与 handler;handler.js 导出非函数;handler.js/login/index.js 导出 settings(参数声明不参与);包内出现 keys.js(已废弃)
- handler.js 不走 buildRequest/mapEvent 管线;模型发现(discovery)按现有声明照常

## 二、请求生命周期

```
pickKey 注入(至少一键,空键按既有语义失败,不进 handler)
  → prepare(ctx)        可选;刷 token/引导的标准位置;锁可延续到 handler
  → 返回值 = ctx.data 初始值(handler 全程随意读写,执行局部,终局丢弃)
  → handler(ctx)        直线代码:出站 → 转换 → 应答
  → 终局:等在途 unlock 写 → storage 归并 → 释放全部锁
```

### prepare(可选导出)

```js
module.exports.prepare = function (ctx) { ...; return { token: t }; };
module.exports = function (ctx) { const { token } = ctx.data; ... };
```

- 可用原语:http / storage / getWithLock / unlock / settings **全量**,`ctx.request`/`ctx.params`/`ctx.key` 只读同 handler——刷 token 标准位置(槽锁+引导+unlock 放行);锁为执行级登记,**自然延续到 handler**,终局统一兜底释放
- 禁:reply 原语、读写 ctx.data(访问即抛错);抛错 = 502,不进 handler
- 与 handler 同一次执行、同一预算;ctx.data 是它的返回值

### 同步直线模型

全同步,禁 Promise/async(导出或返回 Promise 形态 = 失败 502)。阻塞靠宿主原语:开流等响应头、迭代等下一帧、锁排队——从 JS 视角是直线代码。耗时段落用四级出站超时控制(见下),不要 setTimeout。

模块顶层契约:**只准函数/常量,禁止可变模块状态**——运行时实例池化复用,顶层在池构建期求值一次、跨请求共享;可变顶层 = 实例内串请求,后果自担(宿主不校验)。

## 三、ctx 成员

| 成员 | 说明 |
|---|---|
| `ctx.request` | `{model, stream, body, headers}`(入口协议 JSON 文本,经 filters;headers 只读) |
| `ctx.params` | 合并参数(上游预设参数 ⊕ 包级默认),与 protocol 路径同源 |
| `ctx.key` | 当前注入键 `{id, data}` 只读;注入一次整次执行恒为该键;**handler 零键池写**,动态凭据走 storage |
| `ctx.data` | 执行级数据袋,随意读写;初始值 = prepare 返回值(未导出 = undefined);prepare 内访问即抛错 |
| `ctx.settings` | 出站超时覆写(见四级超时);注意与 settings 声明的参数不同——参数在 `ctx.params` |
| `ctx.http` | 出站原语(见下) |
| `ctx.reply` | 应答原语(见下) |
| `ctx.storage` | 包级 KV + 槽锁(见四) |

另:全局注入 `util` / `log` / `storage`(同 hooks)照常可用——handler 内全局 `storage` 与 `ctx.storage` 是**同一面**(锁定槽写封闭、权威槽归并丢弃对两通道同样生效,无绕锁写路径)。

## 四、出站:http

```js
// 缓冲出站
const r = ctx.http.request({
  url: ctx.params.base_url + "/v1/generate", method: "POST",
  headers: { Authorization: "Bearer " + token }, body: JSON.stringify(req),
  transport: "acme-pool",      // 可选;缺省 = 组参数解析结果的传输槽
  sessionKey: "acme:" + ctx.key.id,   // 可选;透传代理会话粘滞
  timeoutMs: 5000,             // 可选;本次调用覆写 totalTimeoutMs
});
// r = {status, headers, body}

// 流式出站
const src = ctx.http.open({ ...同上, stream: true });
// src = {status, headers}
for (const frame of src.frames()) { ... }   // {"event","data"} 逐帧,EOF 自然结束
const full = src.body();                     // 或整块取(与 frames 混用抛错)
```

- **全部出站经代理包装**(transport 命名实例 + IP 租约;名不存在抛错)
- `open` 必须 `stream: true`(缺省/false 抛错,缓冲用 request);status ≥ 300 时 `body()` 取回全量并关闭、`frames()` 抛错;**Content-Type 非 event-stream(如 200+JSON)时 frames() 抛错,改走 body()**(伪流式出口)
- `frames()` 抛错(超时/断流)后迭代器终态(再 next 抛"已终止"),source 连接立即关闭;同 source 二次 frames()、frames/body 混用抛错
- `body()` 无大小上限(缓冲内存自担)

### 四级出站超时与 ctx.settings.overwrite

| 键 | 语义 | 缺省 |
|---|---|---|
| `connectTimeoutMs` | 建连超时 | 30*1000ms |
| `responseTimeoutMs` | 响应超时(发出到对方返回任何数据/首字节;首字节后让位帧间) | 60*1000ms |
| `frameIdleTimeoutMs` | 帧间超时(流式相邻两帧最大间隔) | 180*1000ms |
| `totalTimeoutMs` | 单次出站整体(发出到流读完/缓冲完成) | 10*60*1000ms |

```js
ctx.settings.overwrite({ frameIdleTimeoutMs: 600 * 1000 });   // 合并语义,执行局部
```

- 非法键抛错(键面显式可枚举)
- **生效时机**:connect/response/total 在原语调用时定死(中途 overwrite 不延长已计时调用);**frameIdle 逐帧隙重武装并读当前设置**——for-of 循环体内 overwrite 对下一帧隙生效,这是调大推理型上游思考期预算的唯一窗口;frameIdle 与 total 同时到期优先报 total
- 四级超时是**可 catch 的普通错误**:到期仅使该次调用/source 抛超时错并关闭,不污染执行,catch 后可重开续接(与执行级预算取消不同,见六)

## 五、应答:reply

```js
ctx.reply.json(200, bodyText);   // 非流式;仅入口 stream=false 可用,一次
ctx.reply.sse();                 // 流式开场;仅入口 stream=true 可用,一次
ctx.reply.event(obj);            // 产出一个入口协议事件(宿主按入口协议编码)
ctx.reply.end();                 // 流式正常收尾
ctx.reply.error(obj);            // 流式错误终局;error 已写 = 视同已收尾,return 即正常终局
```

- **应答形态强制校验**:reply.json 仅入口 stream=false、reply.sse 仅 stream=true,违者抛错(502)——应答形态是入口协议契约
- 越序全抛错:json 二次 / sse 二次 / sse 后调 json / end 后再写 / error 后再写
- **零应答 = 502**:handler return 且全程未调任何 reply 原语
- 非流式上游 + 入口 stream=true 的正确姿势 = **伪流式**:request 聚合 → 拆块 → `reply.sse()` + 逐块 `reply.event` + `reply.end()`
- reply.json 完整写出后 handler 抛错 = 下游已收完整应答,宿主正常关闭连接并 log(非流式无错误追加速径)
- 未消费的 source 两种情形:open 后未 body()/未迭代即 return → 宿主关闭 + log warn(包名/URL);迭代中途 return → 异常终局收尾 + source 兜底关闭

## 六、storage:缓冲、槽与锁

两级模型:②执行事务缓冲(set/delete;正常 return 归并持久化,抛错/超时整体丢弃)+ ③SQLite 唯一权威。槽值 = 字符串(存对象自己 JSON 序列化)。

```js
ctx.storage.get(s);                 // 权威槽直读③;普通槽 ②→③
ctx.storage.set(s, v); ctx.storage.delete(s);   // 进②缓冲,return 归并
ctx.storage.getWithLock(s);         // 读并持锁(读③跳②;排队 FIFO;超时抛错;同槽重入抛错)
ctx.storage.unlock(s, v?);          // 白放 / 同步写③+打权威标+释放(锁定槽唯一写通道)
```

**槽写法判据(二选一,禁止混用)**:token/凭据类槽**全程锁通道**(getWithLock/unlock);普通数据槽全程 set/delete。槽一旦被 unlock 落值或外部写即**永久权威**,此后对它的 set 缓冲归并时静默丢弃(log warn)。"平时 set、临期走锁"必然踩丢弃——写包前先给每个槽定类;数据形态迁移 = 换槽名(权威标记永不清除)。

- 锁定期间任何人(含持锁者自己)set/delete 该槽 = 抛错;锁保持到 unlock 或终局兜底释放
- `unlock(s, v)` 同步写③(排队者即刻见新值);写超时 5*1000ms 独立于执行取消,失败 = 抛错 + 锁必释放
- 多槽不推荐:单槽满足全部既有场景;交叉死锁由等待超时兜底

### token 轮换标准姿势(羊群单飞)

```js
const slot = "live_token:" + ctx.key.id;
let held = true;
let token = ctx.storage.getWithLock(slot) || bootstrap(ctx);   // 读即持锁:并发请求排队,单写者
const src = ctx.http.open({ ...up(ctx.request, token), stream: true });
for (const frame of src.frames()) {
  const next = nextToken(frame);                     // 提取一次:轮换判定与落值共用
  if (next && held) { ctx.storage.unlock(slot, next); held = false; }
  ...
}
if (held) ctx.storage.unlock(slot, token);   // 兜底:全程无轮换帧也要落引导成果
```

锁在 prepare 持有则自然延续:prepare 里 getWithLock → handler 消费 → 帧内 unlock。引导复用登录逻辑:`require("./login/index.js")` 后传裁剪对象(见 [03](03-settings-keys.md))。

## 七、执行语义(宿主侧,理解行为即可)

- **池化**:运行时实例按 (包,revision) 池化复用,ctx 是调用参数不进全局——跨请求泄漏构造性不可能;实例借出 N 次丢弃重建(宿主配置,缺省 100)
- **超时双通道**:单次出站四级超时 = 可 catch 可续接;**执行级取消**(整体预算 60min 含排队/语句 10s/下游断开)= 终局失败,不可恢复
- 失败 = 502/错误终帧(零重试零补偿,重试语义归下游);错误信息必含原语名+槽名/形态等定位字段
- handler 包请求**不进既有 history/metrics**(出站多轮/续接与既有"一次上游请求"计量模型不匹配),单独新建统计
- 锁/归并终局序:等在途 unlock 写 → storage 归并 → 释放全部锁

## 八、完整示例(转换 + 续接)

```js
const login = /** @type {import("../ai-api-proxy.d.ts").LoginModule} */ (require("./login/index.js"));

module.exports.prepare = function (ctx) {
  const slot = "live_token:" + ctx.key.id;
  const keyData = /** @type {{account: string}} */ (ctx.key.data);
  let t = live(ctx.storage.getWithLock(slot));       // 读即持锁
  if (!t || t.exp - util.now() < 60 * 1000) {
    t = login({ params: { account: keyData.account },
      http: ctx.http, storage: trimLock(ctx.storage), util });
  }
  ctx.storage.unlock(slot, t.token);                 // 本例临界区短,prepare 内落值放行;需跨流轮换时锁可延续(见六)
  return { token: t.token };
};

module.exports = function (ctx) {
  const data = /** @type {{token: string}} */ (ctx.data);
  const req = JSON.parse(ctx.request.body);
  ctx.reply.sse();                                   // 前置:入口 stream=true(本示例形态;stream=false 用 reply.json)
  let sent = "";
  let src = ctx.http.open({ url: ctx.params.base_url + "/v1/generate", method: "POST",
    headers: { Authorization: "Bearer " + data.token }, body: upstreamBody(req), stream: true,
    sessionKey: "acme:" + ctx.key.id });
  for (let round = 0; round <= 2; round++) {
    try {
      for (const frame of src.frames())
        for (const c of convert(frame)) { ctx.reply.event(c); sent += textOf(c); }
      ctx.reply.end();
      return;                                        // token 槽全程锁通道:刷新用 unlock,不用 set
    } catch (e) {
      if (!isTimeout(e) || round === 2 || !sent) throw e;  // 只续接超时类且已有产出;其余终局
      req.messages.push({ role: "assistant", content: sent });   // 续接:已写出的内容并入上游请求,上游接着写
      src = ctx.http.open({ url: ctx.params.base_url + "/v1/generate", method: "POST",
        headers: { Authorization: "Bearer " + data.token }, body: upstreamBody(req), stream: true,
        sessionKey: "acme:" + ctx.key.id });         // 重开供下一轮消费
    }
  }
};
```

(节选——`live/trimLock/convert/upstreamBody/textOf/isTimeout` 等辅助函数由包自带;真实 handler.js 文件加 `// @ts-check` + d.ts reference 走类型守卫,零错误 = 与契约一致)


