# 03 · 参数配置与凭据(keys)管理

hooks 包的两类运行期数据:**参数**(settings,低频配置,用户在管理台填)与**凭据**(keys,脚本运行期产生/采集的动态值,如 token)。

## 一、参数配置(settings)

### 声明:任意族文件挂 `module.exports.settings`

```js
// settings.js(推荐:共享声明的家;纯声明无逻辑)
module.exports.settings = {
  // 全局构建器 setting,五种类型;opts 透传进 schema
  endpoint: setting.string({ description: "接口地址", default: "https://example.com", required: true }),
  retryCount: setting.int({ description: "重试次数", default: 3 }),
  ratio: setting.number({ description: "倍率", default: 1.5 }),
  verbose: setting.bool({ description: "详细日志", default: false }),
  mode: setting.enum({ description: "运行模式", values: ["fast", "safe"], default: "fast" }),
};
```

- init.js / keys.js / tasks/*.js 也都能挂自己的 `module.exports.settings`,宿主按 settings.js → init.js → keys.js → tasks 固定序合并(同名后者胜)
- 声明在安装时被提取进库(declaration_json),语法错误拒绝安装

### 读取:`ctx.settings` 快照

```js
module.exports = {
  handler: function (ctx) {
    var ep = ctx.settings.endpoint;        // 生效值 = 声明 default ⊕ 用户覆盖
    var all = ctx.settings;                 // 点访问或整体遍历均可;只读
  },
};
```

- 任务每次执行时取快照 → **改参数下一轮任务生效**,无需重启、不触发 onLoad
- 声明里写了 default 的项,用户没改也拿得到默认值(default 不落库,派生)

### 用户侧语义(理解即可,不必写代码)

- 管理台「配置」按声明渲染表单(string/int/number/bool/enum 五类)
- 保存走乐观锁:携带旧 version 提交,过期返回 409 刷新重填
- 提交值剔除声明外的键、类型不对 400;**等于默认值的提交不入库**(恢复默认 = 不传该键)
- 任务行可被用户覆盖 cron 或停用;`disabled` 的行不注册调度

## 二、凭据(keys)管理

keys 是**包级 name → value 映射**(整包一份,current + 上一版 previous)。与 target secrets(静态,用户填)无关——keys 由脚本运行时写入或用户在 GUI 采集。

### 写入途径与写窗

写入两条路,都是**局部命名操作**——**不存在整文档替换 API,这是有意设计**:

> keys 文档有两个写者:脚本(访客,刷新自己采集的凭据)和用户(主人,经管理台手动加备用凭据/多账号)。
> 全量重写会无声吞掉用户手动加的键——凭据静默丢失,下次请求 401 才暴露,故障面不可接受。
> 因此脚本侧永远只有局部命名操作;整文档编辑是用户在 GUI JSON 弹层的专属能力。

- **`ctx.keys.merge({...})`** 键级合并:给定键覆盖,**未提及键保留**
- **`ctx.keys.remove("a", "b")`** 删除指定键(变参,不存在的键忽略)——**凭据最小持有**:密码换 token 后密码必须删,不当作废键留库

两者写前 current 都整体快照进 previous(误删可找回);一次变更只产生一次快照(merge+remove 连续调用 = 两次变更、两次快照,previous 恒为最近一次变更前的状态)。

典型凭据轮转(密码换 token):

```js
ctx.keys.merge({ token: newToken });
ctx.keys.remove("password", "oldToken");   // 作废键即删
```

**写窗(阉割即权限)**:merge/remove 只挂在任务执行与 keySubmit 的 ctx 上;onLoad/keyAction/keyWrite/keyRead/keyForm 的 ctx.keys 上无此方法(`typeof ctx.keys.merge === "undefined"` 可探测)。

| 途径 | 时机 | 写窗 |
|---|---|---|
| 任务里 `ctx.keys.merge({...})` / `ctx.keys.remove(...)` | 运行期(刷 token / 清理作废键) | ✓ 任务执行 |
| keySubmit 表单采集 | 钩子内自行 merge/remove(只写真凭据;验证码这类一次性字段不该落库) | ✓ keySubmit |
| 管理台手动新增/编辑 | 用户操作(声明了 keyWrite 时走归一化,否则原样保存) | 用户侧 |

| 钩子 | keys 能力 |
|---|---|
| 任务 handler / next | get / previous / list / **merge** / **remove** |
| keySubmit | get / previous / list / **merge** / **remove** + http |
| keyAction | get / previous / list + http(只读;发验证码无需写) |
| onLoad | get / previous / list(只读) |
| keyWrite / keyRead / keyForm | get / previous / list(最小面) |

**keys API 参考**(全部钩子可用的只读面):

| 方法 | 签名 | 说明 |
|---|---|---|
| get | `get(name)` | 当前值;无值 undefined |
| previous | `previous(name)` | 上一版快照值(最近一次变更前的 current 中该键);从未变更过返回 undefined |
| list | `list()` | 键名数组,排序,不含值(枚举与读取分离) |

**失败语义**:merge/remove **立即持久化,不随任务回滚**(storage 才有事务缓冲)。凭据轮转因此有顺序约束——**先写新键,后删旧键**:

```js
ctx.keys.merge({ token: newToken });   // 1) 新凭据先落库
ctx.keys.remove("password");           // 2) 再删作废键;即使此处任务崩溃,token 已在
```

倒过来(先删后写)中途崩溃 = 旧凭据已删、新凭据未成,current 凭据失效(previous 尚留删除前快照,可人工找回,但服务已中断)。

读:`ctx.keys.get(name)`;上一版快照:`ctx.keys.previous(name)`(库中无快照时 undefined)。

protocol/filter 部件只读:`util.key(name)` 实时读当前值(与 ctx.keys.get 同源,只是入口在 util——protocol/filter 的 ctx 无 keys 命名空间)。

### keys.js 五钩子(keyWrite / keyRead / keyForm / keyAction / keySubmit;全部可选;不写 → GUI 退化为通用 JSON 编辑)

```js
// @ts-check
/// <reference path="../ai-api-proxy.d.ts" />
module.exports = {
  // 1) 写入归一化(新增/编辑任何键都会过这里):trim/补格式;undefined 返回 = 透传原值;拒绝写入 = 抛错
  keyWrite: function (ctx, keyName, newValue, oldValue) {
    if (keyName === "token" && typeof newValue === "string") return newValue.trim();
    return newValue;                        // 或 undefined 等价
  },

  // 2) 详情解释(每键「详情」按钮):返回任意可 JSON 化对象;输出务必脱敏
  keyRead: function (ctx, keyName) {
    return { masked: String(ctx.keys.get(keyName)).slice(0, 4) + "****" };
  },

  // 3) 添加表单声明(「添加凭据」弹层渲染;fields 用 setting 构建器)
  keyForm: function (ctx) {
    return {
      fields: [
        setting.string({ name: "account", description: "账号", required: true }),
        setting.string({ name: "password", description: "密码" }),
      ],
      actions: [{ name: "sendCode", label: "发送验证码" }],   // 可选
    };
  },

  // 4) 表单按钮回调(可出站,如发验证码);返回值 toast 展示
  keyAction: function (ctx, action, values) {
    ctx.http.run({ url: "https://example/sendCode?to=" + values.account });
    return "已发送";
  },

  // 5) 表单提交(可出站)。三种反馈通道,无需抛异常:
  //    {errors: {字段: 原因}} → 字段级拒绝:GUI 逐输入框标红,存储不变
  //    {message: "..."}       → 失败提示(toast),存储不变
  //    字符串                  → 成功 toast;写入用 ctx.keys.merge 只写真凭据
  keySubmit: function (ctx, values) {
    if (!values.account) return { errors: { account: "必填" } };
    var resp = ctx.http.run({ url: "https://example/login", method: "POST",
      body: JSON.stringify({ u: values.account, p: values.password }) });
    if (resp.status !== 200) return { message: "登录失败 " + resp.status };
    ctx.keys.merge({ token: JSON.parse(resp.body).token });   // 键级合并:只写 token
    ctx.keys.remove("password");                              // 凭据轮转:作废键即删(最小持有)
    return "登录成功";
  },
};
```

要点:
- fields 建议显式 `name`(errors 按它定位输入框);缺省按 description/序号兜底
- **框架不会自动落库表单值**(验证码等一次性字段不该存),要写什么由脚本 merge/remove 决定
- GUI 提交反馈的变更集含 merge 与 remove 的全部键名(删除键也会列出)
- 抛错仍然可用(视为崩溃,GUI 显示错误文本),但业务拒绝请用 errors/message 通道
- 写窗:keyAction 无 merge/remove(发验证码不需要写);业务需要写入的动作归入 keySubmit
- 各钩子超时:keyWrite/keyRead/keyForm 5s,keyAction/keySubmit 10s,init.js onLoad 5s(见 02);超时该次操作失败(storage 缓冲一并丢弃;keys 写入无缓冲,已执行的 merge/remove 不回滚)
- GUI「新增凭据」预填名:已有键序号 `key-N` 兜底;keyForm 声明 fields 后以表单采集为正入口
- 值类型:JSON 可序列化(字符串/数字/布尔/对象/数组);64KB 单包上限(current+previous 联合计量)

### 手动管理兜底

无 keys.js 时用户仍可在 keys 弹层直接编辑 JSON 值(`{"token":"..."}` 任意结构),宿主原样保存——五钩子是**体验增强**,不是必配。

## 三、数据权限三分法

| 数据 | 维护者 | 代码侧 |
|---|---|---|
| settings | 用户(管理台) | 只读(ctx.settings 快照);改参数下一轮任务生效 |
| keys | 共同(用户表单/手动 + 脚本) | 限窗写:merge 仅任务执行与 keySubmit;键级合并(给定键覆盖,未提及保留)不误删 |
| storage | 纯脚本 | 运行时自由读写;**事务语义**:执行期写私有缓冲,本次执行成功才归并持久化,抛错/超时全部丢弃(并发任务互相隔离) |

## 四、选择决策

- 固定不变、随上游账号走的凭据 → target secrets(`secretRefs` + `util.secret`),不走 keys
- 会过期需要刷新、或由脚本采集 → keys + 刷新任务
- 用户要填的行为参数 → settings 声明
- 一次性登录采集 token → keyForm/keyAction/keySubmit;只需用户贴一个 token → keyForm 单字段或手动 JSON
