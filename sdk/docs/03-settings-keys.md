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

keys 是**键池:每个键是独立实体 `{id, data, prev, updatedAt}`**——id 寻址、data 本体任意 JSON、prev 宿主管理的单级备份(JS 不可见)。与 target secrets(静态,用户填)无关——keys 由脚本运行时写入或用户在 GUI 采集。

**单键注入**:宿主每次任务执行只注入一个键(`ctx.key = {id, data}`),键池按 round-robin 轮转;多账号 = 多条目,任务天然逐号执行。

### 写入途径与写窗

写入两条路,**每次只写当前注入的这一个键**:

- **`ctx.keys.set(data)`** 任务窗:整体替换当前键 data;原 data 自动移入 prev(单级,可经管理台「恢复」找回)
- **`ctx.keys.merge(patch)`** 任务窗:当前 data 为对象时的便捷形态——对象浅合并(set 的语法糖)
- **`ctx.keys.set(data)`** keySubmit 窗:**创建新键池条目**(宿主生成 id,形如 `key-<时间>-<序号>`);多次调用 = 多条目

**写窗(阉割即权限)**:set/merge 只挂在任务执行与 keySubmit 的 ctx 上;onLoad/keyAction/keyWrite/keyRead/keyForm 的 ctx.keys 上无此方法(`typeof ctx.keys.set === "undefined"` 可探测)。

| 途径 | 时机 | 写窗 |
|---|---|---|
| 任务里 `ctx.keys.set(...)` / `ctx.keys.merge(...)` | 运行期(刷当前账号 token) | ✓ 任务执行 |
| keySubmit 表单采集 | 钩子内 set 创建条目(只写真凭据;验证码这类一次性字段不该落库) | ✓ keySubmit(创建窗) |
| 管理台手动新增/编辑 | 用户操作(声明了 keyWrite 时走归一化,否则原样保存) | 用户侧 |

| 钩子 | keys 能力 |
|---|---|
| 任务 handler / next | **set / merge**(写当前键)+ ctx.key |
| keySubmit | **set**(创建窗)+ http |
| keyAction | http(只读;发验证码无需写) |
| onLoad | 无 keys 写(初始化自己的 storage) |
| keyWrite / keyRead / keyForm | ctx.key(被操作键只读) |

**失败语义**:set/merge **立即持久化,不随任务回滚**(storage 才有事务缓冲)。每次写自动 data→prev;再写一次即覆盖 prev(只有一级)。

```js
// 任务窗:刷新当前注入键的 token(data 为对象形态)
ctx.keys.merge({ token: newToken });       // 等价 ctx.keys.set(浅合并结果)
```

```js
// keySubmit 窗:登录成功 → 创建一条新凭据条目
ctx.keys.set({ account: values.account, token: JSON.parse(resp.body).token });
```

读:`ctx.key.id` / `ctx.key.data`(当前注入键;无键 undefined)。**没有按名取任意键的 API**——脚本只见宿主注入的那一个键;全组枚举/编辑是管理台(GUI)的专属能力。

protocol/filter 部件只读:`util.key()` 返回当前注入键 data(请求级 round-robin 选键后注入;多余参数忽略)。data 形态由包约定:裸 token(string)或对象(map 取字段,如 `util.key().api_key`)。

### keys.js 五钩子(keyWrite / keyRead / keyForm / keyAction / keySubmit;全部可选;不写 → GUI 退化为通用 JSON 编辑)

keys.js 是独立族文件:纯协议包(无 hooks 部件/任务)也可携带,装入即得表单录入与详情解释增强。ctx.settings 快照同族注入(keyRead/keySubmit 出站查询可读包参数,如 base_url)。

```js
// @ts-check
/// <reference path="../ai-api-proxy.d.ts" />
module.exports = {
  // 1) 写入归一化(新增/编辑该键时):trim/补格式;undefined 返回 = 透传原值;拒绝写入 = 抛错
  keyWrite: function (ctx, keyId, newValue, oldValue) {
    if (keyId === "token" && typeof newValue === "string") return newValue.trim();
    return newValue;                        // 或 undefined 等价
  },

  // 2) 详情解释(该键「详情」按钮;ctx.key = 被查看键):返回任意可 JSON 化对象;输出务必脱敏
  keyRead: function (ctx, keyId) {
    return { masked: String(ctx.key && ctx.key.data).slice(0, 4) + "****" };
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
  //    字符串                  → 成功 toast;写入用 ctx.keys.set 创建凭据条目
  keySubmit: function (ctx, values) {
    if (!values.account) return { errors: { account: "必填" } };
    var resp = ctx.http.run({ url: "https://example/login", method: "POST",
      body: JSON.stringify({ u: values.account, p: values.password }) });
    if (resp.status !== 200) return { message: "登录失败 " + resp.status };
    ctx.keys.set({ account: values.account, token: JSON.parse(resp.body).token }); // 创建新条目(宿主生成 id)
    return "登录成功";
  },
};
```

要点:
- fields 建议显式 `name`(errors 按它定位输入框);缺省按 description/序号兜底
- **框架不会自动落库表单值**(验证码等一次性字段不该存),要写什么由脚本 set 决定
- keySubmit 每次 `set` = 一条新条目(重复提交 = 多账号并存;同毫秒序号防 id 碰撞)
- 抛错仍然可用(视为崩溃,GUI 显示错误文本),但业务拒绝请用 errors/message 通道
- 写窗:keyAction 无写方法(发验证码不需要写);业务需要写入的动作归入 keySubmit
- 各钩子超时:keyWrite/keyRead/keyForm 5s,keyAction/keySubmit 10s,init.js onLoad 5s(见 02);超时该次操作失败(keys 写入不回滚)
- GUI「新增凭据」预填名:`key-N` 兜底;keyForm 声明 fields 后以表单采集为正入口
- data 类型:JSON 可序列化(字符串/数字/布尔/对象/数组);64KB 单包上限(全组键联合计量)

### 手动管理兜底

无 keys.js 时用户仍可在 keys 页直接编辑 JSON 值(`{"token":"..."}` 任意结构),宿主原样保存——五钩子是**体验增强**,不是必配。管理台逐键提供:编辑(自动备份 prev)/详情(data+prev+解释)/删除(幂等)/恢复(data↔prev 交换)。

## 三、数据权限三分法

| 数据 | 维护者 | 代码侧 |
|---|---|---|
| settings | 用户(管理台) | 只读(ctx.settings 快照);改参数下一轮任务生效 |
| keys | 共同(用户表单/手动 + 脚本) | 限窗写:任务只写当前注入键;keySubmit 只创建条目;**无按名取键/删键 API**(误删面归零) |
| storage | 纯脚本 | 运行时自由读写;**事务语义**:执行期写私有缓冲,本次执行成功才归并持久化,抛错/超时全部丢弃(并发任务互相隔离) |

## 四、选择决策

- 固定不变、随上游账号走的凭据 → target secrets(`secretRefs` + `util.secret`),不走 keys
- 会过期需要刷新、或由脚本采集 → keys + 刷新任务(逐键注入,多账号 = 多条目)
- 用户要填的行为参数 → settings 声明
- 一次性登录采集 token → keyForm/keyAction/keySubmit;只需用户贴一个 token → keyForm 单字段或手动 JSON
