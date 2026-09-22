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

### 写入途径

写入只有一条路:**`ctx.keys.overwrite({...})` 合并覆写**——给定键覆盖,未提及的键保留不动;写前 current 整体快照进 previous。不存在"整文档替换"语义,任务刷 token 不会误删用户手动加的其他键。

| 途径 | 时机 |
|---|---|
| 任务/钩子里 `ctx.keys.overwrite({...})` | 运行期(如刷到的 token) |
| 管理台手动新增/编辑 | 用户操作(走 keyWrite 归一化) |
| keySubmit 表单采集 | 钩子内自行 overwrite(只写真凭据;验证码这类一次性字段不该落库) |

读:`ctx.keys.get(name)`;旧版快照:`ctx.keys.previous(name)`(仅热升级的 onLoad/首轮任务提供,平时 undefined)。

protocol/filter 部件只读:`util.key(name)` 实时读当前值。

### keys.js 五钩子(全部可选;不写 → GUI 退化为通用 JSON 编辑)

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
  //    字符串                  → 成功 toast;写入用 ctx.keys.overwrite 只写真凭据
  keySubmit: function (ctx, values) {
    if (!values.account) return { errors: { account: "必填" } };
    var resp = ctx.http.run({ url: "https://example/login", method: "POST",
      body: JSON.stringify({ u: values.account, p: values.password }) });
    if (resp.status !== 200) return { message: "登录失败 " + resp.status };
    ctx.keys.overwrite({ token: JSON.parse(resp.body).token });   // 合并覆写:只写 token
    return "登录成功";
  },
};
```

要点:
- fields 建议显式 `name`(errors 按它定位输入框);缺省按 description/序号兜底
- **框架不会自动落库表单值**(验证码等一次性字段不该存),要写什么由脚本 overwrite 决定
- 抛错仍然可用(视为崩溃,GUI 显示错误文本),但业务拒绝请用 errors/message 通道
- keyAction/keySubmit 才有 `ctx.keys.overwrite` 与出站意义;keyWrite/keyRead 只有 keys.get/previous
- 各钩子超时:归一化/读取/表单声明 5s,按钮回调/提交 10s;超时该次操作失败
- GUI「新增凭据」预填名:已有键序号 `key-N` 兜底;keyForm 声明 fields 后以表单采集为正入口

### 手动管理兜底

无 keys.js 时用户仍可在 keys 弹层直接编辑 JSON 值(`{"token":"..."}` 任意结构),宿主原样保存——五钩子是**体验增强**,不是必配。

## 三、选择决策

- 固定不变、随上游账号走的凭据 → target secrets(`secretRefs` + `util.secret`),不走 keys
- 会过期需要刷新、或由脚本采集 → keys + 刷新任务
- 用户要填的行为参数 → settings 声明
- 一次性登录采集 token → keyForm/keyAction/keySubmit;只需用户贴一个 token → keyForm 单字段或手动 JSON
