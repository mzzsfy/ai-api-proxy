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

- init.js / tasks/*.js 也都能挂自己的 `module.exports.settings`,宿主按 settings.js → init.js → tasks 固定序合并(同名槽拒装(报文件+槽名,冲突显式暴露不静默覆盖));handler.js/login/index.js 不参与族序(导出 settings = 拒装,见 [05](05-handler.md))
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

keys 是**键池:每个键是独立实体 `{id, data, prev, updatedAt}`**——id 寻址、data 本体任意 JSON、prev 宿主管理的单级备份(JS 不可见)。与上游 target 配置(静态连接信息/参数,用户填)无关——keys 由脚本运行时写入或用户在 GUI 采集。

**单键注入**:宿主每次任务执行只注入一个键(`ctx.key = {id, data}`),键池按 round-robin 轮转;多账号 = 多条目,任务天然逐号执行。

### 写入途径与写窗

写入三条路,**全经宿主 Keys.Set 串行**:

| 途径 | 时机 | 形态 |
|---|---|---|
| **login/**(GUI 路径) | 管理页采集后 `aap.login(payload)` | login/index.js 返回 data → 宿主 Keys.Set 落**新账号条目**(登录路径唯一落库通道) |
| **任务窗 `ctx.keys.set/merge`** | 任务运行期(刷当前账号 token) | 整体替换/浅合并**当前注入键** data;原 data 自动移入 prev(单级) |
| **管理台手动新增/编辑** | 用户操作 | 原样保存(无归一化钩子、无掩码回填;展示脱敏归 inspect.js) |

要点:

- **请求期零键池写**:handler 的 ctx 无 keys 写成员——动态凭据走 storage 槽(见 [05](05-handler.md)),GUI 可见性经 inspect 查看时不回写键池
- set/merge **立即持久化,不随任务回滚**;每次写自动 data→prev,再写覆盖 prev(单级)
- handler 引导复用 login 逻辑时**不落键池**:`require("./login/index.js")` 返回值作者自理(标准姿势 = unlock 落 storage 槽),见下文双路径

```js
// 任务窗:刷新当前注入键的 token(data 为对象形态)
ctx.keys.merge({ token: newToken });       // 等价 ctx.keys.set(浅合并结果)
```

读:`ctx.key.id` / `ctx.key.data`(当前注入键;无键 undefined)。**没有按名取任意键的 API**——脚本只见宿主注入的那一个键;全组枚举/编辑是管理台(GUI)的专属能力。

protocol/filter 部件只读:`util.key()` 返回当前注入键 data(请求级 round-robin 选键后注入;多余参数忽略)。data 形态由包约定:裸 token(string)或对象(map 取字段,如 `util.key().api_key`)。

### login/ 目录(自定义凭据采集)

**凭据区只有两态,无中间态**:包不含 `login/` 目录 → 宿主默认单字段 api_key 表单(现状同款);包含 → 管理页接管自定义登录交互。

```
login/
├── ui/           # 页面资源;ui/index.html 必选(宿主整目录 serve)
│   └── index.html
└── index.js      # 登录逻辑入口:module.exports = (ctx) => data
```

- **绑定成对**:`login/` 存在时 index.js 与 ui/index.html 必须同在,缺一拒装;keys.js(旧五钩子文件)出现即拒装
- 页面为浏览器原生 HTML/JS(不经 goja,可引用目录内任意资源);宿主注入受控桥 `window.aap`,全部限域本包:

| 桥端点 | 说明 |
|---|---|
| `aap.login(input)` | 调 login/index.js(ctx.params = input),返回值经宿主 Keys.Set 落新账号——**登录路径唯一落库通道** |
| `aap.accounts.list()` / `get(id)` | 账号列表/详情(inspect 变换后视图) |
| `aap.storage.get/set` | 包级 KV |
| `aap.http.request` | 出站(经代理包装,任务同缺省) |
| `aap.meta()` | `{name, version, revision, params}` 供页面自适应 |

**桥不暴露启停/删除账号端点**(账号生命周期操作归宿主标准 GUI,避免双入口)。

多步登录(发验证码→轮询→提交)由页面自编排,桥只提供原子能力。

```js
module.exports = function (ctx) {          // ctx = {params, http, storage, util}——无锁、无 reply
  var resp = ctx.http.request({ url: "https://example/login", method: "POST",
    body: JSON.stringify(ctx.params) });
  return { account: ctx.params.account, token: JSON.parse(resp.body).token };
};
```

**双路径(单一逻辑源)**:

| 调用点 | 执行环境 | ctx.params | 返回值归宿 |
|---|---|---|---|
| GUI `aap.login(payload)` | hooks runtime | 页面表单 payload | 宿主 Keys.Set 落库 |
| handler 引导 `require("./login/index.js")(裁剪ctx)` | handler runtime 内 | 作者合成 | **作者自理,不落键池**(标准姿势 = unlock 落 storage 槽) |

handler 引导传**裁剪对象** `{params: {...}, http: ctx.http, storage: 剥离锁成员后的 storage(仅 get/set/delete), util}`——两调用点形态一致;login 内 set handler 持有的槽会抛错(引导产物由 handler 落值)。

### inspect.js(展示变换,可选独立)

```js
module.exports = function (data) {          // 纯函数,无 ctx
  return { account: data.account, masked: String(data.token).slice(0, 4) + "****" };
};
```

- GUI 展示账号时经它变换/脱敏;不写 → data 原样 JSON 展示
- 手编账号 PUT 原样入库(无掩码回填)——展示侧脱敏归 inspect,编辑明文用户自担

### 手动管理兜底

用户始终可在 keys 页直接编辑 JSON 值,宿主原样保存。管理台逐键提供:编辑(自动备份 prev)/详情(data+prev+inspect 视图)/删除(幂等)/恢复(data↔prev 交换)。

## 三、数据权限三分法

| 数据 | 维护者 | 代码侧 |
|---|---|---|
| settings | 用户(管理台) | 只读(ctx.settings 快照);改参数下一轮任务生效 |
| keys | 共同(login/ 表单 + 任务窗 + GUI 手编) | 限窗写:任务只写当前注入键;handler 请求期零键池写(动态凭据走 storage 槽);**无按名取键/删键 API**(误删面归零) |
| storage | 纯脚本 | 运行时自由读写;**写入语义按执行者分叉**:handler 执行 = 事务缓冲(return 归并,失败丢弃,例外:unlock 同步写权威层,见 [05](05-handler.md));任务/init/login 等其余执行 = 直写持久层,不回滚 |

## 四、选择决策

- 固定不变、随上游账号走的凭据 → 上游 target 配置(连接信息+预设参数,经 factory config 注入),不走键池
- 会过期需要刷新 → 键池 + 刷新任务(逐键注入);或 handler 模式全托管(storage 槽 + login 引导,见 [05](05-handler.md))
- 用户要填的行为参数 → settings 声明
- 一次性登录采集账号 → login/ 目录(自定义多步交互);只需用户贴一个 token → GUI 手编(默认表单)
- 展示需脱敏/美化 → inspect.js
