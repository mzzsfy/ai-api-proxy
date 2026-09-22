// ai-api-proxy 插件 SDK 类型定义(hook 全同步;代理层恰一次上游请求,无重试——归下游)
// 声明式单协议:一个协议部件服务且仅服务 manifest 声明的唯一协议槽
// 槽位取值只允许 "openai-completions" | "anthropic-messages";入口与槽位不符由主体 400 拒绝,不做任何转换
// 多文件布局(无单入口):init.js(onLoad)/ keys.js(keyWrite/keyRead/keyForm/keyAction/keySubmit)/ tasks/<name>.js(任务)
// 任意族文件可挂 settings 声明片段(module.exports.settings);共享常量走 lib/*.js require(包内相对路径,幂等单例)
// 全局注入:util / log / storage / setting(声明构建器)/ module / exports / require

/** 协议槽枚举(manifest:parts.protocol.protocol) */
export type ProtocolSlot = "openai-completions" | "anthropic-messages";

/** 声明形态(manifest:parts.protocol.forms 子集) */
export type Form = "streaming" | "non_streaming";

/** 管道请求上下文(部件只读;私有数据用 state) */
export interface Context {
  requestId: string;
  upstream: { name: string; models: string[] };
  target: { id: string; name: string; baseUrl: string };
  state: Record<string, unknown>;
  vars: { model: string; entryStream: boolean };
}

/** 上游请求载体(host 执行网络,恰一次) */
export interface UpstreamRequest {
  url: string;
  method: string;
  headers: Record<string, string>;
  /** 入口原文字节;声明式单协议下与入口同格式,部件不得改写语义 */
  body: string;
  /** 声明分发意图;须 ∈ 本部件 manifest 的 forms */
  stream: boolean;
}

/** 流式单帧(host 分帧;入方向恒为 {"event","data"} 信封) */
export interface SSEFrame {
  event: string;
  data: string;
}

/** 协议适配部件(恰一;主包必须含;协议槽由 manifest 声明) */
export interface ProtocolHooks {
  /**
   * 构造上游请求(同步)
   * @param entry 入口原文字节(声明协议格式,未经任何转换)
   */
  buildRequest(ctx: Context, entry: string): UpstreamRequest;
  /**
   * 流式帧 → 声明协议事件对象数组的 JSON 字符串;返回 null / "[]" / "" 跳帧
   * 仅在 manifest 声明 streaming 时实现,否则校验期拒绝
   * 失败 = 该帧降级原样透传并告警,流不中断
   */
  mapEvent?(ctx: Context, event: string): string | null;
  /**
   * 非流式响应体 → 声明协议响应 JSON 字符串
   * 仅在 manifest 声明 non_streaming 时实现,否则校验期拒绝
   */
  mapResponse?(ctx: Context, body: string): string;
  /** 错误终局映射(可选;一次;未实现原样透传) */
  mapError?(ctx: Context, status: number, body: string): string;
}

/** 请求修改部件(0..n 串行;不参与能力协商) */
export interface FilterHooks {
  /** 请求改写(入口格式 → 入口格式);失败 = 请求 502 带部件名 */
  mapRequest(ctx: Context, entry: string): string | null;
  /** 流式 chunk 改写(逆序);失败 = 单 chunk 降级 raw */
  mapChunk?(ctx: Context, chunk: string): string | null;
  /** 非流式响应改写(逆序);失败 = 502 */
  mapResponse?(ctx: Context, resp: string): string | null;
}

/**
 * 部件 configSchema 实例值(factory 闭包捕获,不经 ctx)
 * 注意:config 另含 host 注入的 protocol(即本部件声明的协议槽),不得在 configSchema 中重复声明该键,
 * 否则 key 剥离会把注入值删掉而看不到声明槽
 */
export interface PartConfig {
  /** host 注入的协议槽值(= manifest 声明的 protocol) */
  protocol: ProtocolSlot;
  [key: string]: unknown;
}

export type ProtocolPart = ProtocolHooks | ((config: PartConfig) => ProtocolHooks);
export type FilterPart = FilterHooks | ((config: PartConfig) => FilterHooks);

/** util 宿主面(全部同步) */
export interface Util {
  deepMerge(a: object, b: object): object;
  deepClone<T>(v: T): T;
  get(obj: object, path: string): unknown;
  set(obj: object, path: string, v: unknown): object;
  pick(obj: object, keys: string[]): object;
  omit(obj: object, keys: string[]): object;
  b64encode(s: string): string;
  b64decode(s: string): string;
  b64urlEncode(s: string): string;
  b64urlDecode(s: string): string;
  sha256hex(s: string): string;
  hmacSha256hex(key: string, s: string): string;
  /** RFC 4122 v4 */
  uuid(): string;
  /** Unix 毫秒 */
  now(): number;
  /** RFC3339 UTC */
  isoNow(): string;
  template(s: string, vars: Record<string, unknown>): string;
  inspect(obj: unknown): string;
  /** 当前 target.secrets 的键(ref 必须 ∈ 部件 secretRefs);缺键抛错 */
  secret(ref: string): string;
  /** 读包级 key 当前值;无值 undefined(protocol/filter 侧只读;hooks 任务写入) */
  key(name: string): unknown;
  /**
   * 主动失效上报:aap 传输失效命令转发(仅失效当前绑定,不触发重试)
   * scope: "lease"(value=lease_id 32 字符 hex)| "egress"(value=IP 字符串)
   * 成功 true;非法 scope/非 aap 传输/节点失败抛错
   */
  evict(transport: string, scope: "lease" | "egress", value: string): boolean;
}

export interface Log {
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
}

export interface Storage {
  get(key: string): string | null;
  /** 单键上限 64KB;ns=包名 */
  set(key: string, value: string): void;
  delete(key: string): void;
}

/** 包级 key 只读说明:hooks 任务写入,protocol/filter 侧经 util.key 只读实时 */

/** ctx.http 请求体(hooks 部件专属;经宿主全局出站传输) */
export interface HookHttpRequest {
  url: string;
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  /** 缺省 = min(30s, 外层剩余);显式取值亦按外层剩余钳制 */
  timeoutMs?: number;
}

/** ctx.http 响应体 */
export interface HookHttpResponse {
  status: number;
  headers: Record<string, string>;
  /** 超过 1MB 截断 */
  body: string;
}

/** ctx.keys 包级 key 读写(hooks 部件专属;set 前 current 整体移入 previous) */
export interface HookKeys {
  get(name: string): unknown;
  set(values: Record<string, unknown>): void;
  /** 仅热加载提供旧包快照;启停/首载 undefined */
  previous(name: string): unknown;
  /** 键名枚举(排序;不含值——值走 get;多账号遍历场景) */
  list(): string[];
}

/** settings 声明构建器(setting 全局;宿主求值时注入,GUI 渲染归宿主) */
export interface SettingBuilders {
  string(opts: SettingOptions): SettingDecl;
  int(opts: SettingOptions): SettingDecl;
  number(opts: SettingOptions): SettingDecl;
  bool(opts: SettingOptions): SettingDecl;
  enum(opts: SettingOptions & { values: string[] }): SettingDecl;
}

export interface SettingOptions {
  /** 展示名 */
  description?: string;
  /** 缺省值(未覆盖时生效) */
  default?: unknown;
  required?: boolean;
  [key: string]: unknown;
}

export interface SettingDecl {
  type: "string" | "int" | "number" | "bool" | "enum";
  [key: string]: unknown;
}

/** hooks 部件调用上下文(onLoad/任务/keys 钩子;settings 仅注入了快照时存在) */
export interface HookContext {
  http: { run(req: HookHttpRequest): HookHttpResponse };
  keys: HookKeys;
  cron: { runAt: string };
  /** 运行时配置快照(声明 ⊕ 管理台覆盖;settings.js/init.js/keys.js/tasks 均可读) */
  settings?: Record<string, unknown>;
  /** 当前任务名(多行同指一实现时区分调用来源;仅任务调用注入) */
  task?: string;
}

/**
 * 任务文件导出(tasks/<name>.js 或 manifest 缺省路径)
 * handler:任务体;next 形态(声明 next:true)另导出 next
 */
export interface TaskModule {
  /** 任务体(同步) */
  handler(ctx: HookContext): void;
  /**
   * 自调度链(声明 next:true 时必须导出;与 cron 互斥)
   * 返回下次触发的 Unix 毫秒时间戳;null = 停止链;返回值 ≤ now = 立即再触发
   */
  next?(ctx: HookContext): number | null;
  /** 可挂 settings 声明片段 */
  settings?: Record<string, unknown>;
}

/** init.js 导出(onLoad 处理器) */
export interface InitModule {
  onLoad(ctx: HookContext): void;
  settings?: Record<string, unknown>;
}

/**
 * keys.js 导出(凭据读写钩子 + 添加表单采集;全部可选)
 * ctx 裁剪:keyWrite/keyRead 仅 keys.get/previous;keyForm 仅 keys.get;
 * keyAction/keySubmit 另含 keys.set + http(采集 = 出站动作)
 */
export interface KeysModule {
  /** 键写入归一化(管理台新增/编辑);undefined/null = 透传;拒绝写入须抛错 */
  keyWrite?(ctx: HookContext, keyName: string, newValue: unknown, oldValue: unknown): unknown;
  /** 键详情解释(管理台 [详情]);undefined → detail null;输出须脱敏 */
  keyRead?(ctx: HookContext, keyName: string): unknown;
  /**
   * 添加表单声明(声明后 GUI 弹层渲染采集表单)
   * fields 用 setting 构建器声明;actions 按钮点击回调 keyAction
   */
  keyForm?(ctx: HookContext): {
    fields: SettingDecl[];
    actions?: { name: string; label: string }[];
  };
  /** 表单按钮回调(可出站;如发送验证码);返回值 toast 呈现 */
  keyAction?(ctx: HookContext, action: string, values: Record<string, unknown>): unknown;
  /** 表单提交(可出站;写入经 ctx.keys.set——校验全部通过后再 set,set 后抛错不回滚);返回值 toast 呈现 */
  keySubmit?(ctx: HookContext, values: Record<string, unknown>): unknown;
  settings?: Record<string, unknown>;
}

/** 包级 key 只读说明:hooks 任务写入,protocol/filter 侧经 util.key 只读实时 */
declare global {
  const util: Util;
  const log: Log;
  const storage: Storage;
  /** settings 声明构建器(族文件求值时注入;keyForm 函数体运行期同样可用) */
  const setting: SettingBuilders;
  /** CommonJS 模块面(包内相对路径;扩展名可选/目录索引;同 runtime 幂等单例) */
  function require(spec: string): unknown;
  var module: { exports: unknown };
  var exports: unknown;
}

export {}