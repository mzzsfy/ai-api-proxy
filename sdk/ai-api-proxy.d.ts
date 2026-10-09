// ai-api-proxy 插件 SDK 类型定义(hook 全同步;代理层恰一次上游请求,无重试——归下游)
// 声明式单协议:一个协议部件服务且仅服务 manifest 声明的唯一协议槽
// 槽位取值只允许 "openai-completions" | "anthropic-messages";入口与槽位不符由主体 400 拒绝,不做任何转换
// 多文件布局(无单入口):init.js(onLoad)/ tasks/<name>.js(任务)/ handler.js(handler 部件,与 protocol 互斥)/
//   login/index.js + login/ui/index.html(凭据采集,绑定成对)/ inspect.js(账号展示变换)
// keys.js 五钩子已废除:出现即拒装;凭据管理见 login/ 目录(03 文档)
// 任意族文件可挂 settings 声明片段(module.exports.settings;handler.js/login/index.js 除外——导出即拒装);共享常量走 lib/*.js require(包内相对路径,幂等单例)
// 全局注入:util / log / storage / setting(声明构建器)/ module / exports / require

/** 协议槽枚举(manifest:parts.protocol.protocol) */
export type ProtocolSlot = "openai-completions" | "anthropic-messages";

/** 请求形态(由实现推导:导出 mapEvent = 支持流式;导出 mapResponse = 支持非流式) */
export type Form = "streaming" | "non_streaming";

/** 管道请求上下文(部件只读;私有数据用 state;v2:无 target——连接信息在 factory config,密钥经 util.key) */
export interface Context {
  requestId: string;
  /** 模型行名(= 路由命中的模型名) */
  upstream: { name: string };
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
  /** 声明分发意图;须 ∈ 实现推导支持的形态(导出 mapEvent = 流式,mapResponse = 非流式) */
  stream: boolean;
  /** 出站传输实例名(v2;空/缺省 = 内置 direct;命名实例在 config.yaml transports 定义) */
  transport?: string | null;
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
 * 部件参数闭包(v2:ResolveParams 产物 = 声明 default ⊕ 包参数 ⊕ 模型行覆盖;factory 闭包捕获,不经 ctx)
 * 注意:参数槽在本文件 module.exports.settings 声明(与读值代码同址);未声明槽不进 config
 */
export interface PartConfig {
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
  /** 当前注入键 data(无键 undefined;唯一凭据出口;data 形态由包约定:string token 或 {api_key,...});多余参数忽略 */
  key(): unknown;
  /**
   * 主动失效上报:aap 传输失效命令转发(仅失效当前绑定,不触发重试)
   * scope: "lease"(value=lease_id 32 字符 hex)| "egress"(value=IP 字符串)
   * 成功 true;非法 scope/非 aap 传输/节点失败抛错
   */
  evict(transport: string, scope: "lease" | "egress", value: string): boolean;
  /**
   * 原始 JSON 文档句柄:读字面量、定位编辑,不经 JS Number/stringify,未触碰文本逐字节保留
   * 非法 JSON 抛错。路径为 gjson 点路径;数组索引;`-1` 读最后元素,`set` 路径尾 `.-1` 为追加;键含 `.` 不支持
   */
  json(raw: string): JsonDoc;
}

export interface JsonDocOp {
  /** set:替换(尾 `.-1` 追加)| remove:删除(缺失跳过)| setIfAbsent:存在跳过 */
  op: "set" | "remove" | "setIfAbsent";
  path: string;
  /** JSON 字面量字符串(仅 set/setIfAbsent;数字必须以字面量传入保精度) */
  value?: string;
}

export interface JsonDoc {
  /** 定位读取,返回字面量文本(对象/数组为子树原文;缺失 undefined) */
  get(path: string): string | undefined;
  has(path: string): boolean;
  /** 顺序执行编辑;回执 "<op> <path>" 列表。指令中途失败时已执行指令生效,异常抛出 */
  ops(list: JsonDocOp[]): { applied: string[]; skipped: string[] };
  /** 终检合法后产出请求体与全量回执 */
  finalize(): { body: string; applied: string[]; skipped: string[] };
}

export interface Log {
  info(...args: unknown[]): void;
  warn(...args: unknown[]): void;
  error(...args: unknown[]): void;
}

export interface Storage {
  get(key: string): string | null;
  /** 单键上限 64KB;ns=包名;写入语义按执行者分叉:handler 执行 = 事务缓冲(return 归并,失败丢弃;例外:unlock 同步写权威层);任务/init/login 等其余执行 = 直写持久层 */
  set(key: string, value: string): void;
  delete(key: string): void;
}

/** ctx.http 请求体(hooks 部件专属;经宿主全局出站传输) */
export interface HookHttpRequest {
  url: string;
  method?: string;
  headers?: Record<string, string>;
  body?: string;
  /** 缺省 = min(30s, 外层剩余);显式取值亦按外层剩余钳制 */
  timeoutMs?: number;
  /** 出站命名实例(不存在抛错);缺省 = 全局出站 */
  transport?: string;
  /** 亲和素材透传(pipeline Request.APIKey) */
  sessionKey?: string;
}

/** ctx.http 响应体 */
export interface HookHttpResponse {
  status: number;
  headers: Record<string, string>;
  /** 超过 1MB 截断 */
  body: string;
}

/** ctx.keys 当前键窗口(hooks 部件专属;宿主每次注入一个键) */
export interface KeyRef {
  /** 键 ID(管理台手工键 = 键名;login 落库 = 宿主生成) */
  id: string;
  /** data 本体(任意 JSON;形态由包约定) */
  data: unknown;
}

/** ctx.keys 写窗能力:任务窗 = set/merge 整体替换当前键;其余钩子无写方法 */
export interface HookKeys {
  /**
   * 任务窗:整体替换当前键 data(原 data 自动移入 prev,单级)
   * 仅任务执行的 ctx 挂载(其余钩子 undefined——阉割即权限);立即持久化,不随任务回滚
   */
  set?(data: unknown): string | void;
  /**
   * 仅任务窗:对象浅合并到当前 data(当前 data 须为对象;set 语义的便捷形态)
   */
  merge?(patch: Record<string, unknown>): void;
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

/** hooks 部件调用上下文(onLoad/任务;settings 仅注入了快照时存在) */
export interface HookContext {
  http: { request(req: HookHttpRequest): HandlerHttpResponse };
  /** keys 写窗能力(仅任务执行挂载;其余钩子 undefined——阉割即权限) */
  keys?: HookKeys;
  cron: { runAt: string };
  /** 当前注入键(任务窗存在;无键 undefined) */
  key?: KeyRef;
  /** 运行时配置快照(声明 ⊕ 管理台覆盖;settings.js/init.js/tasks 均可读) */
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
 * handler.js 导出(与 protocol 部件互斥;manifest parts.handler 声明入口协议)
 * 主函数持有整个请求生命周期:出站时机/次数/流处理/续接全内联,同步直线代码
 */
export interface HandlerModule {
  (ctx: HandlerContext): void;
  /**
   * 可选:数据准备(请求开始时执行,返回值 = ctx.data 初始值)
   * 可用原语:http/storage(含槽锁)/settings 全量,request/params/key 只读同主函数;禁 reply 与 ctx.data(访问即抛错)
   * 持有的槽锁自然延续到主函数,终局统一兜底释放
   */
  prepare?(ctx: PrepareContext): unknown;
}

/** prepare 调用上下文(与 HandlerContext 同源,去掉 reply 与 data) */
export interface PrepareContext {
  request: HandlerRequestInfo;
  params: Record<string, any>;
  key: KeyRef;
  http: HandlerHttp;
  storage: SlotLockStorage;
  settings: HandlerSettings;
}

/** 入口请求信息(经 filters;headers 只读) */
export interface HandlerRequestInfo {
  model: string;
  stream: boolean;
  body: string;
  headers: Record<string, string>;
}

/** 出站请求体(全部经代理包装;transport 缺省 = 组参数解析结果的传输槽) */
export interface HandlerHttpRequest {
  url: string;
  method?: string;
  /**
   * 缺省注入浏览器头组(UA/Accept/Accept-Language/Accept-Encoding/Sec-Ch-Ua*,显式值优先);
   * headers["X-Aap-Browser-Defaults"]="off" 关闭整组(纯 API 场景)
   */
  headers?: Record<string, string>;
  body?: string;
  /** 命名传输实例(config.yaml transports);不存在抛错 */
  transport?: string;
  /** 代理会话粘滞键(透传) */
  sessionKey?: string;
  /** 本次调用覆写 totalTimeoutMs */
  timeoutMs?: number;
}

/** 缓冲出站响应(body 无大小上限) */
export interface HandlerHttpResponse {
  status: number;
  headers: Record<string, string>;
  body: string;
}

/** 流式出站 source(frames/body 混用抛错;超时/断流后迭代器终态) */
export interface HandlerSource {
  status: number;
  headers: Record<string, string>;
  /** 同步迭代器:逐帧产出 {"event","data"};Content-Type 非 event-stream 时抛错(走 body) */
  frames(): IterableIterator<SSEFrame>;
  /** 整块取回(与 frames 混用抛错;status>=300 时为全量错误体) */
  body(): string;
}

/** handler 出站原语 */
export interface HandlerHttp {
  request(req: HandlerHttpRequest): HandlerHttpResponse;
  /** 流式出站(必须 stream:true,缺省/false 抛错) */
  open(req: HandlerHttpRequest & { stream: true }): HandlerSource;
}

/** 应答原语(形态强制校验:json 仅入口 stream=false;sse/event/end/error 仅 stream=true;越序全抛错;零应答 = 502) */
export interface HandlerReply {
  /** 非流式应答(一次;其后任何 reply 调用抛错) */
  json(status: number, bodyText: string): void;
  /** 流式开场(一次) */
  sse(): void;
  /** 产出一个入口协议事件(宿主按入口协议编码,直接 flush) */
  event(obj: unknown): void;
  /** 流式正常收尾 */
  end(): void;
  /** 流式错误终局;error 已写 = 视同已收尾,return 即正常终局 */
  error(obj: unknown): void;
}

/** 执行级出站超时覆写(合并语义,执行局部;非法键抛错) */
export interface HandlerSettings {
  overwrite(opts: {
    /** 建连超时(缺省 30*1000ms;调用时定死) */
    connectTimeoutMs?: number;
    /** 响应超时:发出到首字节(缺省 60*1000ms;调用时定死,首字节后让位帧间) */
    responseTimeoutMs?: number;
    /** 帧间超时(缺省 180*1000ms;逐帧隙重武装,循环体内覆写下一帧隙生效) */
    frameIdleTimeoutMs?: number;
    /** 单次出站整体(缺省 10*60*1000ms;调用时定死) */
    totalTimeoutMs?: number;
  }): void;
}

/**
 * storage + 槽锁(handler 专属)
 * 两级:②执行事务缓冲(set/delete;return 归并,抛错丢弃)+ ③SQLite 唯一权威
 * 槽值 = 字符串;槽写法判据:token 槽全程锁通道,普通槽全程 set/delete,禁止混用
 */
export interface SlotLockStorage extends Storage {
  /** 读并持锁(读③跳②;排队 FIFO 超时抛错;同槽重入抛错;终局兜底释放) */
  getWithLock(slot: string): string | null;
  /**
   * 白放(仅释放)或同步写③+打权威标+释放(排队者即刻见新值;锁定槽唯一写通道)
   * 写超时 5*1000ms 独立于执行取消;失败 = 抛错 + 锁必释放;未持锁调用抛错
   * 权威标记永不清除:经锁通道落值的槽不可再回退普通 set 模式(缓冲会被归并丢弃)
   */
  unlock(slot: string, value?: string): void;
}

/** handler 主函数调用上下文(ctx 为调用参数,不进全局——跨请求泄漏构造性不可能) */
export interface HandlerContext {
  request: HandlerRequestInfo;
  /** 合并参数(上游预设 ⊕ 包级默认),与 protocol 路径同源;自由键面(any:键集合由上游参数决定,无法静态枚举) */
  params: Record<string, any>;
  /** 当前注入键(只读;handler 零键池写,动态凭据走 storage 槽) */
  key: KeyRef;
  /** 执行级数据袋(随意读写;初始值 = prepare 返回值;终局丢弃) */
  data: unknown;
  http: HandlerHttp;
  reply: HandlerReply;
  storage: SlotLockStorage;
  settings: HandlerSettings;
}

/**
 * login/index.js 导出(login/ 目录:index.js 与 ui/index.html 绑定,缺一拒装)
 * 登录引导:GUI 经 aap.login 调用(返回值由宿主 Keys.Set 落库,登录路径唯一落库通道);
 * handler 引导时 require 后传裁剪对象 {params, http, storage(仅 get/set/delete), util},返回值作者自理(标准姿势 unlock 落槽)
 */
export type LoginModule = (ctx: LoginContext) => unknown;

/** login 调用上下文(无锁原语、无 reply;http 两调用点统一 request 形态:handler 路径 = ctx.http 透传,GUI 路径 = 宿主适配提供——登录是缓冲出站,不涉流) */
export interface LoginContext {
  /** 调用方提供:GUI 桥表单 payload / handler 合成 */
  params: Record<string, unknown>;
  /** 出站统一 request 形态:handler 路径透传 ctx.http,GUI 路径宿主适配提供;响应 body 无截断(两调用点形态一致;登录非流式,不涉 open) */
  http: { request(req: HandlerHttpRequest): HandlerHttpResponse }; // 响应 body 上限 1MB 截断
  /** 裁剪形态:仅 get/set/delete(handler 路径剥离锁成员,与 GUI 路径一致) */
  storage: Storage;
  util: Util;
}

/** inspect.js 导出(可选,独立;GUI 展示账号时的变换/脱敏;纯函数) */
export type InspectModule = (data: unknown) => unknown;

/** 包级键:每键独立实体(id+data+prev+updatedAt);protocol/filter 经 util.key() 只读当前注入键 */
declare global {
  const util: Util;
  const log: Log;
  const storage: Storage;
  /** settings 声明构建器(族文件求值时注入) */
  const setting: SettingBuilders;
  /** CommonJS 模块面(包内相对路径;扩展名可选/目录索引;同 runtime 幂等单例);exports 为 any——部件可自由挂函数/prepare 声明 */
  function require(spec: string): unknown;
  var module: { exports: any };
  var exports: any;
}

export {}