// ai-api-proxy 插件 SDK 类型定义(hook 全同步;代理层恰一次上游请求,无重试——归下游)
// 部件导出形态:hooks 对象(无参)或 factory 函数 (config) => hooks
// 全局注入:util / log / storage / module / exports;无 require(禁 npm)

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
  body: string;
  stream: boolean;
}

/** 流式单帧(host 分帧;入方向 {"event","data"} 形态) */
export interface SSEFrame {
  event: string;
  data: string;
}

/** 协议适配部件(恰一;主包必须含) */
export interface ProtocolHooks {
  /** 构造上游请求(同步;stream 标志承载双声明分发意图) */
  buildRequest(ctx: Context, pivot: string): UpstreamRequest;
  /** 流式帧 → pivot chunk JSON 字符串;返回 null 跳帧 */
  mapEvent?(ctx: Context, event: string): string | null;
  /** 非流式响应体 → pivot JSON 字符串 */
  mapResponse?(ctx: Context, body: string): string;
  /** 错误终局映射(可选;一次;未实现原样透传) */
  mapError?(ctx: Context, status: number, body: string): string;
}

/** 请求修改部件(0..n 串行) */
export interface FilterHooks {
  /** 请求改写(pivot → pivot);失败 = 请求 502 带部件名 */
  mapRequest(ctx: Context, pivot: string): string | null;
  /** 流式 chunk 改写(逆序);失败 = 单 chunk 降级 raw */
  mapChunk?(ctx: Context, chunk: string): string | null;
  /** 非流式响应改写(逆序);失败 = 502 */
  mapResponse?(ctx: Context, resp: string): string | null;
}

/** 部件 configSchema 实例值(factory 闭包捕获,不经 ctx) */
export type PartConfig = Record<string, unknown>;

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

// 宿主注入全局(CommonJS 部件经 reference 引用本文件时可用)
declare global {
  const util: Util;
  const log: Log;
  const storage: Storage;
}

export {}
