// ai-api-proxy 插件开发本地测试宿主
// 用法:在包 test/*.test.js 中 require 本文件,获得与 Go host 一致的注入面
// 行为基准 = internal/plugin/runtime.go(bindUtil/bindLog/bindStorage)
"use strict";

const crypto = require("node:crypto");

// ─── util 全集(与 host 绑定语义一致) ───

const isPlainObject = (v) => typeof v === "object" && v !== null && !Array.isArray(v);

function deepMerge(a, b) {
  const out = { ...a };
  for (const k of Object.keys(b)) {
    const bv = b[k];
    const av = out[k];
    if (isPlainObject(bv) && isPlainObject(av)) out[k] = deepMerge(av, bv);
    else out[k] = bv;
  }
  return out;
}

function deepClone(v) { return v === undefined ? v : JSON.parse(JSON.stringify(v)); }

function get(obj, path) {
  return String(path).split(".").reduce((a, k) => (a == null ? a : a[k]), obj);
}

function set(obj, path, v) {
  const keys = String(path).split(".");
  let cur = obj;
  for (let i = 0; i < keys.length - 1; i++) {
    if (typeof cur[keys[i]] !== "object" || cur[keys[i]] === null) cur[keys[i]] = {};
    cur = cur[keys[i]];
  }
  cur[keys[keys.length - 1]] = v;
  return obj;
}

function pick(obj, keys) {
  const out = {};
  for (const k of keys) if (k in obj) out[k] = obj[k];
  return out;
}

function omit(obj, keys) {
  const out = { ...obj };
  for (const k of keys) delete out[k];
  return out;
}

const b64encode = (s) => Buffer.from(s, "utf8").toString("base64");
const b64decode = (s) => Buffer.from(s, "base64").toString("utf8");
const b64urlEncode = (s) => Buffer.from(s, "utf8").toString("base64url");
const b64urlDecode = (s) => Buffer.from(s, "base64url").toString("utf8");
const sha256hex = (s) => crypto.createHash("sha256").update(s, "utf8").digest("hex");
const hmacSha256hex = (key, s) => crypto.createHmac("sha256", key).update(s, "utf8").digest("hex");
const uuid = () => crypto.randomUUID();
const now = () => Date.now();
const isoNow = () => new Date().toISOString();

function template(s, vars) {
  return String(s).replace(/\{(\w+)\}/g, (_, k) => (k in vars ? String(vars[k]) : `{${k}}`));
}

// inspect:脱敏(secret 字段掩码)+ 长截断(与 host inspect 语义一致)
function inspect(obj) {
  let s = JSON.stringify(obj, (k, v) => (/secret|token|key|password/i.test(k) ? "***" : v));
  const maxLen = 400;
  if (s.length > maxLen) s = s.slice(0, maxLen) + `…(+${s.length - maxLen})`;
  return s;
}

// secret:由测试用例提供目标凭据(缺省抛错,与 host 缺键行为一致)
let secretStore = {};

// ─── log / storage mock ───

const log = {
  records: [],
  info(...args) { this.records.push(["info", ...args]); },
  warn(...args) { this.records.push(["warn", ...args]); },
  error(...args) { this.records.push(["error", ...args]); },
};

function makeStorage(ns) {
  const full = (k) => `test/${ns}/${k}`;
  return {
    get: (k) => (full(k) in storageData ? storageData[full(k)] : null),
    set: (k, v) => { storageData[full(k)] = String(v); },
    delete: (k) => { delete storageData[full(k)]; },
  };
}
const storageData = {};
const storage = makeStorage("default");

// ─── 部件装载(与 host CommonJS 包装一致) ───

// loadPart 装载部件源码;hooks=对象(无参)或 factory(config)
// secrets:可选 {api_key: "..."} 注入 util.secret
function loadPart(src, config, secrets) {
  secretStore = secrets || {};
  const mod = { exports: {} };
  const fn = new Function("module", "exports", "util", "log", "storage", src);
  fn(mod, mod.exports, util, log, storage);
  const exported = mod.exports;
  return typeof exported === "function" ? exported(config || {}) : exported;
}

const util = {
  deepMerge, deepClone, get, set, pick, omit,
  b64encode, b64decode, b64urlEncode, b64urlDecode,
  sha256hex, hmacSha256hex, uuid, now, isoNow, template, inspect,
  secret: (ref) => {
    if (!(ref in secretStore)) throw new Error(`secret not found: ${ref}`);
    return secretStore[ref];
  },
};

// mockCtx 请求上下文(host PipelineContext 子集)
function mockCtx(over) {
  return Object.assign({
    requestId: "test-req",
    upstream: { name: "test-upstream", models: ["m"] },
    target: { id: "t", name: "t1", baseUrl: "https://upstream.test" },
    state: {},
    vars: { model: "m", entryStream: false },
  }, over);
}

module.exports = { loadPart, mockCtx, util, log, storage, makeStorage };

// ─── 自测(node --test sdk/test.js) ───
if (require.main === module) {
  const test = require("node:test");
  const assert = require("node:assert");

  test("util util set matches host semantics", () => {
    assert.equal(b64encode("hi"), "aGk=");
    assert.equal(b64decode("aGk="), "hi");
    assert.equal(b64urlEncode("?&="), "PyY9");
    assert.equal(b64urlDecode("PyY9"), "?&=");
    assert.equal(sha256hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    assert.equal(hmacSha256hex("k", "data"), hmacSha256hex("k", "data"));
    assert.match(uuid(), /^[0-9a-f-]{36}$/);
    assert.ok(Number.isInteger(now()));
    assert.ok(!Number.isNaN(Date.parse(isoNow())));
  });

  test("deepMerge/get/set/pick/omit/template", () => {
    assert.deepEqual(deepMerge({ a: { x: 1 } }, { a: { y: 2 } }), { a: { x: 1, y: 2 } });
    assert.deepEqual(deepClone({ a: 1 }), { a: 1 });
    assert.deepEqual(get({ a: { b: 7 } }, "a.b"), 7);
    assert.deepEqual(set({ a: {} }, "a.b", 1), { a: { b: 1 } });
    assert.deepEqual(pick({ a: 1, b: 2 }, ["a"]), { a: 1 });
    assert.deepEqual(omit({ a: 1, b: 2 }, ["b"]), { a: 1 });
    assert.equal(template("/v1/{model}", { model: "gpt" }), "/v1/gpt");
  });

  test("loadPart honors factory config and secret", () => {
    const hooks = loadPart(
      "module.exports = function (config) { return { buildRequest: function (ctx) { return { url: ctx.target.baseUrl + '/' + config.p, headers: { Authorization: 'Bearer ' + util.secret('api_key') } }; } }; };",
      { p: "v1" },
      { api_key: "sk-1" }
    );
    const req = hooks.buildRequest(mockCtx());
    assert.equal(req.url, "https://upstream.test/v1");
    assert.equal(req.headers.Authorization, "Bearer sk-1");
  });

  test("secret missing throws", () => {
    assert.throws(() =>
      loadPart(
        "module.exports = { buildRequest: function(){ util.secret('nope'); return {}; } };",
        {},
        {}
      ).buildRequest(mockCtx())
    );
  });
}
