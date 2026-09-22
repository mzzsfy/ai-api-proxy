// gemini 协议包:Google Gemini GenerateContent 上游适配(声明槽 openai-completions;entry ↔ gemini 双向翻译)
// 协议来源:GenerateContent REST(POST {base}/{v}/models/{model}:generateContent | :streamGenerateContent?alt=sse)
// 恰一次直发,无重试(归下游);纯文本面(features:[] 由能力协商挡 tools/vision)
// v2:连接=包参数(config.base_url/api_version),密钥=包级 keys(util.key("api_key"))
"use strict";
// @ts-check
/// <reference path="../../ai-api-proxy.d.ts" />

/**
 * @param {import("../../ai-api-proxy.d.ts").PartConfig} config
 */
module.exports = function (config) {
  const cfg = config || {};
  const apiVersion = typeof cfg.api_version === "string" && cfg.api_version ? cfg.api_version : "v1beta";
  if (typeof cfg.base_url !== "string" || !cfg.base_url) throw new Error("package param base_url unset");

  // openai content(string | text 块数组)→ 单文本;非文本块丢弃(vision/tools 未声明)
  /**
   * @param {any} content
   * @returns {string}
   */
  function textOf(content) {
    if (typeof content === "string") return content;
    if (Array.isArray(content)) {
      return content
        .map(function (/** @type {{ type?: string, text?: string }} */ p) {
          return p && p.type === "text" && typeof p.text === "string" ? p.text : "";
        })
        .join("");
    }
    return "";
  }

  // finishReason → openai finish_reason(拦截/扣留系必须显式 content_filter,错映射会让客户端当正常结束)
  /**
   * @param {string|undefined} fr
   * @returns {string}
   */
  function finishReason(fr) {
    if (fr === "MAX_TOKENS") return "length";
    if (fr === "SAFETY" || fr === "BLOCKLIST" || fr === "PROHIBITED_CONTENT" || fr === "SPII" || fr === "RECITATION") {
      return "content_filter";
    }
    return "stop";
  }

  /**
   * @param {any} g
   * @returns {any}
   */
  function firstCandidate(g) {
    return Array.isArray(g.candidates) ? g.candidates[0] : undefined;
  }

  /**
   * @param {any} cand
   * @returns {string}
   */
  function partsText(cand) {
    const parts = cand && cand.content && Array.isArray(cand.content.parts) ? cand.content.parts : [];
    return parts
      .map(function (/** @type {{ text?: string }} */ p) {
        return p && typeof p.text === "string" ? p.text : "";
      })
      .join("");
  }

  /**
   * @param {any} v
   * @returns {number}
   */
  function count(v) {
    return typeof v === "number" ? v : 0;
  }

  // usageMetadata → openai usage;上游在候选被截断/为空时省略 candidatesTokenCount,
  // 缺键补零而非丢键(键缺失会让下游读到 null)
  // 思考 token(thoughtsTokenCount)计入 completion:上游 total 已含思考量,
  // 不计入则 total != prompt + completion,破坏 openai 客户端依赖的配额不变量
  /**
   * @param {any} g
   * @returns {Record<string, number>|undefined}
   */
  function usageOf(g) {
    const u = g.usageMetadata;
    if (!u) return undefined;
    return {
      prompt_tokens: count(u.promptTokenCount),
      completion_tokens: count(u.candidatesTokenCount) + count(u.thoughtsTokenCount),
      total_tokens: count(u.totalTokenCount)
    };
  }

  /**
   * @param {any} g
   * @param {string} fallback
   * @returns {string}
   */
  function respId(g, fallback) {
    return typeof g.responseId === "string" && g.responseId ? g.responseId : fallback;
  }

  return {
    /**
     * 构造上游请求:x-goog-api-key 头(key 不进 URL);system 消息提为 systemInstruction
     * @param {import("../../ai-api-proxy.d.ts").Context} ctx
     * @param {string} entry
     * @returns {import("../../ai-api-proxy.d.ts").UpstreamRequest}
     */
    buildRequest: function (ctx, entry) {
      const req = JSON.parse(entry); // 入口非法原文抛错 → host 502 注明部件原因
      /** @type {{ role: string, parts: { text: string }[]}[]} */
      const contents = [];
      /** @type {string[]} */
      const sys = [];
      const msgs = Array.isArray(req.messages) ? req.messages : [];
      for (const m of msgs) {
        if (!m) continue;
        // developer 为 openai 现行 system 等价 role,同归 systemInstruction
        if (m.role === "system" || m.role === "developer") {
          const st = textOf(m.content);
          if (st) sys.push(st);
          continue;
        }
        const t = textOf(m.content);
        if (!t) continue; // 空文本消息跳过(上游拒空 part)
        const role = m.role === "assistant" ? "model" : "user";
        const last = contents[contents.length - 1];
        if (last && last.role === role) last.parts.push({ text: t }); // 上游强制 user/model 交替,连续同角色合并
        else contents.push({ role: role, parts: [{ text: t }] });
      }
      if (!contents.length) throw new Error("no translatable contents"); // host 502 优于上游裸 400
      /** @type {Record<string, any>} */
      const gen = {};
      const mt = typeof req.max_completion_tokens === "number" ? req.max_completion_tokens : req.max_tokens;
      if (typeof mt === "number") gen.maxOutputTokens = mt;
      if (typeof req.temperature === "number") gen.temperature = req.temperature;
      if (typeof req.top_p === "number") gen.topP = req.top_p;
      if (typeof req.stop === "string" && req.stop) gen.stopSequences = [req.stop];
      else if (Array.isArray(req.stop) && req.stop.length) gen.stopSequences = req.stop.map(String);
      /** @type {Record<string, any>} */
      const body = { contents: contents };
      if (sys.length) body.systemInstruction = { parts: sys.map(function (t) { return { text: t }; }) };
      if (Object.keys(gen).length) body.generationConfig = gen;
      const stream = ctx.vars.entryStream === true;
      const action = stream ? ":streamGenerateContent?alt=sse" : ":generateContent";
      return {
        url: cfg.base_url.replace(/\/+$/, "") + "/" + encodeURIComponent(apiVersion) + "/models/" +
          encodeURIComponent(ctx.vars.model) + action,
        method: "POST",
        headers: { "Content-Type": "application/json", "x-goog-api-key": util.key("api_key") },
        body: JSON.stringify(body),
        stream: stream
      };
    },

    /**
     * SSE 帧 → 声明协议事件对象数组:GenerateContentResponse 增量解包;无文本/终止/用量则跳帧
     * @param {import("../../ai-api-proxy.d.ts").Context} ctx
     * @param {string} event
     * @returns {string | null}
     */
    mapEvent: function (ctx, event) {
      const g = JSON.parse(JSON.parse(event).data);
      const cand = firstCandidate(g);
      const text = partsText(cand);
      // prompt 级拦截:200 + 空 candidates + promptFeedback.blockReason;拦截信号是存在性,与取值无关
      const block = g.promptFeedback && g.promptFeedback.blockReason;
      const fr = block ? "content_filter" : cand && cand.finishReason ? finishReason(cand.finishReason) : null;
      const usage = usageOf(g);
      if (!text && !fr && !usage) return null;
      // 声明协议(openai-completions)chunk 语义
      /** @type {Record<string, any>} */
      const chunk = {
        id: respId(g, ctx.requestId),
        object: "chat.completion.chunk",
        created: Math.floor(util.now() / 1000),
        model: ctx.vars.model,
        choices: [{ index: 0, delta: text ? { content: text } : {}, finish_reason: fr }]
      };
      if (usage) chunk.usage = usage;
      return JSON.stringify([chunk]);
    },

    /**
     * 非流式响应 → 声明协议 chat.completion 信封
     * @param {import("../../ai-api-proxy.d.ts").Context} ctx
     * @param {string} body
     * @returns {string}
     */
    mapResponse: function (ctx, body) {
      const g = JSON.parse(body);
      const cand = firstCandidate(g);
      // 拦截信号是 blockReason 存在性,不经 finishReason 值映射(OTHER 等取值同样拦截)
      const block = g.promptFeedback && g.promptFeedback.blockReason;
      /** @type {Record<string, any>} */
      const out = {
        id: respId(g, util.uuid()),
        object: "chat.completion",
        created: Math.floor(util.now() / 1000),
        model: ctx.vars.model,
        choices: [
          {
            index: 0,
            message: { role: "assistant", content: partsText(cand) },
            finish_reason: block ? "content_filter" : finishReason(cand && cand.finishReason)
          }
        ]
      };
      const usage = usageOf(g);
      if (usage) out.usage = usage;
      return JSON.stringify(out);
    },

    /**
     * 错误终局映射:声明协议(openai-completions)错误信封形态;
     * 上游错误原样反映:type/message 取上游 status 与 message,status 取上游 code 或 HTTP 状态,
     * details 原样搬运(不重命名、不挑选、不做语义翻译)
     * @param {import("../../ai-api-proxy.d.ts").Context} ctx
     * @param {number} status
     * @param {string} body
     * @returns {string}
     */
    mapError: function (ctx, status, body) {
      // 限速/配额拒绝常在 SSE 开始前掐断,此时 body 为空:空体无法承载上游信息,
      // 至少给出 HTTP 状态与 type,避免下游拿到空错误体无从判断
      let parsed;
      try { parsed = JSON.parse(body); } catch (e) { parsed = undefined; }
      if (!parsed || !parsed.error || typeof parsed.error !== "object") {
        return JSON.stringify({
          error: {
            type: String(status),
            message: typeof body === "string" ? body : "",
            status: status
          }
        });
      }
      const e = parsed.error;
      const code = typeof e.code === "number" ? e.code : status;
      /** @type {{ error: { type: string, message: string, status: number, details?: any } }} */
      const out = {
        error: {
          type: typeof e.status === "string" && e.status ? e.status : String(code),
          message: typeof e.message === "string" ? e.message : "",
          status: code
        }
      };
      if (e.details !== undefined) {
        out.error.details = e.details;
      }
      return JSON.stringify(out);
    }
  };
};

// 包参数声明(v2 settings 片段):连接与版本槽;密钥 api_key 走包级 keys 表单
module.exports.settings = {
  base_url: setting.string({ description: "Gemini API 地址", required: true }),
  api_version: setting.string({ description: "API 版本路径段(默认 v1beta;写死单版本会随上游弃用失效)" })
};
