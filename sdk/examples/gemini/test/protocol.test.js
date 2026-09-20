// gemini 包单测:mock 回放(Gemini GenerateContent 协议样例);node --test
const test = require("node:test");
const assert = require("node:assert");
const { loadPackage, mockCtx } = require("../../../test.js");

const pkg = (config) => loadPackage(__dirname + "/..", { config: config, secrets: SECRETS });

const SECRETS = { api_key: "AIza-test-key" };
const BASE = { id: "t", name: "gemini", baseUrl: "https://generativelanguage.googleapis.com" };
const CTX = mockCtx({ target: BASE, vars: { model: "gemini-2.0-flash", entryStream: false } });

const CHAT_PIVOT = JSON.stringify({
  model: "gemini-2.0-flash",
  messages: [
    { role: "system", content: "Be terse." },
    { role: "user", content: "hi" },
    { role: "assistant", content: "hello" },
    { role: "user", content: [{ type: "text", text: "again" }] }
  ],
  max_tokens: 100,
  temperature: 0.5,
  top_p: 0.9,
  stream: false
});

test("buildRequest: generateContent endpoint + api key header + body translation", () => {
  const hooks = pkg();
  const req = hooks.buildRequest(CTX, CHAT_PIVOT);
  assert.equal(req.method, "POST");
  assert.equal(req.url, "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent");
  assert.equal(req.headers["x-goog-api-key"], "AIza-test-key");
  assert.equal(req.headers["Content-Type"], "application/json");
  assert.equal(req.stream, false);
  const body = JSON.parse(req.body);
  assert.deepEqual(body.systemInstruction, { parts: [{ text: "Be terse." }] });
  assert.deepEqual(body.contents, [
    { role: "user", parts: [{ text: "hi" }] },
    { role: "model", parts: [{ text: "hello" }] },
    { role: "user", parts: [{ text: "again" }] }
  ]);
  assert.deepEqual(body.generationConfig, { maxOutputTokens: 100, temperature: 0.5, topP: 0.9 });
  assert.equal(body.stream, undefined);
  assert.equal(body.model, undefined);
  assert.equal(body.messages, undefined);
});

test("buildRequest: stream endpoint via entryStream; apiVersion config", () => {
  const hooks = pkg({ apiVersion: "v1" });
  const sctx = mockCtx({ target: BASE, vars: { model: "gemini-2.0-flash", entryStream: true } });
  const req = hooks.buildRequest(
    sctx,
    JSON.stringify({ model: "gemini-2.0-flash", messages: [{ role: "user", content: "hi" }] })
  );
  assert.equal(req.url, "https://generativelanguage.googleapis.com/v1/models/gemini-2.0-flash:streamGenerateContent?alt=sse");
  assert.equal(req.stream, true);
});

test("mapEvent: text delta frame → openai chunk; empty frame → null", () => {
  const hooks = pkg();
  const frame = JSON.stringify({
    event: "message",
    data: JSON.stringify({
      responseId: "r1",
      candidates: [{ content: { parts: [{ text: "he" }], role: "model" } }]
    })
  });
  const chunk = JSON.parse(hooks.mapEvent(CTX, frame));
  assert.equal(chunk.object, "chat.completion.chunk");
  assert.equal(chunk.id, "r1");
  assert.equal(chunk.model, "gemini-2.0-flash");
  assert.deepEqual(chunk.choices[0].delta, { content: "he" });
  assert.equal(chunk.choices[0].finish_reason, null);
  assert.equal(chunk.usage, undefined);

  const empty = JSON.stringify({ event: "message", data: JSON.stringify({ candidates: [{ content: { parts: [] } }] }) });
  assert.equal(hooks.mapEvent(CTX, empty), null);

  const noId = JSON.stringify({
    event: "message",
    data: JSON.stringify({ candidates: [{ content: { parts: [{ text: "x" }] } }] })
  });
  assert.equal(JSON.parse(hooks.mapEvent(CTX, noId)).id, "test-req"); // 无 responseId 回退 ctx.requestId
});

test("mapEvent: in-stream error frame → x_error chunk", () => {
  const hooks = pkg();
  const frame = JSON.stringify({
    event: "message",
    data: JSON.stringify({ error: { code: 429, message: "quota exceeded", status: "RESOURCE_EXHAUSTED" } })
  });
  const chunk = JSON.parse(hooks.mapEvent(CTX, frame));
  assert.deepEqual(chunk, { x_error: { type: "RESOURCE_EXHAUSTED", message: "quota exceeded" } });
});

test("mapEvent: final frame finishReason + usage; finish-only frame yields empty delta", () => {
  const hooks = pkg();
  const frame = JSON.stringify({
    event: "message",
    data: JSON.stringify({
      responseId: "r2",
      candidates: [{ content: { parts: [{ text: "!" }] }, finishReason: "MAX_TOKENS" }],
      usageMetadata: { promptTokenCount: 5, candidatesTokenCount: 7, totalTokenCount: 12 }
    })
  });
  const chunk = JSON.parse(hooks.mapEvent(CTX, frame));
  assert.deepEqual(chunk.choices[0].delta, { content: "!" });
  assert.equal(chunk.choices[0].finish_reason, "length");
  assert.deepEqual(chunk.usage, { prompt_tokens: 5, completion_tokens: 7, total_tokens: 12 });

  const finOnly = JSON.stringify({
    event: "message",
    data: JSON.stringify({
      candidates: [{ finishReason: "SAFETY" }],
      usageMetadata: { promptTokenCount: 1, candidatesTokenCount: 0, totalTokenCount: 1 }
    })
  });
  const fin = JSON.parse(hooks.mapEvent(CTX, finOnly));
  assert.deepEqual(fin.choices[0].delta, {});
  assert.equal(fin.choices[0].finish_reason, "content_filter");
  assert.deepEqual(fin.usage, { prompt_tokens: 1, completion_tokens: 0, total_tokens: 1 });
});

test("usage: 候选截断时上游省略 candidatesTokenCount 且思考 token 计入 completion", () => {
  // 实测上游形态(3.x 思考模型被 maxOutputTokens 截断):
  // usageMetadata 只有 prompt/thoughts/total,无 candidatesTokenCount
  const hooks = pkg();
  const truncated = JSON.stringify({
    event: "message",
    data: JSON.stringify({
      candidates: [{ content: { parts: [{ text: "", thoughtSignature: "sig" }], role: "model" }, finishReason: "MAX_TOKENS" }],
      usageMetadata: { promptTokenCount: 3, totalTokenCount: 16, thoughtsTokenCount: 13 }
    })
  });
  const chunk = JSON.parse(hooks.mapEvent(CTX, truncated));
  assert.deepEqual(chunk.usage, { prompt_tokens: 3, completion_tokens: 13, total_tokens: 16 });
  assert.equal(chunk.usage.prompt_tokens + chunk.usage.completion_tokens, chunk.usage.total_tokens);
  assert.equal(chunk.choices[0].finish_reason, "length");
});

test("mapResponse: GenerateContentResponse → chat.completion envelope", () => {
  const hooks = pkg();
  const upstream = JSON.stringify({
    responseId: "r9",
    candidates: [{ content: { parts: [{ text: "hey there" }], role: "model" }, finishReason: "STOP" }],
    usageMetadata: { promptTokenCount: 3, candidatesTokenCount: 2, totalTokenCount: 5 },
    modelVersion: "gemini-2.0-flash"
  });
  const out = JSON.parse(hooks.mapResponse(CTX, upstream));
  assert.equal(out.object, "chat.completion");
  assert.equal(out.id, "r9");
  assert.equal(out.model, "gemini-2.0-flash");
  assert.equal(typeof out.created, "number");
  assert.deepEqual(out.choices, [
    { index: 0, message: { role: "assistant", content: "hey there" }, finish_reason: "stop" }
  ]);
  assert.deepEqual(out.usage, { prompt_tokens: 3, completion_tokens: 2, total_tokens: 5 });

  const rec = JSON.parse(
    hooks.mapResponse(CTX, JSON.stringify({ candidates: [{ finishReason: "RECITATION" }] }))
  );
  assert.equal(rec.choices[0].finish_reason, "content_filter");

  const other = JSON.parse(hooks.mapResponse(CTX, JSON.stringify({ promptFeedback: { blockReason: "OTHER" } })));
  assert.equal(other.choices[0].finish_reason, "content_filter");
});

test("mapError: 上游错误原样反映(type/status/details 不做翻译)", () => {
  const hooks = pkg();
  // 实测上游 429 形态:status 为上游枚举,details 带 QuotaFailure
  const quota = JSON.stringify({
    error: {
      code: 429,
      message: "You exceeded your current quota",
      status: "RESOURCE_EXHAUSTED",
      details: [{
        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
        violations: [{
          quotaMetric: "generativelanguage.googleapis.com/generate_content_free_tier_requests",
          quotaId: "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
          quotaValue: "20"
        }]
      }]
    }
  });
  const err = JSON.parse(hooks.mapError(CTX, 429, quota));
  assert.equal(err.error.type, "RESOURCE_EXHAUSTED");
  assert.equal(err.error.status, 429);
  assert.equal(err.error.message, "You exceeded your current quota");
  assert.deepEqual(err.error.details, JSON.parse(quota).error.details);

  // 4xx/5xx 一律原样,不按状态码改写 type
  const unauthorized = JSON.parse(hooks.mapError(CTX, 401, '{"error":{"code":401,"message":"bad key","status":"UNAUTHENTICATED"}}'));
  assert.equal(unauthorized.error.type, "UNAUTHENTICATED");
  const notFound = JSON.parse(hooks.mapError(CTX, 404, '{"error":{"code":404,"message":"no model","status":"NOT_FOUND"}}'));
  assert.equal(notFound.error.type, "NOT_FOUND");
  const broken = JSON.parse(hooks.mapError(CTX, 500, '{"error":{"code":500,"message":"boom"}}'));
  assert.equal(broken.error.type, "500");
  assert.equal(broken.error.details, undefined);

  // 上游无数字 code 时回落 HTTP 状态
  const noCode = JSON.parse(hooks.mapError(CTX, 403, '{"error":{"message":"denied"}}'));
  assert.equal(noCode.error.status, 403);
  assert.equal(noCode.error.type, "403");

  // 实测:流式请求被限速时上游在 SSE 前掐断,body 为空
  const empty = JSON.parse(hooks.mapError(CTX, 429, ""));
  assert.equal(empty.error.type, "429");
  assert.equal(empty.error.status, 429);
  assert.equal(empty.error.message, "");

  const html = JSON.parse(hooks.mapError(CTX, 502, "<html>gateway</html>"));
  assert.equal(html.error.status, 502);
  assert.equal(html.error.message, "<html>gateway</html>");
});

test("buildRequest: consecutive same-role merged; empty messages skipped; empty contents throws", () => {
  const hooks = pkg();
  const pivot = JSON.stringify({
    model: "gemini-2.0-flash",
    messages: [
      { role: "user", content: "a" },
      { role: "user", content: "b" },
      { role: "assistant", content: "" },
      { role: "assistant", content: "c" }
    ]
  });
  const body = JSON.parse(hooks.buildRequest(CTX, pivot).body);
  assert.deepEqual(body.contents, [
    { role: "user", parts: [{ text: "a" }, { text: "b" }] },
    { role: "model", parts: [{ text: "c" }] }
  ]);

  assert.throws(() =>
    hooks.buildRequest(CTX, JSON.stringify({ model: "m", messages: [{ role: "user", content: "" }] }))
  );
});

test("buildRequest: developer role as system; stop/max_completion_tokens mapped", () => {
  const hooks = pkg();
  const pivot = JSON.stringify({
    model: "gemini-2.0-flash",
    messages: [
      { role: "developer", content: "sys" },
      { role: "user", content: "hi" }
    ],
    stop: ["END", "STOP"],
    max_completion_tokens: 55,
    max_tokens: 99
  });
  const req = hooks.buildRequest(CTX, pivot);
  const body = JSON.parse(req.body);
  assert.deepEqual(body.systemInstruction, { parts: [{ text: "sys" }] });
  assert.deepEqual(body.generationConfig, { maxOutputTokens: 55, stopSequences: ["END", "STOP"] });
});

test("buildRequest: model path segment url-encoded", () => {
  const hooks = pkg();
  const ectx = mockCtx({ target: BASE, vars: { model: "a:b/c", entryStream: false } });
  const req = hooks.buildRequest(ectx, JSON.stringify({ model: "a:b/c", messages: [{ role: "user", content: "hi" }] }));
  assert.ok(req.url.includes("/models/a%3Ab%2Fc:generateContent"));
});

test("mapEvent: promptFeedback blockReason yields finish chunk regardless of value", () => {
  const hooks = pkg();
  const frame = JSON.stringify({
    event: "message",
    data: JSON.stringify({ promptFeedback: { blockReason: "OTHER" } })
  });
  const chunk = JSON.parse(hooks.mapEvent(CTX, frame));
  assert.deepEqual(chunk.choices[0].delta, {});
  assert.equal(chunk.choices[0].finish_reason, "content_filter");
});

test("mapResponse: promptFeedback-only response maps to content_filter", () => {
  const hooks = pkg();
  const out = JSON.parse(hooks.mapResponse(CTX, JSON.stringify({ promptFeedback: { blockReason: "SAFETY" } })));
  assert.equal(out.choices[0].message.content, "");
  assert.equal(out.choices[0].finish_reason, "content_filter");
});
