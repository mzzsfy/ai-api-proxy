// anthropic-messages 协议包:入口 Anthropic Messages 原文透传(声明槽 anthropic-messages)
// 管道权威格式 = 声明协议格式;入口与上游同为 Anthropic Messages,字节 1:1,无中间格式
// v2:连接=包参数(config.base_url/api_version),密钥=包级 keys(util.key("api_key"));模型行只声明模型名
// 恰一次直发,无重试(归调用方)
"use strict";
// @ts-check
/// <reference path="../../sdk/ai-api-proxy.d.ts" />

const DEFAULT_API_VERSION = "2023-06-01";
const KEY_API_TOKEN = "api_key";

/**
 * @param {import("../../sdk/ai-api-proxy.d.ts").PartConfig} config
 */
module.exports = function (config) {
  const cfg = config || {};
  const apiVersion =
    typeof cfg.api_version === "string" && cfg.api_version ? cfg.api_version : DEFAULT_API_VERSION;

  // 包级 key 读取(键缺失抛错 → host 502 注明部件原因;值须为非空字符串)
  /**
   * @param {string} name
   * @returns {string}
   */
  function keyString(name) {
    const v = util.key(name);
    if (typeof v !== "string" || !v) throw new Error(`package key "${name}" empty or missing`);
    return v;
  }

  return {
    /**
     * @param {import("../../sdk/ai-api-proxy.d.ts").Context} ctx
     * @param {string} entry
     * @returns {import("../../sdk/ai-api-proxy.d.ts").UpstreamRequest}
     */
    buildRequest: function (ctx, entry) {
      if (typeof cfg.base_url !== "string" || !cfg.base_url) throw new Error("package param base_url unset");
      JSON.parse(entry); // 入口非法原文抛错 → host 502 注明部件原因
      return {
        url: cfg.base_url.replace(/\/+$/, "") + "/v1/messages",
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": keyString(KEY_API_TOKEN),
          "anthropic-version": apiVersion,
        },
        body: entry,
        stream: ctx.vars.entryStream,
      };
    },

    /**
     * @param {import("../../sdk/ai-api-proxy.d.ts").Context} ctx
     * @param {string} event
     * @returns {string|null}
     */
    mapEvent: function (ctx, event) {
      var frame = JSON.parse(event); // {"event","data"} 信封
      if (typeof frame.data !== "string" || !frame.data) return null;
      return JSON.stringify([JSON.parse(frame.data)]);
    },

    /**
     * @param {import("../../sdk/ai-api-proxy.d.ts").Context} ctx
     * @param {string} body
     * @returns {string}
     */
    mapResponse: function (ctx, body) {
      return body;
    },
  };
};

// 包参数声明(v2 settings 片段);密钥 api_key 走包级 keys(插件配置页)
module.exports.settings = {
  base_url: setting.string({ description: "上游地址(如 https://api.anthropic.com 或兼容网关)", required: true }),
  api_version: setting.string({ description: "anthropic-version 请求头(默认 2023-06-01)" })
};
