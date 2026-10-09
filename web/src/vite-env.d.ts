/// <reference types="vite/client" />

// .vue SFC 模块声明(vue-tsc 原生理解 SFC;此 shim 供 tsc/容器环境类型解析兜底)
declare module "*.vue" {
  import type { DefineComponent } from "vue";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const component: DefineComponent<Record<string, never>, Record<string, never>, any>;
  export default component;
}
