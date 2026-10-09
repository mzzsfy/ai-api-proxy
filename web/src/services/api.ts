import { get, post, postRaw, put, del } from './http'
import type {
  ChatTestResult,
  DiscoveryResult,
  DiscoveryRule,
  GroupMember,
  HistoryEntry,
  HistoryPage,
  InspectSummary,
  AccountsView,
  KeysView,
  MetaView,
  MetricsLive,
  MetricsSeriesRow,
  ModelGroup,
  PackageInfo,
  PackageSettings,
  Subscription,
  SubscriptionCreate,
  TaskResultRow,
  TestResult,
  Transport,
} from './types'

// 二进制下载需要浏览器原生处理 attachment, 不走 JSON 封装
export const packageExportUrl = (name: string) => `/admin/api/packages/${encodeURIComponent(name)}/export`
export const packageTemplateUrl = '/packages/template'

export const api = {
  groups: {
    list: () => get<ModelGroup[]>('/groups'),
    create: (group: Omit<ModelGroup, 'id'>) => post<{ ok: true; id: number }>('/groups', group),
    update: (id: number, group: Omit<ModelGroup, 'id'> & { id: number }) =>
      put<{ ok: true; id: number }>(`/groups/${id}`, group),
    remove: (id: number) => del(`/groups/${id}`),
    test: (id: number) => post<TestResult>(`/groups/${id}/test`),
    chatTest: (id: number, message: string) =>
      post<ChatTestResult>(`/groups/${id}/chat-test`, { message }),
    members: (id: number) => get<GroupMember[]>(`/groups/${id}/members`),
    createMember: (groupId: number, member: { name: string; enabled: boolean }) =>
      post<{ ok: true; id: number }>(`/groups/${groupId}/members`, member),
    updateMember: (id: number, member: { group_id: number; name: string; enabled: boolean }) =>
      put<{ ok: true; id: number }>(`/members/${id}`, member),
    removeMember: (id: number) => del(`/members/${id}`),
    discover: (id: number, rules: DiscoveryRule[], enabled: boolean, apply: boolean) =>
      post<DiscoveryResult>(`/groups/${id}/discover`, { rules, enabled, apply }),
  },

  packages: {
    list: () => get<PackageInfo[]>('/packages'),
    install: (bytes: ArrayBuffer) =>
      postRaw<{ ok: true }>('/packages', bytes),
    inspectBytes: (bytes: ArrayBuffer) => postRaw<InspectSummary>('/packages/inspect', bytes),
    inspectUrl: (url: string) => post<InspectSummary>('/packages/inspect', { url }),
    importUrl: (url: string) => post<{ ok: true }>('/packages/import-url', { url }),
    remove: (name: string) => del(`/packages/${encodeURIComponent(name)}`),
    enable: (name: string, enabled: boolean) =>
      post<{ ok: true }>(`/packages/${encodeURIComponent(name)}/enable`, { enabled }),
    settings: (name: string) => get<PackageSettings>(`/packages/${encodeURIComponent(name)}/settings`),
    saveSettings: (
      name: string,
      body: { config: Record<string, unknown>; tasks: Record<string, { cron?: string; disabled?: boolean }>; version: number },
    ) => put<{ ok: true; version: number }>(`/packages/${encodeURIComponent(name)}/settings`, body),
    runTask: (name: string, task: string, key = '') =>
      post<{ ok: true; results: TaskResultRow[] }>(
        `/packages/${encodeURIComponent(name)}/tasks/${encodeURIComponent(task)}/run${key ? `?key=${encodeURIComponent(key)}` : ''}`,
      ),
  },

  keys: {
    list: (name: string) => get<KeysView>(`/packages/${encodeURIComponent(name)}/keys`),
    write: (name: string, key: string, value: unknown, baseUpdatedAt: number) =>
      put<{ updatedAt: number }>(
        `/packages/${encodeURIComponent(name)}/keys/${encodeURIComponent(key)}`,
        { value, baseUpdatedAt },
      ),
    remove: (name: string, key: string) =>
      del(`/packages/${encodeURIComponent(name)}/keys/${encodeURIComponent(key)}`),
  },

  creds: {
    login: (name: string, input: unknown) =>
      post<{ id: string; data: unknown }>(`/packages/${encodeURIComponent(name)}/login`, { input }),
    accounts: (name: string) => get<AccountsView>(`/packages/${encodeURIComponent(name)}/accounts`),
    storageGet: (name: string, key: string) =>
      post<{ value: unknown }>(`/packages/${encodeURIComponent(name)}/accounts/storage`, { op: 'get', key }),
    storageSet: (name: string, key: string, value: string) =>
      post<{ ok: true }>(`/packages/${encodeURIComponent(name)}/accounts/storage`, { op: 'set', key, value }),
    meta: (name: string) => get<MetaView>(`/packages/${encodeURIComponent(name)}/meta`),
    loginUIUrl: (name: string) => `/admin/api/packages/${encodeURIComponent(name)}/login/ui/`,
  },

  metrics: {
    live: () => get<MetricsLive>('/metrics/live'),
    series: (minutes: number) => get<MetricsSeriesRow[]>(`/metrics/series?minutes=${minutes}`),
  },

  history: {
    query: (params: URLSearchParams) => get<HistoryPage>(`/history?${params.toString()}`),
    detail: (id: number) => get<HistoryEntry>(`/history/${id}`),
  },

  transports: {
    list: () => get<Transport[]>('/transports'),
    test: (name: string) => post<TestResult>(`/transports/${encodeURIComponent(name)}/test`),
    evict: (name: string, scope: 'lease' | 'egress', value: string) =>
      post<{ ok: true }>(`/transports/${encodeURIComponent(name)}/evict`, { scope, value }),
  },

  subscriptions: {
    list: () => get<Subscription[]>('/subscriptions'),
    create: (sub: SubscriptionCreate) => post<{ ok: true; id: number }>('/subscriptions', sub),
    remove: (id: number) => del(`/subscriptions/${id}`),
    sync: (id: number) => post<{ ok: true }>(`/subscriptions/${id}/sync`),
  },
}
