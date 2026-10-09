// 对齐 internal/admin 契约(docs/bootstrap-vue-web/api-contract.md)

export type UpstreamCounters = {
  requests: number
  errors: number
  max_concurrent: number
}

export type MetricsLive = {
  active_concurrent: number
  total_requests: number
  total_errors: number
  by_upstream: Record<string, UpstreamCounters>
  by_target: Record<string, UpstreamCounters>
  by_package: Record<string, UpstreamCounters>
}

export type MetricsSeriesRow = {
  minute: string
  requests: number
  errors: number
  max_concurrent: number
  by_upstream: Record<string, UpstreamCounters> | null
  by_target: Record<string, UpstreamCounters> | null
  by_package: Record<string, UpstreamCounters> | null
}

export type ModelGroup = {
  id: number
  prefix: string
  plugin: string
  params: Record<string, unknown>
  enabled: boolean
}

export type GroupMember = {
  id: number
  group_id: number
  name: string
  enabled: boolean
}

export type DiscoveryRule = {
  op: 'only' | 'exclude' | 'rename' | 'limit'
  regex?: string
  from?: string
  to?: string
  limit?: number
}

export type DiscoveryHit = {
  upstream_name: string
  member_name: string
  action: 'create' | 'skip_exists'
  poll_groups: number
}

export type DiscoveryResult = {
  fetched_total: number
  models: DiscoveryHit[]
  applied?: { created: number; skipped: number }
}

export type TestResult = {
  ok: boolean
  latency_ms: number
  error?: string
}

export type ChatTestResult = TestResult & {
  status?: number
  body?: unknown
}

export type PackageInfo = {
  name: string
  version: string
  revision: number
  hasProtocol: boolean
  filters: string[]
  protocol: string
  hasHooks: boolean
  hasPage: boolean
  enabled: boolean
  title: string
  description: string
  author: string
  homepage: string
  license: string
}

export type InspectSummary = {
  name: string
  title: string
  description: string
  version: string
  filters: string[]
  protocol?: string
  features?: string[]
  hooks?: { tasks: { name: string; cron: string; next: string }[] }
  exists: boolean
  installedVersion?: string
}

export type TaskResultRow = {
  key: string
  ok: boolean
  error?: string
  duration_ms: number
}

export type SettingsSchema = {
  type?: string
  description?: string
  default?: unknown
  values?: string[]
  required?: boolean
}

export type SettingsSlot = {
  name: string
  schema: SettingsSchema
  value: unknown
  overridden: boolean
}

export type SettingsTask = {
  name: string
  cron: string
  next: string
  timeoutMs: number
  overridden: boolean
  enabled: boolean
}

export type PackageSettings = {
  config: SettingsSlot[]
  tasks: SettingsTask[] | null
  version: number
  revision: number
}

export type KeyRow = {
  id: string
  data: unknown
  updatedAt: number
}

export type KeysView = {
  keys: KeyRow[]
  rotation: number
  login: boolean
}

export type AccountRow = {
  id: string
  data: unknown
  updatedAt: number
}

export type AccountsView = {
  accounts: AccountRow[]
}

export type MetaView = {
  name: string
  version: string
  revision: number
  params: Record<string, unknown>
}

export type HistoryEntry = {
  id: number
  ts: number
  method: string
  path: string
  model: string
  upstream: string
  target: string
  status: number
  duration_ms: number
  stream: boolean
  request_body?: string
  response_body?: string
}

export type HistoryPage = { items: HistoryEntry[]; total: number }

export type Transport = { name: string; url: string }

export type Subscription = {
  id: number
  name: string
  repo_url: string
  user: string
  branch: string
  path: string
  token_set: boolean
  package_count: number
  last_synced_commit: string
  last_synced_at: string
  last_error: string
}

export type SubscriptionCreate = {
  name: string
  repo_url: string
  user?: string
  path?: string
  token?: string
}
