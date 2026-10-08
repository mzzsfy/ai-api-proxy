<script setup lang="ts">
import { computed, h, onMounted, ref } from 'vue'
import { NButton, NCheckbox, NCard, NDataTable, NDatePicker, NIcon, NInput, NPagination, NSelect, NTag, useMessage, type DataTableColumns } from 'naive-ui'
import { SearchOutline } from '@vicons/ionicons5'
import { api } from '@/services/api'
import type { HistoryEntry } from '@/services/types'

const PAGE_SIZE = 50

type PresetKey = 'all' | '30m' | '1h' | '6h' | '1d' | '7d' | '30d' | 'custom'

const PRESETS: Array<{ label: string; value: PresetKey; minutes?: number }> = [
  { label: '全部', value: 'all' },
  { label: '30 分钟', value: '30m', minutes: 30 },
  { label: '1 小时', value: '1h', minutes: 60 },
  { label: '6 小时', value: '6h', minutes: 6 * 60 },
  { label: '1 天', value: '1d', minutes: 24 * 60 },
  { label: '7 天', value: '7d', minutes: 7 * 24 * 60 },
  { label: '30 天', value: '30d', minutes: 30 * 24 * 60 },
  { label: '自定义', value: 'custom' },
]

const message = useMessage()
const preset = ref<PresetKey>('all')
const range = ref<[number, number] | null>(null)
const modelFilter = ref('')
const upstreamFilter = ref('')
const errorsOnly = ref(false)
const page = ref(1)
const items = ref<HistoryEntry[]>([])
const total = ref(0)
const loading = ref(false)

const detail = ref<HistoryEntry | null>(null)
const detailLoading = ref(false)

const presetOptions = PRESETS.map(({ label, value }) => ({ label, value }))

function epochFilter(): { start?: number; end?: number } {
  if (preset.value === 'custom') {
    if (!range.value) return {}
    return { start: range.value[0], end: range.value[1] }
  }
  const selected = PRESETS.find((p) => p.value === preset.value)
  if (!selected?.minutes) return {}
  const now = Date.now()
  return { start: now - selected.minutes * 60 * 1000, end: now }
}

async function query(targetPage = 1) {
  loading.value = true
  try {
    const time = epochFilter()
    const params = new URLSearchParams({
      limit: String(PAGE_SIZE),
      offset: String((targetPage - 1) * PAGE_SIZE),
    })
    if (modelFilter.value) params.set('model', modelFilter.value)
    if (upstreamFilter.value) params.set('upstream', upstreamFilter.value)
    if (errorsOnly.value) params.set('error', '1')
    if (time.start !== undefined) params.set('start', String(time.start))
    if (time.end !== undefined) params.set('end', String(time.end))
    const result = await api.history.query(params)
    items.value = result.items
    total.value = result.total
    page.value = targetPage
  } catch (err) {
    message.error(err instanceof Error ? err.message : '查询失败')
  } finally {
    loading.value = false
  }
}

async function openDetail(row: HistoryEntry) {
  detail.value = row
  detailLoading.value = true
  try {
    detail.value = await api.history.detail(row.id)
  } catch {
    // 保留列表行数据兜底展示
  } finally {
    detailLoading.value = false
  }
}

function prettyBody(text: string | undefined): string {
  if (!text) return '(未记录)'
  try {
    return JSON.stringify(JSON.parse(text), null, 2)
  } catch {
    return text
  }
}

function formatTs(ts: number): string {
  return new Date(ts).toLocaleString('zh-CN', { hour12: false })
}

function statusType(status: number): 'error' | 'warning' | 'success' {
  if (status >= 500) return 'error'
  if (status >= 400) return 'warning'
  return 'success'
}

const columns: DataTableColumns<HistoryEntry> = [
  { title: '时间', key: 'ts', width: 165, render: (row) => formatTs(row.ts) },
  { title: '模型', key: 'model', width: 150, ellipsis: { tooltip: true } },
  { title: '上游', key: 'upstream', width: 140, ellipsis: { tooltip: true } },
  { title: '目标', key: 'target', width: 140, ellipsis: { tooltip: true } },
  {
    title: '状态',
    key: 'status',
    width: 80,
    render: (row) => h(NTag, { size: 'small', type: statusType(row.status) }, { default: () => row.status }),
  },
  { title: '耗时', key: 'duration_ms', width: 85, render: (row) => `${row.duration_ms}ms` },
  {
    title: '路径',
    key: 'path',
    ellipsis: { tooltip: true },
    render: (row) =>
      h('span', null, [
        row.path,
        row.stream ? h(NTag, { size: 'tiny', style: 'margin-left:6px' }, { default: () => '流式' }) : null,
      ]),
  },
]

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / PAGE_SIZE)))

onMounted(() => query(1))
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>请求历史</h1>
        <p class="subtitle">共 {{ total }} 条</p>
      </div>
    </div>

    <div class="filter-bar">
      <NSelect v-model:value="preset" :options="presetOptions" class="preset-select" />
      <NDatePicker
        v-if="preset === 'custom'"
        v-model:value="range"
        type="datetimerange"
        clearable
      />
      <NInput v-model:value="modelFilter" placeholder="模型" clearable class="filter-input" />
      <NInput v-model:value="upstreamFilter" placeholder="上游" clearable class="filter-input" />
      <NCheckbox v-model:checked="errorsOnly">仅错误</NCheckbox>
      <NButton type="primary" :loading="loading" @click="query(1)">
        <template #icon><NIcon :component="SearchOutline" /></template>
        查询
      </NButton>
    </div>

    <NDataTable
      :columns="columns"
      :data="items"
      :loading="loading"
      :row-props="(row) => ({ style: 'cursor: pointer', onClick: () => openDetail(row) })"
      size="small"
    />

    <div class="pagination">
      <NPagination
        :page="page"
        :page-count="totalPages"
        @update:page="(p: number) => query(p)"
      />
    </div>

    <NCard v-if="detail" title="请求详情" class="section" closable @close="detail = null">
      <template #header-extra>
        <span class="mono">#{{ detail.id }} · {{ formatTs(detail.ts) }}</span>
      </template>
      <div class="detail-meta mono">
        {{ detail.method }} {{ detail.path }} · {{ detail.model }} → {{ detail.upstream }}/{{ detail.target }} · {{ detail.status }} · {{ detail.duration_ms }}ms
        <NTag v-if="detail.stream" size="small" style="margin-left:8px">流式(响应体不记录)</NTag>
      </div>
      <h4>请求体</h4>
      <pre class="body-pre mono">{{ prettyBody(detail.request_body) }}</pre>
      <h4>响应体</h4>
      <pre class="body-pre mono">{{ prettyBody(detail.response_body) }}</pre>
    </NCard>
  </div>
</template>

<style scoped>
.filter-bar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 14px;
}

.preset-select {
  width: 120px;
}

.filter-input {
  width: 180px;
}

.pagination {
  display: flex;
  justify-content: center;
  margin-top: 14px;
}

.section {
  margin-top: 16px;
}

.detail-meta {
  font-size: 12px;
  opacity: 0.75;
}

.body-pre {
  padding: 12px;
  border-radius: 8px;
  background: rgba(128, 128, 128, 0.12);
  max-height: 320px;
  overflow: auto;
  white-space: pre-wrap;
  word-break: break-all;
}
</style>
