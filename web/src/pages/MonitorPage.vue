<script setup lang="ts">
import { computed, h, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { NButton, NCard, NDataTable, NIcon, NTag, useMessage, type DataTableColumns } from 'naive-ui'
import { RefreshOutline } from '@vicons/ionicons5'
import { use } from 'echarts/core'
import { CanvasRenderer } from 'echarts/renderers'
import { GridComponent, LegendComponent, TooltipComponent } from 'echarts/components'
import { LineChart } from 'echarts/charts'
import VChart from 'vue-echarts'
import { api } from '@/services/api'
import type { MetricsSeriesRow, Transport, UpstreamCounters } from '@/services/types'

use([CanvasRenderer, GridComponent, LegendComponent, TooltipComponent, LineChart])

const METRICS_WINDOW_MINUTES = 60

const message = useMessage()
const refreshing = ref(false)

const liveQuery = useQuery({
  queryKey: ['metrics-live'],
  queryFn: api.metrics.live,
})
const seriesQuery = useQuery({
  queryKey: ['metrics-series', METRICS_WINDOW_MINUTES],
  queryFn: () => api.metrics.series(METRICS_WINDOW_MINUTES),
})
const transportsQuery = useQuery({ queryKey: ['transports'], queryFn: api.transports.list })

async function refreshAll() {
  refreshing.value = true
  await Promise.allSettled([liveQuery.refetch(), seriesQuery.refetch(), transportsQuery.refetch()])
  refreshing.value = false
}

const live = computed(() => liveQuery.data.value)
const series = computed(() => seriesQuery.data.value ?? [])

const currentRpm = computed(() => live.value?.total_requests ?? 0)
const avgRpm = computed(() => {
  const rows = series.value
  return rows.length ? Math.round(rows.reduce((sum, row) => sum + row.requests, 0) / rows.length) : 0
})
const peakConcurrent = computed(() =>
  series.value.reduce((peak, row) => Math.max(peak, row.max_concurrent), 0),
)
const errorRate = computed(() => {
  if (!live.value || live.value.total_requests === 0) return '0%'
  const rate = (live.value.total_errors / live.value.total_requests) * 100
  return `${rate.toFixed(1)}%`
})

const chartOption = computed(() => ({
  backgroundColor: 'transparent',
  tooltip: { trigger: 'axis' },
  legend: { data: ['请求数', '错误数'] },
  grid: { left: 48, right: 24, top: 40, bottom: 32 },
  xAxis: {
    type: 'category',
    data: series.value.map((row) => row.minute.slice(11)),
  },
  yAxis: [{ type: 'value', minInterval: 1 }],
  series: [
    {
      name: '请求数',
      type: 'line',
      smooth: true,
      showSymbol: false,
      areaStyle: { opacity: 0.15 },
      data: series.value.map((row) => row.requests),
    },
    {
      name: '错误数',
      type: 'line',
      smooth: true,
      showSymbol: false,
      itemStyle: { color: '#ef4444' },
      data: series.value.map((row) => row.errors),
    },
  ],
}))

// 维度表按分钟窗口聚合: 请求数/错误数累加, 并发取峰值
function aggregateDimension(
  pick: (row: MetricsSeriesRow) => Record<string, UpstreamCounters> | null,
): Array<{ name: string } & UpstreamCounters> {
  const merged = new Map<string, UpstreamCounters>()
  for (const row of series.value) {
    for (const [name, c] of Object.entries(pick(row) ?? {})) {
      const cur = merged.get(name) ?? { requests: 0, errors: 0, max_concurrent: 0 }
      merged.set(name, {
        requests: cur.requests + c.requests,
        errors: cur.errors + c.errors,
        max_concurrent: Math.max(cur.max_concurrent, c.max_concurrent),
      })
    }
  }
  return [...merged.entries()]
    .map(([name, counters]) => ({ name, ...counters }))
    .sort((a, b) => b.requests - a.requests)
}

function dimensionColumns(withConcurrent: boolean): DataTableColumns<{ name: string } & UpstreamCounters> {
  const base: DataTableColumns<{ name: string } & UpstreamCounters> = [
    { title: '名称', key: 'name', ellipsis: { tooltip: true } },
    { title: '请求数', key: 'requests', width: 100, sorter: 'default' },
    { title: '错误数', key: 'errors', width: 100 },
    {
      title: '错误率',
      key: 'errorRate',
      width: 90,
      render: (row) => (row.requests ? `${((row.errors / row.requests) * 100).toFixed(1)}%` : '0%'),
    },
  ]
  if (withConcurrent) {
    base.push({ title: '并发峰值', key: 'max_concurrent', width: 100 })
  }
  return base
}

async function testTransport(name: string) {
  try {
    const result = await api.transports.test(name)
    if (result.ok) message.success(`${name} 连通 ${result.latency_ms}ms`)
    else message.error(`${name} 断: ${result.error ?? '未知错误'}`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '测试失败')
  }
}

const transportColumns: DataTableColumns<Transport> = [
  { title: '名称', key: 'name', width: 160 },
  { title: '地址', key: 'url', ellipsis: { tooltip: true } },
  {
    title: '操作',
    key: 'actions',
    width: 110,
    render: (row) =>
      h(NButton, { size: 'small', onClick: () => testTransport(row.name) }, { default: () => '测试连通' }),
  },
]
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>监控</h1>
        <p class="subtitle">近 {{ METRICS_WINDOW_MINUTES }} 分钟请求指标</p>
      </div>
      <NButton :loading="refreshing" @click="refreshAll">
        <template #icon><NIcon :component="RefreshOutline" /></template>
        刷新
      </NButton>
    </div>

    <div v-if="liveQuery.isError.value || seriesQuery.isError.value" class="error-state">
      <p>指标加载失败</p>
      <NButton @click="refreshAll">重试</NButton>
    </div>

    <template v-else>
      <div class="card-grid stats-grid">
        <div class="stat-card">
          <div class="label">当前 RPM</div>
          <div class="value">{{ currentRpm }}</div>
        </div>
        <div class="stat-card">
          <div class="label">平均 RPM</div>
          <div class="value">{{ avgRpm }}</div>
        </div>
        <div class="stat-card">
          <div class="label">当前并发</div>
          <div class="value">{{ live?.active_concurrent ?? 0 }}</div>
        </div>
        <div class="stat-card">
          <div class="label">并发峰值</div>
          <div class="value">{{ peakConcurrent }}</div>
        </div>
        <div class="stat-card">
          <div class="label">错误率</div>
          <div class="value">{{ errorRate }}</div>
        </div>
      </div>

      <NCard title="请求走势" class="section">
        <VChart :option="chartOption" autoresize class="trend-chart" />
      </NCard>

      <div class="card-grid dims-grid">
        <NCard title="按包">
          <NDataTable :columns="dimensionColumns(false)" :data="aggregateDimension((row) => row.by_package)" size="small" />
        </NCard>
        <NCard title="按上游">
          <NDataTable :columns="dimensionColumns(false)" :data="aggregateDimension((row) => row.by_upstream)" size="small" />
        </NCard>
        <NCard title="按目标">
          <NDataTable :columns="dimensionColumns(true)" :data="aggregateDimension((row) => row.by_target)" size="small" />
        </NCard>
      </div>

      <NCard title="传输实例" class="section">
        <NDataTable
          :columns="transportColumns"
          :data="transportsQuery.data.value ?? []"
          :loading="transportsQuery.isLoading.value"
          size="small"
        />
      </NCard>
    </template>
  </div>
</template>

<style scoped>
.stats-grid {
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
}

.section {
  margin-top: 16px;
}

.trend-chart {
  height: 300px;
}

.dims-grid {
  grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
  margin-top: 16px;
}

.error-state {
  padding: 48px;
  text-align: center;
}
</style>
