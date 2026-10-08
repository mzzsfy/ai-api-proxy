<script setup lang="ts">
import { h, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { NButton, NDataTable, NForm, NFormItem, NIcon, NInput, NModal, NSelect, useMessage, type DataTableColumns } from 'naive-ui'
import { PulseOutline } from '@vicons/ionicons5'
import { api } from '@/services/api'
import type { Transport } from '@/services/types'

const SCOPES = [
  { label: 'egress(出口 IP)', value: 'egress' },
  { label: 'lease(会话租约)', value: 'lease' },
]

const message = useMessage()
const evictOpen = ref(false)
const evictTransport = ref('')
const evictScope = ref<'egress' | 'lease'>('egress')
const evictValue = ref('')

const transportsQuery = useQuery({ queryKey: ['transports'], queryFn: api.transports.list })

async function testTransport(name: string) {
  try {
    const result = await api.transports.test(name)
    if (result.ok) message.success(`${name} 连通 ${result.latency_ms}ms`)
    else message.error(`${name} 断: ${result.error ?? '未知错误'}`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '测试失败')
  }
}

async function submitEvict() {
  if (!evictValue.value.trim()) {
    message.error('失效值不能为空')
    return
  }
  try {
    await api.transports.evict(evictTransport.value, evictScope.value, evictValue.value.trim())
    message.success(`已对 ${evictTransport.value} 执行失效命令`)
    evictOpen.value = false
  } catch (err) {
    message.error(err instanceof Error ? err.message : '失效命令失败')
  }
}

function openEvict(row: Transport) {
  evictTransport.value = row.name
  evictScope.value = 'egress'
  evictValue.value = ''
  evictOpen.value = true
}

const columns: DataTableColumns<Transport> = [
  { title: '名称', key: 'name', width: 160 },
  { title: '地址', key: 'url', ellipsis: { tooltip: true } },
  {
    title: '操作',
    key: 'actions',
    width: 210,
    render: (row) => [
      h(
        NButton,
        { size: 'small', onClick: () => testTransport(row.name) },
        { icon: () => h(NIcon, { component: PulseOutline }), default: () => '测试连通' },
      ),
      h(
        NButton,
        { size: 'small', quaternary: true, type: 'warning', style: 'margin-left:6px', onClick: () => openEvict(row) },
        { default: () => '失效命令' },
      ),
    ],
  },
]
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>传输通道</h1>
        <p class="subtitle">共 {{ transportsQuery.data.value?.length ?? 0 }} 个实例</p>
      </div>
      <NButton @click="transportsQuery.refetch()">刷新</NButton>
    </div>

    <NDataTable
      :columns="columns"
      :data="transportsQuery.data.value ?? []"
      :loading="transportsQuery.isLoading.value"
      size="small"
    />

    <NModal
      v-model:show="evictOpen"
      preset="card"
      :title="`失效命令 · ${evictTransport}`"
      class="evict-modal"
    >
      <NForm label-placement="top">
        <NFormItem label="范围">
          <NSelect v-model:value="evictScope" :options="SCOPES" />
        </NFormItem>
        <NFormItem label="失效值(egress 为 IP, lease 为会话 ID)">
          <NInput v-model:value="evictValue" class="mono" />
        </NFormItem>
        <NButton type="warning" block @click="submitEvict">执行失效</NButton>
      </NForm>
    </NModal>
  </div>
</template>

<style scoped>
.evict-modal {
  width: min(440px, 92vw);
}
</style>
