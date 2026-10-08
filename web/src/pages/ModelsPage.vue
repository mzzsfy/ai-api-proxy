<script setup lang="ts">
import { computed, h, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import {
  NButton,
  NCheckbox,
  NDataTable,
  NDrawer,
  NDrawerContent,
  NForm,
  NFormItem,
  NIcon,
  NInput,
  NModal,
  NSelect,
  NTag,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import { AddOutline, ChatbubbleEllipsesOutline, PlayOutline, TrashOutline } from '@vicons/ionicons5'
import { api } from '@/services/api'
import type { ModelRow } from '@/services/types'

const message = useMessage()

const modelsQuery = useQuery({ queryKey: ['models'], queryFn: api.models.list })
const packagesQuery = useQuery({ queryKey: ['packages'], queryFn: api.packages.list })
const transportsQuery = useQuery({ queryKey: ['transports'], queryFn: api.transports.list })

const transportOptions = computed(() => [
  { label: 'direct', value: '' },
  ...(transportsQuery.data.value ?? []).map((t) => ({ label: t.name, value: t.name })),
])

const pluginOptions = computed(() =>
  (packagesQuery.data.value ?? []).filter((p) => p.hasProtocol).map((p) => ({ label: p.title || p.name, value: p.name })),
)

// 编辑态: name/plugin 不可改, transport 从 params 拆出独立编辑
const editOpen = ref(false)
const editId = ref<number | null>(null)
const editName = ref('')
const editPlugin = ref('')
const editEnabled = ref(true)
const editTransport = ref('')
const editParamsJson = ref('')

function paramsWithoutTransport(params: Record<string, unknown>): string {
  const rest = Object.fromEntries(Object.entries(params).filter(([k]) => k !== 'transport'))
  return JSON.stringify(rest, null, 2)
}

function openCreate() {
  editId.value = null
  editName.value = ''
  editPlugin.value = pluginOptions.value[0]?.value ?? ''
  editEnabled.value = true
  editTransport.value = ''
  editParamsJson.value = '{}'
  editOpen.value = true
}

function openEdit(row: ModelRow) {
  editId.value = row.id
  editName.value = row.name
  editPlugin.value = row.plugin
  editEnabled.value = row.enabled
  editTransport.value = typeof row.params['transport'] === 'string' ? row.params['transport'] : ''
  editParamsJson.value = paramsWithoutTransport(row.params)
  editOpen.value = true
}

async function saveEdit() {
  let params: Record<string, unknown>
  try {
    params = JSON.parse(editParamsJson.value) as Record<string, unknown>
  } catch {
    message.error('params 不是合法 JSON')
    return
  }
  if (editTransport.value) params['transport'] = editTransport.value
  else delete params['transport']

  try {
    if (editId.value === null) {
      const names = parseBatchNames(editName.value)
      if (!names.length) {
        message.error('模型名不能为空')
        return
      }
      let saved = 0
      let firstError = ''
      for (const name of names) {
        try {
          await api.models.create({ id: 0, name, plugin: editPlugin.value, params, enabled: editEnabled.value })
          saved += 1
        } catch (err) {
          firstError ||= err instanceof Error ? err.message : '未知错误'
        }
      }
      if (saved === names.length) message.success(`已保存 ${saved}/${names.length}`)
      else message.error(`已保存 ${saved}/${names.length}, 首个失败: ${firstError}`)
    } else {
      await api.models.update(editId.value, {
        id: editId.value,
        name: editName.value,
        plugin: editPlugin.value,
        params,
        enabled: editEnabled.value,
      })
      message.success('已保存')
    }
    editOpen.value = false
    await modelsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '保存失败')
  }
}

function parseBatchNames(raw: string): string[] {
  const names = raw.split(/[,，;；、\s]+/).map((s) => s.trim()).filter(Boolean)
  return [...new Set(names)]
}

const chatOpen = ref(false)
const chatModelId = ref<number | null>(null)
const chatMessage = ref('')
const chatBusy = ref(false)
const chatResult = ref<string>('')

function openChatTest(row: ModelRow) {
  chatModelId.value = row.id
  chatMessage.value = ''
  chatResult.value = ''
  chatOpen.value = true
}

async function runChatTest() {
  if (!chatMessage.value.trim()) {
    message.warning('请输入测试消息')
    return
  }
  if (chatModelId.value === null || chatBusy.value) return
  chatBusy.value = true
  try {
    const result = await api.models.chatTest(chatModelId.value, chatMessage.value)
    const bodyText = typeof result.body === 'string' ? result.body : JSON.stringify(result.body, null, 2)
    chatResult.value = [
      `ok: ${result.ok}`,
      result.status !== undefined ? `status: ${result.status}` : '',
      `latency: ${result.latency_ms}ms`,
      result.error ? `error: ${result.error}` : '',
      bodyText ? `body:\n${bodyText}` : '',
    ].filter(Boolean).join('\n')
  } catch (err) {
    chatResult.value = err instanceof Error ? err.message : '测试失败'
  } finally {
    chatBusy.value = false
  }
}

const deleting = ref<ModelRow | null>(null)

async function confirmDelete() {
  if (!deleting.value) return
  try {
    await api.models.remove(deleting.value.id)
    message.success('已删除')
    deleting.value = null
    await modelsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '删除失败')
  }
}

async function runTest(row: ModelRow) {
  try {
    const result = await api.models.test(row.id)
    if (result.ok) message.success(`${row.name} 连通 ${result.latency_ms}ms`)
    else message.error(`${row.name} 失败: ${result.error ?? '未知错误'}`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '测试失败')
  }
}

const columns: DataTableColumns<ModelRow> = [
  { title: 'ID', key: 'id', width: 70 },
  { title: '模型名', key: 'name', width: 200, ellipsis: { tooltip: true } },
  { title: '插件包', key: 'plugin', width: 160 },
  {
    title: 'params',
    key: 'params',
    render: (row) => h('code', { class: 'mono' }, paramsWithoutTransport(row.params)),
  },
  {
    title: '状态',
    key: 'enabled',
    width: 80,
    render: (row) =>
      h(NTag, { size: 'small', type: row.enabled ? 'success' : 'default' }, { default: () => (row.enabled ? '启用' : '停用') }),
  },
  {
    title: '操作',
    key: 'actions',
    width: 280,
    render: (row) => [
      h(NButton, { size: 'small', onClick: () => openEdit(row) }, { default: () => '编辑' }),
      h(NButton, { size: 'small', style: 'margin-left:6px', onClick: () => runTest(row) }, { icon: () => h(NIcon, { component: PlayOutline }) }),
      h(NButton, { size: 'small', style: 'margin-left:6px', onClick: () => openChatTest(row) }, { icon: () => h(NIcon, { component: ChatbubbleEllipsesOutline }) }),
      h(NButton, { size: 'small', type: 'error', quaternary: true, style: 'margin-left:6px', onClick: () => (deleting.value = row) }, { icon: () => h(NIcon, { component: TrashOutline }) }),
    ],
  },
]
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>模型管理</h1>
        <p class="subtitle">共 {{ modelsQuery.data.value?.length ?? 0 }} 行</p>
      </div>
      <NButton type="primary" @click="openCreate">
        <template #icon><NIcon :component="AddOutline" /></template>
        新建模型行
      </NButton>
    </div>

    <NDataTable
      :columns="columns"
      :data="modelsQuery.data.value ?? []"
      :loading="modelsQuery.isLoading.value"
      size="small"
    />

    <NDrawer v-model:show="editOpen" :width="480" placement="right">
      <NDrawerContent :title="editId === null ? '新建模型行' : `编辑 #${editId}`" closable>
        <NForm label-placement="top">
          <NFormItem label="模型名(新建支持批量: 逗号/空格分隔)">
            <NInput
              v-model:value="editName"
              :disabled="editId !== null"
              :placeholder="editId === null ? 'gpt-4o, claude-sonnet …' : ''"
            />
          </NFormItem>
          <NFormItem v-if="editId !== null" label="插件包">
            <NInput :value="editPlugin" disabled />
          </NFormItem>
          <NFormItem v-else label="插件包">
            <NSelect v-model:value="editPlugin" :options="pluginOptions" />
          </NFormItem>
          <NFormItem label="传输通道">
            <NSelect v-model:value="editTransport" :options="transportOptions" />
          </NFormItem>
          <NFormItem label="params(JSON)">
            <NInput v-model:value="editParamsJson" type="textarea" :rows="8" class="mono" />
          </NFormItem>
          <NFormItem label="启用">
            <NCheckbox v-model:checked="editEnabled" />
          </NFormItem>
          <NButton type="primary" block @click="saveEdit">保存</NButton>
        </NForm>
      </NDrawerContent>
    </NDrawer>

    <NModal
      v-model:show="chatOpen"
      preset="card"
      :title="`对话测试 #${chatModelId}`"
      class="chat-modal"
    >
      <NInput v-model:value="chatMessage" type="text" placeholder="测试消息" @keyup.enter="runChatTest" />
      <NButton type="primary" :loading="chatBusy" style="margin-top:10px" @click="runChatTest">
        发送
      </NButton>
      <pre v-if="chatResult" class="mono chat-result">{{ chatResult }}</pre>
    </NModal>

    <NModal
      :show="deleting !== null"
      preset="dialog"
      type="warning"
      title="删除模型行"
      :content="`确认删除 ${deleting?.name ?? ''} ?删除后需重新创建。`"
      positive-text="删除"
      negative-text="取消"
      @positive-click="confirmDelete"
      @negative-click="deleting = null"
      @update:show="(v: boolean) => { if (!v) deleting = null }"
    />
  </div>
</template>

<style scoped>
.chat-modal {
  width: min(680px, 92vw);
}

.chat-result {
  margin-top: 12px;
  padding: 12px;
  border-radius: 8px;
  background: rgba(128, 128, 128, 0.12);
  max-height: 320px;
  overflow: auto;
  white-space: pre-wrap;
}
</style>
