<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import {
  NButton,
  NForm,
  NFormItem,
  NInput,
  NModal,
  NSelect,
  NTag,
  useMessage,
} from 'naive-ui'
import { api } from '@/services/api'
import type { KeyDetail, KeyFormField, KeyRow } from '@/services/types'

const props = defineProps<{ packageName: string }>()

const DATA_SUMMARY_LENGTH = 60

const message = useMessage()

const keysQuery = useQuery({
  queryKey: ['keys', props.packageName],
  queryFn: () => api.keys.list(props.packageName),
})

const keys = computed(() => keysQuery.data.value?.keys ?? [])
const rotation = computed(() => keysQuery.data.value?.rotation ?? 0)
const hooks = computed(() => keysQuery.data.value?.hooks ?? { keyWrite: false, keyRead: false, keyForm: false })

function dataSummary(row: KeyRow): string {
  const text = JSON.stringify(row.data)
  return text.length > DATA_SUMMARY_LENGTH ? `${text.slice(0, DATA_SUMMARY_LENGTH)}…` : text
}

function formatTs(ts: number): string {
  return new Date(ts).toLocaleString('zh-CN', { hour12: false })
}

// 编辑键值
const editing = ref<KeyRow | null>(null)
const editJson = ref('')

function openEdit(row: KeyRow) {
  editing.value = row
  editJson.value = JSON.stringify(row.data, null, 2)
}

async function saveEdit() {
  if (!editing.value) return
  let value: unknown
  try {
    value = JSON.parse(editJson.value)
  } catch {
    message.error('不是合法 JSON')
    return
  }
  try {
    const result = await api.keys.write(props.packageName, editing.value.id, value, editing.value.updatedAt)
    message.success(result.transformed ? '已保存(钩子已归一化)' : '已保存')
    editing.value = null
    await keysQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '保存失败')
  }
}

// 新增键
const adding = ref(false)
const newKeyName = ref('')
const newValueJson = ref('{}')

function nextKeyName(): string {
  const existing = new Set(keys.value.map((k) => k.id))
  let i = keys.value.length + 1
  while (existing.has(`key-${i}`)) i += 1
  return `key-${i}`
}

function openAdd() {
  adding.value = true
  newKeyName.value = nextKeyName()
  newValueJson.value = '{}'
}

async function submitAdd() {
  let value: unknown
  try {
    value = JSON.parse(newValueJson.value)
  } catch {
    message.error('不是合法 JSON')
    return
  }
  try {
    await api.keys.write(props.packageName, newKeyName.value, value, 0)
    message.success('已添加')
    adding.value = false
    await keysQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '添加失败')
  }
}

// 详情 / 恢复 / 删除
const detail = ref<KeyDetail | null>(null)

async function openDetail(row: KeyRow) {
  try {
    detail.value = await api.keys.detail(props.packageName, row.id)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '详情获取失败')
  }
}

async function restore(row: KeyRow) {
  try {
    const d = await api.keys.detail(props.packageName, row.id)
    if (d.prev === undefined) {
      message.error('无可恢复的历史值')
      return
    }
    await api.keys.write(props.packageName, row.id, d.prev, d.updatedAt)
    message.success('已恢复上一版')
    await keysQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '恢复失败')
  }
}

const removing = ref<KeyRow | null>(null)

async function confirmRemove() {
  if (!removing.value) return
  try {
    await api.keys.remove(props.packageName, removing.value.id)
    message.success('已删除')
    removing.value = null
    await keysQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '删除失败')
  }
}

// keyForm 插件表单
const formOpen = ref(false)
const formValues = ref<Record<string, unknown>>({})
const formFieldErrors = ref<Record<string, string>>({})

async function openKeyForm() {
  formValues.value = {}
  formFieldErrors.value = {}
  try {
    await formQuery.refetch()
    formOpen.value = true
  } catch (err) {
    message.error(err instanceof Error ? err.message : '表单不可用')
  }
}

const formQuery = useQuery({
  queryKey: ['key-form', props.packageName],
  queryFn: () => api.keys.form(props.packageName),
  enabled: false,
})

async function runAction(name: string) {
  try {
    const result = await api.keys.formAction(props.packageName, name, formValues.value)
    message.success(result.message || '已执行')
  } catch (err) {
    message.error(err instanceof Error ? err.message : '执行失败')
  }
}

async function submitForm() {
  try {
    const result = await api.keys.formSubmit(props.packageName, formValues.value)
    if (result.ok) {
      message.success(`已添加 ${result.ids.length} 个账号`)
      formOpen.value = false
      await keysQuery.refetch()
    } else {
      formFieldErrors.value = result.errors ?? {}
    }
  } catch (err) {
    message.error(err instanceof Error ? err.message : '提交失败')
  }
}

function coerceValue(field: KeyFormField, raw: string): unknown {
  switch (field.type) {
    case 'bool':
      return raw === 'true'
    case 'int':
      return Number.parseInt(raw, 10)
    case 'number':
      return Number.parseFloat(raw)
    default:
      return raw
  }
}
</script>

<template>
  <div>
    <div class="keys-toolbar">
      <span>密钥池(下次轮询 {{ rotation < keys.length ? rotation : '复位' }})</span>
      <div class="toolbar-actions">
        <NButton v-if="hooks.keyForm" size="small" type="primary" @click="openKeyForm">添加账号</NButton>
        <NButton size="small" @click="openAdd">手动添加</NButton>
      </div>
    </div>

    <table class="keys-table">
      <thead>
        <tr>
          <th>键名</th>
          <th>数据摘要</th>
          <th>更新时间</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="(row, index) in keys" :key="row.id">
          <td class="mono">
            <NTag v-if="index === rotation" size="tiny" type="success" style="margin-right:6px">▸</NTag>
            {{ row.id }}
          </td>
          <td class="mono summary">{{ dataSummary(row) }}</td>
          <td>{{ formatTs(row.updatedAt) }}</td>
          <td class="row-actions">
            <NButton v-if="hooks.keyWrite" size="tiny" @click="openEdit(row)">编辑</NButton>
            <NButton size="tiny" quaternary @click="openDetail(row)">详情</NButton>
            <NButton v-if="row.hasPrev" size="tiny" quaternary type="warning" @click="restore(row)">恢复</NButton>
            <NButton size="tiny" quaternary type="error" @click="removing = row">删除</NButton>
          </td>
        </tr>
        <tr v-if="!keys.length">
          <td colspan="4" class="empty">暂无密钥</td>
        </tr>
      </tbody>
    </table>

    <details class="raw-dump">
      <summary>原始 JSON</summary>
      <pre class="body-pre mono">{{ JSON.stringify(keys, null, 2) }}</pre>
    </details>

    <NModal v-model:show="adding" preset="card" title="添加密钥" class="key-modal">
      <NForm label-placement="top">
        <NFormItem label="键名">
          <NInput v-model:value="newKeyName" class="mono" />
        </NFormItem>
        <NFormItem label="值(JSON)">
          <NInput v-model:value="newValueJson" type="textarea" :rows="6" class="mono" />
        </NFormItem>
        <NButton type="primary" block @click="submitAdd">保存</NButton>
      </NForm>
    </NModal>

    <NModal
      :show="editing !== null"
      preset="card"
      :title="`编辑 ${editing?.id ?? ''}`"
      class="key-modal"
      @update:show="(v: boolean) => { if (!v) editing = null }"
    >
      <NInput v-model:value="editJson" type="textarea" :rows="10" class="mono" />
      <NButton type="primary" block style="margin-top:10px" @click="saveEdit">保存</NButton>
    </NModal>

    <NModal
      :show="detail !== null"
      preset="card"
      :title="`详情 ${detail?.id ?? ''}`"
      class="key-modal wide"
      @update:show="(v: boolean) => { if (!v) detail = null }"
    >
      <pre class="body-pre mono">{{ JSON.stringify(detail?.data, null, 2) }}</pre>
      <template v-if="detail?.prev !== undefined">
        <h4>上一版</h4>
        <pre class="body-pre mono">{{ JSON.stringify(detail?.prev, null, 2) }}</pre>
      </template>
      <template v-if="detail?.explain !== undefined">
        <h4>读取解释(explain)</h4>
        <pre class="body-pre mono">{{ JSON.stringify(detail?.explain, null, 2) }}</pre>
      </template>
    </NModal>

    <NModal
      v-model:show="formOpen"
      preset="card"
      title="添加账号"
      class="key-modal"
    >
      <NForm label-placement="top">
        <NFormItem
          v-for="field in formQuery.data.value?.fields ?? []"
          :key="field.name"
          :label="`${field.description || field.name}${field.required ? ' *' : ''}`"
          :validation-status="field.name && formFieldErrors[field.name] ? 'error' : undefined"
          :feedback="field.name ? formFieldErrors[field.name] : undefined"
        >
          <NSelect
            v-if="field.type === 'enum'"
            :options="(field.values ?? []).map((v) => ({ label: v, value: v }))"
            @update:value="(v: string) => { if (field.name) formValues[field.name] = v }"
          />
          <NSelect
            v-else-if="field.type === 'bool'"
            :options="[{ label: 'true', value: 'true' }, { label: 'false', value: 'false' }]"
            @update:value="(v: string) => { if (field.name) formValues[field.name] = coerceValue(field, v) }"
          />
          <NInput
            v-else
            type="text"
            @update:value="(v: string) => { if (field.name && v !== '') formValues[field.name] = coerceValue(field, v) }"
          />
        </NFormItem>
      </NForm>
      <div class="form-actions">
        <NButton
          v-for="action in formQuery.data.value?.actions ?? []"
          :key="action.name"
          @click="runAction(action.name)"
        >
          {{ action.label || action.name }}
        </NButton>
      </div>
      <NButton type="primary" block style="margin-top:10px" @click="submitForm">提交</NButton>
    </NModal>

    <NModal
      :show="removing !== null"
      preset="dialog"
      type="warning"
      title="删除密钥"
      :content="`确认删除 ${removing?.id ?? ''} ?`"
      positive-text="删除"
      negative-text="取消"
      @positive-click="confirmRemove"
      @negative-click="removing = null"
      @update:show="(v: boolean) => { if (!v) removing = null }"
    />
  </div>
</template>

<style scoped>
.keys-toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 10px;
  font-size: 13px;
}

.toolbar-actions {
  display: flex;
  gap: 8px;
}

.keys-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}

.keys-table th,
.keys-table td {
  padding: 8px 10px;
  text-align: left;
  border-bottom: 1px solid rgba(128, 128, 128, 0.2);
}

.summary {
  max-width: 360px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.row-actions {
  white-space: nowrap;
}

.row-actions button {
  margin-right: 4px;
}

.empty {
  text-align: center;
  opacity: 0.5;
  padding: 20px;
}

.raw-dump {
  margin-top: 12px;
  font-size: 13px;
}

.key-modal {
  width: min(520px, 92vw);
}

.key-modal.wide {
  width: min(680px, 92vw);
}

.form-actions {
  display: flex;
  gap: 8px;
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
