<script setup lang="ts">
import { computed, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import { NButton, NForm, NFormItem, NInput, NModal, NTag, useMessage } from 'naive-ui'
import { api } from '@/services/api'
import type { KeyRow } from '@/services/types'

const props = defineProps<{ packageName: string }>()

const DATA_SUMMARY_LENGTH = 60

const message = useMessage()

const keysQuery = useQuery({
  queryKey: ['keys', props.packageName],
  queryFn: () => api.keys.list(props.packageName),
})

const keys = computed(() => keysQuery.data.value?.keys ?? [])
const rotation = computed(() => keysQuery.data.value?.rotation ?? 0)
// 凭据区两态:含 login/ = 管理页接管(iframe);否则宿主单字段手编
const hasLogin = computed(() => keysQuery.data.value?.login ?? false)

function dataSummary(row: KeyRow): string {
  const text = JSON.stringify(row.data)
  return text.length > DATA_SUMMARY_LENGTH ? `${text.slice(0, DATA_SUMMARY_LENGTH)}…` : text
}

function formatTs(ts: number): string {
  return new Date(ts).toLocaleString('zh-CN', { hour12: false })
}

// 编辑键值(PUT 原样入库;展示脱敏由包 inspect.js 承担)
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
    await api.keys.write(props.packageName, editing.value.id, value, editing.value.updatedAt)
    message.success('已保存')
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

// 删除
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

// 管理页接管(login/ui/ iframe,同源 serve)
const mgmtOpen = ref(false)
const mgmtKey = ref(0)

function openMgmt() {
  mgmtOpen.value = true
  mgmtKey.value += 1 // 重开重载页面(登录态刷新)
}
</script>

<template>
  <div>
    <div class="keys-toolbar">
      <span>账号池(下次轮询 {{ rotation < keys.length ? rotation : '复位' }})</span>
      <div class="toolbar-actions">
        <NButton v-if="hasLogin" size="small" type="primary" @click="openMgmt">管理页</NButton>
        <NButton size="small" @click="openAdd">手动添加</NButton>
      </div>
    </div>

    <table class="keys-table">
      <thead>
        <tr>
          <th>账号</th>
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
            <NButton size="tiny" @click="openEdit(row)">编辑</NButton>
            <NButton size="tiny" quaternary type="error" @click="removing = row">删除</NButton>
          </td>
        </tr>
        <tr v-if="!keys.length">
          <td colspan="4" class="empty">暂无账号</td>
        </tr>
      </tbody>
    </table>

    <details class="raw-dump">
      <summary>原始 JSON</summary>
      <pre class="body-pre mono">{{ JSON.stringify(keys, null, 2) }}</pre>
    </details>

    <NModal v-model:show="adding" preset="card" title="添加账号" class="key-modal">
      <NForm label-placement="top">
        <NFormItem label="账号 ID">
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
      :show="mgmtOpen"
      preset="card"
      title="登录管理页"
      class="mgmt-modal"
      @update:show="(v: boolean) => { if (!v) mgmtOpen = false }"
    >
      <iframe
        v-if="mgmtOpen"
        :key="mgmtKey"
        :src="api.creds.loginUIUrl(props.packageName)"
        class="mgmt-frame"
      />
    </NModal>

    <NModal
      :show="removing !== null"
      preset="dialog"
      type="warning"
      title="删除账号"
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

.mgmt-modal {
  width: min(720px, 94vw);
}

.mgmt-frame {
  width: 100%;
  height: 70vh;
  border: none;
  border-radius: 8px;
  background: rgba(128, 128, 128, 0.06);
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
