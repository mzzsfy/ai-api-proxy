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
  NInputNumber,
  NModal,
  NSelect,
  NTag,
  useMessage,
  type DataTableColumns,
} from 'naive-ui'
import {
  AddOutline,
  ChatbubbleEllipsesOutline,
  GitNetworkOutline,
  PlayOutline,
  TrashOutline,
} from '@vicons/ionicons5'
import { api } from '@/services/api'
import type { DiscoveryHit, DiscoveryRule, GroupMember, ModelGroup } from '@/services/types'

const message = useMessage()

const groupsQuery = useQuery({ queryKey: ['groups'], queryFn: api.groups.list })
const packagesQuery = useQuery({ queryKey: ['packages'], queryFn: api.packages.list })
const transportsQuery = useQuery({ queryKey: ['transports'], queryFn: api.transports.list })

const transportOptions = computed(() => [
  { label: 'direct', value: '' },
  ...(transportsQuery.data.value ?? []).map((t) => ({ label: t.name, value: t.name })),
])

const pluginOptions = computed(() =>
  (packagesQuery.data.value ?? []).filter((p) => p.hasProtocol).map((p) => ({ label: p.title || p.name, value: p.name })),
)

// ── 渠道编辑(抽屉;插件编辑态不可改,传输从 params 拆出独立编辑)──
const editOpen = ref(false)
const editId = ref<number | null>(null)
const editPrefix = ref('')
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
  editPrefix.value = ''
  editPlugin.value = pluginOptions.value[0]?.value ?? ''
  editEnabled.value = true
  editTransport.value = ''
  editParamsJson.value = '{}'
  editOpen.value = true
}

function openEdit(row: ModelGroup) {
  editId.value = row.id
  editPrefix.value = row.prefix
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
  if (!editPrefix.value.trim()) {
    message.error('前缀不能为空')
    return
  }
  try {
    if (editId.value === null) {
      await api.groups.create({ prefix: editPrefix.value.trim(), plugin: editPlugin.value, params, enabled: editEnabled.value })
      message.success('已创建渠道')
    } else {
      await api.groups.update(editId.value, {
        id: editId.value,
        prefix: editPrefix.value.trim(),
        plugin: editPlugin.value,
        params,
        enabled: editEnabled.value,
      })
      message.success('已保存')
    }
    editOpen.value = false
    await groupsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '保存失败')
  }
}

// ── 渠道级测试/对话测试/删除 ──
async function runTest(row: ModelGroup) {
  try {
    const result = await api.groups.test(row.id)
    if (result.ok) message.success(`${row.prefix} 连通 ${result.latency_ms}ms`)
    else message.error(`${row.prefix} 失败: ${result.error ?? '未知错误'}`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '测试失败')
  }
}

const chatOpen = ref(false)
const chatGroupId = ref<number | null>(null)
const chatGroupLabel = ref('')
const chatMessage = ref('')
const chatBusy = ref(false)
const chatResult = ref<string>('')

function openChatTest(row: ModelGroup) {
  chatGroupId.value = row.id
  chatGroupLabel.value = row.prefix
  chatMessage.value = ''
  chatResult.value = ''
  chatOpen.value = true
}

async function runChatTest() {
  if (!chatMessage.value.trim()) {
    message.warning('请输入测试消息')
    return
  }
  if (chatGroupId.value === null || chatBusy.value) return
  chatBusy.value = true
  try {
    const result = await api.groups.chatTest(chatGroupId.value, chatMessage.value)
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

const deleting = ref<ModelGroup | null>(null)

async function confirmDelete() {
  if (!deleting.value) return
  try {
    await api.groups.remove(deleting.value.id)
    message.success('已删除')
    deleting.value = null
    await groupsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '删除失败')
  }
}

// ── 渠道内模型管理(弹窗)──
const membersOpen = ref(false)
const membersGroup = ref<ModelGroup | null>(null)
const membersQuery = useQuery({
  queryKey: computed(() => ['members', membersGroup.value?.id]),
  queryFn: () => api.groups.members(membersGroup.value!.id),
  enabled: computed(() => membersGroup.value !== null),
})
const newMemberName = ref('')
const newMemberEnabled = ref(false)

function openMembers(row: ModelGroup) {
  membersGroup.value = row
  newMemberName.value = ''
  newMemberEnabled.value = false
  membersOpen.value = true
  // 重开同一渠道也强制拉最新数据(模型可能经自动发现等其他路径变更)
  void membersQuery.refetch()
}

async function addMember() {
  const name = newMemberName.value.trim()
  if (!name || !membersGroup.value) {
    message.error('模型名不能为空')
    return
  }
  try {
    await api.groups.createMember(membersGroup.value.id, { name, enabled: newMemberEnabled.value })
    newMemberName.value = ''
    await membersQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '添加失败')
  }
}

const memberBusy = ref(false)

async function toggleMember(member: GroupMember) {
  if (!membersGroup.value || memberBusy.value) return
  memberBusy.value = true
  try {
    await api.groups.updateMember(member.id, { group_id: member.group_id, name: member.name, enabled: !member.enabled })
    await membersQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '更新失败')
  } finally {
    memberBusy.value = false
  }
}

const deletingMember = ref<GroupMember | null>(null)

async function confirmDeleteMember() {
  if (!deletingMember.value) return
  try {
    await api.groups.removeMember(deletingMember.value.id)
    deletingMember.value = null
    await membersQuery.refetch()
    await groupsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '删除失败')
  }
}

const memberColumns: DataTableColumns<GroupMember> = [
  { title: 'ID', key: 'id', width: 70 },
  { title: '短名', key: 'name', ellipsis: { tooltip: true } },
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
    width: 140,
    render: (row) => [
      h(NButton, { size: 'small', onClick: () => toggleMember(row) }, { default: () => (row.enabled ? '停用' : '启用') }),
      h(
        NButton,
        { size: 'small', type: 'error', quaternary: true, style: 'margin-left:6px', onClick: () => (deletingMember.value = row) },
        { icon: () => h(NIcon, { component: TrashOutline }) },
      ),
    ],
  },
]

// ── 渠道模型发现(弹窗:规则管道 + 预览 + 导入)──
interface RuleRow {
  op: 'only' | 'exclude' | 'rename' | 'limit'
  regex: string
  from: string
  to: string
  limit: number | null
}

function emptyRule(): RuleRow {
  return { op: 'only', regex: '', from: '', to: '', limit: null }
}

const discoverOpen = ref(false)
const discoverGroup = ref<ModelGroup | null>(null)
const discoverRules = ref<RuleRow[]>([emptyRule()])
const discoverEnabled = ref(false)
const discoverBusy = ref(false)
const discoverPreview = ref<DiscoveryHit[] | null>(null)
const discoverFetched = ref(0)

const opOptions = [
  { label: 'only(保留匹配)', value: 'only' },
  { label: 'exclude(丢弃匹配)', value: 'exclude' },
  { label: 'rename(改名)', value: 'rename' },
  { label: 'limit(截断)', value: 'limit' },
]

function openDiscover(row: ModelGroup) {
  discoverGroup.value = row
  discoverRules.value = [emptyRule()]
  discoverEnabled.value = false
  discoverPreview.value = null
  discoverFetched.value = 0
  discoverOpen.value = true
}

function toRequestRules(rules: RuleRow[]): DiscoveryRule[] {
  return rules.map((r) => {
    switch (r.op) {
      case 'rename':
        return { op: r.op, from: r.from, to: r.to }
      case 'limit':
        return { op: r.op, limit: r.limit ?? 50 }
      default:
        return { op: r.op, regex: r.regex }
    }
  })
}

async function runDiscover(apply: boolean) {
  if (!discoverGroup.value || discoverBusy.value) return
  if (!apply && discoverPreview.value !== null) {
    // 已预览过再点预览:直接复用,避免重复外呼
    return
  }
  discoverBusy.value = true
  try {
    const result = await api.groups.discover(discoverGroup.value.id, toRequestRules(discoverRules.value), discoverEnabled.value, apply)
    discoverPreview.value = result.models
    discoverFetched.value = result.fetched_total
    if (apply) {
      message.success(`导入完成: 新建 ${result.applied?.created ?? 0},跳过 ${result.applied?.skipped ?? 0}`)
      await groupsQuery.refetch()
      if (membersGroup.value?.id === discoverGroup.value.id) await membersQuery.refetch()
      discoverOpen.value = false
    } else {
      message.success(`上游共 ${result.fetched_total} 个模型,匹配 ${result.models.length} 个`)
    }
  } catch (err) {
    message.error(err instanceof Error ? err.message : '发现失败')
  } finally {
    discoverBusy.value = false
  }
}

function rulePlaceholder(field: 'regex' | 'from' | 'to', op: RuleRow['op']): string {
  if (op === 'rename') return field === 'from' ? '^gemini-' : '替换为(可空)'
  return field === 'regex' ? '^gemini' : ''
}

const discoverColumns: DataTableColumns<DiscoveryHit> = [
  { title: '上游模型名', key: 'upstream_name', ellipsis: { tooltip: true } },
  { title: '路由模型名', key: 'member_name', ellipsis: { tooltip: true } },
  {
    title: '动作',
    key: 'action',
    width: 200,
    render: (row) =>
      h('span', null, [
        h(
          NTag,
          { size: 'small', type: row.action === 'create' ? 'success' : 'default' },
          { default: () => (row.action === 'create' ? '新建' : '已存在跳过') },
        ),
        row.action === 'create' && row.poll_groups > 1
          ? h('span', { style: 'margin-left:6px;color:var(--n-warning-color, #f0a020)' }, `轮询×${row.poll_groups}`)
          : null,
      ]),
  },
]

// ── 渠道表格 ──
const columns: DataTableColumns<ModelGroup> = [
  { title: 'ID', key: 'id', width: 70 },
  { title: '渠道标识', key: 'prefix', width: 160, ellipsis: { tooltip: true } },
  { title: '协议插件', key: 'plugin', width: 180 },
  {
    title: '渠道参数',
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
    width: 400,
    render: (row) => [
      h(NButton, { size: 'small', onClick: () => openMembers(row) }, { default: () => '模型' }),
      h(NButton, { size: 'small', style: 'margin-left:6px', onClick: () => openDiscover(row) }, { icon: () => h(NIcon, { component: GitNetworkOutline }) }),
      h(NButton, { size: 'small', style: 'margin-left:6px', onClick: () => openEdit(row) }, { default: () => '编辑' }),
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
        <h1>渠道管理</h1>
        <p class="subtitle">共 {{ groupsQuery.data.value?.length ?? 0 }} 个渠道；配置上游协议、参数与传输方式，模型作为渠道下的可调用路由</p>
      </div>
      <NButton type="primary" @click="openCreate">
        <template #icon><NIcon :component="AddOutline" /></template>
        新建渠道
      </NButton>
    </div>

    <NDataTable
      :columns="columns"
      :data="groupsQuery.data.value ?? []"
      :loading="groupsQuery.isLoading.value"
      size="small"
    />

    <NDrawer v-model:show="editOpen" :width="480" placement="right">
      <NDrawerContent :title="editId === null ? '新建渠道' : `编辑渠道 #${editId}`" closable>
        <NForm label-placement="top">
          <NFormItem label="渠道前缀(模型全路径请求的命名空间,不可含 /)">
            <NInput v-model:value="editPrefix" placeholder="opencode" />
          </NFormItem>
          <NFormItem v-if="editId !== null" label="协议插件包">
            <NInput :value="editPlugin" disabled />
          </NFormItem>
          <NFormItem v-else label="协议插件包">
            <NSelect v-model:value="editPlugin" :options="pluginOptions" />
          </NFormItem>
          <NFormItem label="传输通道">
            <NSelect v-model:value="editTransport" :options="transportOptions" />
          </NFormItem>
          <NFormItem label="渠道参数(JSON)">
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
      v-model:show="membersOpen"
      preset="card"
      class="members-modal"
      :title="`渠道模型 — ${membersGroup?.prefix ?? ''}`"
    >
      <div style="display: flex; gap: 8px; margin-bottom: 12px">
        <NInput v-model:value="newMemberName" placeholder="渠道模型名(如 gpt-4o,可含 /)" @keyup.enter="addMember" />
        <NCheckbox v-model:checked="newMemberEnabled" style="align-self: center; white-space: nowrap">直接启用</NCheckbox>
        <NButton type="primary" @click="addMember">添加</NButton>
      </div>
      <NDataTable
        :columns="memberColumns"
        :data="membersQuery.data.value ?? []"
        :loading="membersQuery.isLoading.value"
        size="small"
        :max-height="360"
      />
    </NModal>

    <NModal
      v-model:show="discoverOpen"
      preset="card"
      class="discover-modal"
      :title="`渠道模型发现 — ${discoverGroup?.prefix ?? ''}`"
    >
      <p class="discover-hint">
        从上游 /v1/models 拉取列表,按规则管道过滤改名。至少需要一条 only/exclude 规则。
      </p>
      <div v-for="(rule, i) in discoverRules" :key="i" class="rule-row">
        <NSelect
          v-model:value="rule.op"
          :options="opOptions"
          style="width: 190px"
          @update:value="(v: string) => { if (v === 'limit' && rule.limit === null) rule.limit = 50 }"
        />
        <NInput
          v-if="rule.op === 'only' || rule.op === 'exclude'"
          v-model:value="rule.regex"
          :placeholder="rulePlaceholder('regex', rule.op)"
          class="mono"
        />
        <template v-else-if="rule.op === 'rename'">
          <NInput v-model:value="rule.from" :placeholder="rulePlaceholder('from', rule.op)" class="mono" />
          <NInput v-model:value="rule.to" :placeholder="rulePlaceholder('to', rule.op)" class="mono" />
        </template>
        <NInputNumber v-else v-model:value="rule.limit" :min="1" placeholder="上限" style="width: 140px" />
        <NButton quaternary type="error" @click="discoverRules.splice(i, 1)">删</NButton>
      </div>
      <NButton size="small" style="margin-top: 8px" @click="discoverRules.push(emptyRule())">加一条规则</NButton>
      <div style="margin-top: 12px">
        <NCheckbox v-model:checked="discoverEnabled">导入后直接启用(默认停用,待人工确认)</NCheckbox>
      </div>
      <div style="margin-top: 12px; display: flex; gap: 8px">
        <NButton :loading="discoverBusy" @click="discoverPreview = null; runDiscover(false)">预览</NButton>
        <NButton type="primary" :loading="discoverBusy" @click="runDiscover(true)">导入</NButton>
        <span v-if="discoverPreview !== null" class="discover-hint" style="align-self: center">
          上游 {{ discoverFetched }} 个,匹配 {{ discoverPreview.length }} 个
        </span>
      </div>
      <NDataTable
        v-if="discoverPreview !== null"
        :columns="discoverColumns"
        :data="discoverPreview"
        size="small"
        :max-height="320"
        style="margin-top: 12px"
      />
    </NModal>

    <NModal
      v-model:show="chatOpen"
      preset="card"
      :title="`渠道对话测试 — ${chatGroupLabel}`"
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
      title="删除渠道"
      :content="`确认删除渠道 ${deleting?.prefix ?? ''} ?渠道下的模型一并删除。`"
      positive-text="删除"
      negative-text="取消"
      @positive-click="confirmDelete"
      @negative-click="deleting = null"
      @update:show="(v: boolean) => { if (!v) deleting = null }"
    />

    <NModal
      :show="deletingMember !== null"
      preset="dialog"
      type="warning"
      title="删除渠道模型"
      :content="`确认删除渠道模型 ${deletingMember?.name ?? ''} ?`"
      positive-text="删除"
      negative-text="取消"
      @positive-click="confirmDeleteMember"
      @negative-click="deletingMember = null"
      @update:show="(v: boolean) => { if (!v) deletingMember = null }"
    />
  </div>
</template>

<style scoped>
.chat-modal,
.members-modal {
  width: min(680px, 92vw);
}

.discover-modal {
  width: min(760px, 94vw);
}

.discover-hint {
  color: var(--n-text-color-disabled, rgba(128, 128, 128, 0.8));
  font-size: 13px;
  margin: 0 0 10px;
}

.rule-row {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-bottom: 8px;
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
