<script setup lang="ts">
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import { NButton, NCheckbox, NInput, NSelect, NTabPane, NTabs, NTag, useMessage } from 'naive-ui'
import KeysPanel from '@/components/KeysPanel.vue'
import { api } from '@/services/api'
import type { PackageSettings, SettingsTask } from '@/services/types'

const route = useRoute()
const router = useRouter()
const message = useMessage()

const packagesQuery = useQuery({ queryKey: ['packages'], queryFn: api.packages.list })
const packageOptions = computed(() =>
  (packagesQuery.data.value ?? []).map((p) => ({
    label: `${p.title || p.name} · ${p.name}`,
    value: p.name,
  })),
)

const selected = computed(() => route.params.name as string | undefined)

function selectPackage(name: string | null) {
  if (!name) return
  router.push({ name: 'package-settings', params: { name } })
}

const settingsQuery = useQuery({
  queryKey: ['package-settings', selected],
  queryFn: () => api.packages.settings(selected.value!),
  enabled: computed(() => !!selected.value),
})

const activeTab = ref('config')

// 配置与任务本地副本(保存时全量提交)
const draft = reactive({
  loaded: false,
  config: {} as Record<string, unknown>,
  tasks: {} as Record<string, { cron: string; disabled: boolean }>,
  version: 0,
  slots: [] as PackageSettings['config'],
  taskRows: [] as SettingsTask[],
})

watch(
  () => settingsQuery.data.value,
  (data) => {
    if (!data) return
    draft.loaded = true
    draft.version = data.version
    draft.slots = data.config
    draft.taskRows = data.tasks ?? []
    draft.config = Object.fromEntries(data.config.map((slot) => [slot.name, slot.value]))
    draft.tasks = Object.fromEntries(
      (data.tasks ?? []).map((task) => [
        task.name,
        { cron: task.cron, disabled: !task.enabled },
      ]),
    )
  },
)

const packageMeta = computed(() =>
  (packagesQuery.data.value ?? []).find((p) => p.name === selected.value),
)

const saving = ref(false)

// 槽位类型转换(int/number 槽后端只接受数值类型,文本输入需转换)
function coerceSlots(): Record<string, unknown> | null {
  const out: Record<string, unknown> = {}
  for (const slot of draft.slots) {
    const raw = draft.config[slot.name]
    if (raw === undefined || raw === '') continue
    if (slot.schema.type === 'int' || slot.schema.type === 'number') {
      const num = Number(raw)
      if (Number.isNaN(num)) {
        message.error(`槽 ${slot.name} 需要数值, 当前值 "${String(raw)}" 非法`)
        return null
      }
      out[slot.name] = num
    } else {
      out[slot.name] = raw
    }
  }
  return out
}

async function save() {
  if (!selected.value || saving.value) return
  const config = coerceSlots()
  if (config === null) return
  saving.value = true
  try {
    const result = await api.packages.saveSettings(selected.value, {
      config,
      tasks: draft.tasks,
      version: draft.version,
    })
    draft.version = result.version
    message.success('已保存')
    await settingsQuery.refetch()
  } catch (err) {
    const text = err instanceof Error ? err.message : '保存失败'
    if (text.includes('version mismatch')) {
      message.error('设置已被他人修改, 已刷新, 请重试')
      await settingsQuery.refetch()
    } else {
      message.error(text)
    }
  } finally {
    saving.value = false
  }
}

async function resetDefaults() {
  const withDefault = draft.slots.filter((slot) => slot.schema.default !== undefined)
  for (const slot of draft.slots) {
    if (slot.schema.default !== undefined) draft.config[slot.name] = slot.schema.default
    else delete draft.config[slot.name]
  }
  message.info(`已回填 ${withDefault.length} 个默认槽, 其余覆盖将清除, 保存后生效`)
  await save()
}

// 任务测试
const taskTesting = ref<string | null>(null)

async function runTask(task: SettingsTask) {
  if (!selected.value || taskTesting.value) return
  taskTesting.value = task.name
  try {
    const result = await api.packages.runTask(selected.value, task.name)
    const okCount = result.results.filter((r) => r.ok).length
    const failures = result.results.filter((r) => !r.ok)
    if (failures.length) {
      message.error(`任务 ${task.name}: ${okCount} 成功, ${failures.length} 失败: ${failures[0]?.error ?? ''}`)
    } else {
      const totalMs = result.results.reduce((sum, r) => sum + r.duration_ms, 0)
      message.success(`任务 ${task.name}: ${okCount} 账号 · 总耗时 ${totalMs}ms`)
    }
  } catch (err) {
    message.error(err instanceof Error ? err.message : '任务执行失败')
  } finally {
    taskTesting.value = null
  }
}

function slotInputValue(slot: PackageSettings['config'][number]): string {
  return String(draft.config[slot.name] ?? '')
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>插件配置</h1>
        <p class="subtitle">选择包后管理配置、账号与任务</p>
      </div>
      <NSelect
        :value="selected ?? null"
        :options="packageOptions"
        placeholder="选择插件包"
        class="pkg-select"
        @update:value="selectPackage"
      />
    </div>

    <div v-if="!selected" class="empty-state">请先选择一个插件包</div>

    <template v-else>
      <div v-if="packageMeta" class="meta-bar">
        <NTag size="small">v{{ packageMeta.version || '?' }} · r{{ packageMeta.revision }}</NTag>
        <NTag v-if="packageMeta.protocol" size="small" type="info">{{ packageMeta.protocol }}</NTag>
        <NTag v-for="f in packageMeta.filters" :key="f" size="small" type="info">{{ f }}</NTag>
        <NTag v-if="packageMeta.hasHooks" size="small" type="warning">hooks</NTag>
      </div>

      <NTabs v-model:value="activeTab" type="line" animated>
        <NTabPane name="config" tab="配置">
          <div v-if="settingsQuery.isLoading.value" class="empty-state">加载中…</div>
          <div v-else-if="settingsQuery.isError.value" class="empty-state">
            加载失败
            <NButton size="small" style="margin-left:8px" @click="settingsQuery.refetch()">重试</NButton>
          </div>
          <template v-else>
            <div v-for="slot in draft.slots" :key="slot.name" class="slot-row">
              <div class="slot-label">
                <span>{{ slot.schema.description || slot.name }}</span>
                <span class="slot-name mono">{{ slot.name }}</span>
                <NTag v-if="slot.overridden" size="tiny" type="warning">已覆盖</NTag>
              </div>
              <NSelect
                v-if="slot.schema.type === 'enum'"
                :value="String(draft.config[slot.name] ?? '')"
                :options="(slot.schema.values ?? []).map((v) => ({ label: v, value: v }))"
                @update:value="(v: string) => { draft.config[slot.name] = v }"
              />
              <NCheckbox
                v-else-if="slot.schema.type === 'bool'"
                :checked="draft.config[slot.name] === true"
                @update:checked="(v: boolean) => { draft.config[slot.name] = v }"
              />
              <NInput
                v-else
                :value="slotInputValue(slot)"
                :type="slot.schema.type === 'int' || slot.schema.type === 'number' ? 'text' : 'text'"
                :placeholder="slot.schema.default !== undefined ? String(slot.schema.default) : ''"
                @update:value="(v: string) => { draft.config[slot.name] = v }"
              />
            </div>
            <div v-if="!draft.slots.length" class="empty-state">此包无可配置项</div>
            <div class="tab-actions">
              <NButton quaternary @click="resetDefaults">恢复默认</NButton>
              <NButton type="primary" :loading="saving" @click="save">保存配置</NButton>
            </div>
          </template>
        </NTabPane>

        <NTabPane name="keys" tab="账号管理">
          <KeysPanel v-if="activeTab === 'keys'" :package-name="selected" />
        </NTabPane>

        <NTabPane name="tasks" tab="任务">
          <div v-for="task in draft.taskRows" :key="task.name" class="task-row">
            <div class="task-name">
              <span>{{ task.name }}</span>
              <NTag v-if="task.overridden" size="tiny" type="warning">已覆盖</NTag>
            </div>
            <NInput
              v-if="!task.next"
              :value="draft.tasks[task.name]?.cron ?? task.cron"
              placeholder="cron 表达式"
              class="mono task-cron"
              @update:value="(v: string) => { const t = draft.tasks[task.name]; if (t) t.cron = v }"
            />
            <span v-else class="task-next">自调度 · 下次 {{ task.next }}</span>
            <NCheckbox
              :checked="!(draft.tasks[task.name]?.disabled)"
              @update:checked="(v: boolean) => { const t = draft.tasks[task.name]; if (t) t.disabled = !v }"
            >启用</NCheckbox>
            <NButton size="small" :loading="taskTesting === task.name" @click="runTask(task)">
              测试
            </NButton>
          </div>
          <div v-if="!draft.taskRows.length" class="empty-state">此包无定时任务</div>
          <div v-if="draft.taskRows.length" class="tab-actions">
            <NButton type="primary" :loading="saving" @click="save">保存任务改动</NButton>
          </div>
        </NTabPane>
      </NTabs>
    </template>
  </div>
</template>

<style scoped>
.pkg-select {
  width: 300px;
}

.empty-state {
  padding: 40px;
  text-align: center;
  opacity: 0.55;
}

.meta-bar {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  margin-bottom: 14px;
}

.slot-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 0;
  border-bottom: 1px solid rgba(128, 128, 128, 0.14);
}

.slot-label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
}

.slot-name {
  font-size: 11px;
  opacity: 0.5;
}

.tab-actions {
  display: flex;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 16px;
}

.task-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 10px 0;
  border-bottom: 1px solid rgba(128, 128, 128, 0.14);
}

.task-name {
  display: flex;
  align-items: center;
  gap: 8px;
  min-width: 160px;
  font-size: 14px;
}

.task-cron {
  width: 200px;
}

.task-next {
  font-size: 12px;
  opacity: 0.6;
}
</style>
