<script setup lang="ts">
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { useQuery } from '@tanstack/vue-query'
import {
  NButton,
  NCard,
  NIcon,
  NModal,
  NSwitch,
  NTag,
  useMessage,
} from 'naive-ui'
import {
  ArrowDownOutline,
  CloudDownloadOutline,
  DocumentsOutline,
  RocketOutline,
  SettingsOutline,
  TrashOutline,
} from '@vicons/ionicons5'
import { api, packageExportUrl, packageTemplateUrl } from '@/services/api'
import type { InspectSummary } from '@/services/types'

const router = useRouter()
const message = useMessage()

const packagesQuery = useQuery({ queryKey: ['packages'], queryFn: api.packages.list })

const fileInput = ref<HTMLInputElement | null>(null)
const inspecting = ref(false)
const pendingInspect = ref<InspectSummary | null>(null)
const pendingBytes: { bytes: ArrayBuffer | null; url: string } = { bytes: null, url: '' }

function pickFile() {
  fileInput.value?.click()
}

async function onFileChosen(event: Event) {
  const input = event.target as HTMLInputElement
  const file = input.files?.[0]
  input.value = ''
  if (!file || inspecting.value) return
  inspecting.value = true
  try {
    const bytes = await file.arrayBuffer()
    const summary = await api.packages.inspectBytes(bytes)
    pendingBytes.bytes = bytes
    pendingBytes.url = ''
    pendingInspect.value = summary
  } catch (err) {
    message.error(err instanceof Error ? err.message : '解析失败')
  } finally {
    inspecting.value = false
  }
}

const urlOpen = ref(false)
const importUrl = ref('')
const urlInspecting = ref(false)

async function inspectUrl() {
  if (!importUrl.value.trim() || urlInspecting.value) return
  urlInspecting.value = true
  try {
    const summary = await api.packages.inspectUrl(importUrl.value.trim())
    pendingBytes.bytes = null
    pendingBytes.url = importUrl.value.trim()
    pendingInspect.value = summary
    urlOpen.value = false
  } catch (err) {
    message.error(err instanceof Error ? err.message : '拉取失败')
  } finally {
    urlInspecting.value = false
  }
}

const installing = ref(false)

async function confirmInstall() {
  if (installing.value) return
  installing.value = true
  try {
    if (pendingBytes.bytes !== null) await api.packages.install(pendingBytes.bytes)
    else await api.packages.importUrl(pendingBytes.url)
    message.success('安装成功')
    pendingInspect.value = null
    await packagesQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '安装失败')
  } finally {
    installing.value = false
  }
}

async function toggleEnabled(name: string, enabled: boolean) {
  try {
    await api.packages.enable(name, enabled)
    await packagesQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '操作失败')
    await packagesQuery.refetch()
  }
}

const removing = ref<string | null>(null)

async function confirmRemove() {
  if (!removing.value) return
  try {
    await api.packages.remove(removing.value)
    message.success('已卸载')
    removing.value = null
    await packagesQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '卸载失败')
  }
}
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>插件管理</h1>
        <p class="subtitle">共 {{ packagesQuery.data.value?.length ?? 0 }} 个包</p>
      </div>
      <div class="header-actions">
        <a :href="packageTemplateUrl" target="_blank" rel="noreferrer">
          <NButton quaternary>
            <template #icon><NIcon :component="DocumentsOutline" /></template>
            模板包
          </NButton>
        </a>
        <NButton :loading="urlInspecting" @click="urlOpen = true">
          <template #icon><NIcon :component="CloudDownloadOutline" /></template>
          从 URL 导入
        </NButton>
        <NButton type="primary" :loading="inspecting" @click="pickFile">
          <template #icon><NIcon :component="RocketOutline" /></template>
          导入 .aap
        </NButton>
        <input ref="fileInput" type="file" accept=".aap,.zip" hidden @change="onFileChosen" />
      </div>
    </div>

    <div class="pkg-grid">
      <NCard v-for="pkg in packagesQuery.data.value ?? []" :key="pkg.name" class="pkg-card">
        <div class="pkg-head">
          <div>
            <div class="pkg-title">{{ pkg.title || pkg.name }}</div>
            <div class="pkg-name mono">{{ pkg.name }}</div>
          </div>
          <NSwitch
            :value="pkg.enabled"
            size="small"
            @update:value="(v: boolean) => toggleEnabled(pkg.name, v)"
          />
        </div>
        <p class="pkg-desc">{{ pkg.description || '无描述' }}</p>
        <div class="pkg-badges">
          <NTag size="small">v{{ pkg.version || '?' }} · r{{ pkg.revision }}</NTag>
          <NTag v-if="pkg.hasProtocol" size="small" type="info">协议</NTag>
          <NTag v-for="f in pkg.filters" :key="f" size="small" type="info">{{ f }}</NTag>
          <NTag v-if="pkg.hasHooks" size="small" type="warning">hooks</NTag>
        </div>
        <div class="pkg-actions">
          <NButton
            size="small"
            @click="router.push({ name: 'package-settings', params: { name: pkg.name } })"
          >
            <template #icon><NIcon :component="SettingsOutline" /></template>
            管理
          </NButton>
          <a :href="packageExportUrl(pkg.name)">
            <NButton size="small" quaternary>
              <template #icon><NIcon :component="ArrowDownOutline" /></template>
              导出
            </NButton>
          </a>
          <NButton size="small" quaternary type="error" @click="removing = pkg.name">
            <template #icon><NIcon :component="TrashOutline" /></template>
            卸载
          </NButton>
        </div>
      </NCard>
    </div>

    <NModal
      :show="pendingInspect !== null"
      preset="card"
      title="安装确认"
      class="inspect-modal"
      @update:show="(v: boolean) => { if (!v) pendingInspect = null }"
    >
      <template v-if="pendingInspect">
        <div class="inspect-meta">
          <div class="pkg-title">{{ pendingInspect.title || pendingInspect.name }}</div>
          <div class="mono">{{ pendingInspect.name }} · v{{ pendingInspect.version }}</div>
          <p>{{ pendingInspect.description || '无描述' }}</p>
          <div class="pkg-badges">
            <NTag v-if="pendingInspect.protocol" size="small" type="info">{{ pendingInspect.protocol }}</NTag>
            <NTag v-for="f in pendingInspect.filters" :key="f" size="small" type="info">{{ f }}</NTag>
            <NTag v-if="pendingInspect.hooks" size="small" type="warning">hooks</NTag>
            <NTag
              v-if="pendingInspect.exists"
              size="small"
              type="warning"
            >已安装 v{{ pendingInspect.installedVersion ?? '?' }}(将升级)</NTag>
          </div>
        </div>
        <NButton type="primary" block :loading="installing" @click="confirmInstall">
          确认安装
        </NButton>
      </template>
    </NModal>

    <NModal
      v-model:show="urlOpen"
      preset="card"
      title="从 URL 导入"
      class="url-modal"
    >
      <NInput v-model:value="importUrl" placeholder="https://…/package.aap" @keyup.enter="inspectUrl" />
      <NButton type="primary" block :loading="urlInspecting" style="margin-top:10px" @click="inspectUrl">
        解析
      </NButton>
    </NModal>

    <NModal
      :show="removing !== null"
      preset="dialog"
      type="warning"
      title="卸载插件包"
      :content="`确认卸载 ${removing ?? ''} ?被渠道引用时将拒绝。`"
      positive-text="卸载"
      negative-text="取消"
      @positive-click="confirmRemove"
      @negative-click="removing = null"
      @update:show="(v: boolean) => { if (!v) removing = null }"
    />
  </div>
</template>

<style scoped>
.header-actions {
  display: flex;
  gap: 8px;
}

.header-actions a {
  text-decoration: none;
}

.pkg-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 14px;
}

.pkg-head {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 10px;
}

.pkg-title {
  font-weight: 600;
}

.pkg-name {
  font-size: 12px;
  opacity: 0.6;
}

.pkg-desc {
  margin: 8px 0;
  font-size: 13px;
  opacity: 0.75;
  min-height: 2.6em;
}

.pkg-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.pkg-actions {
  display: flex;
  gap: 6px;
  margin-top: 12px;
}

.pkg-actions a {
  text-decoration: none;
}

.inspect-modal,
.url-modal {
  width: min(480px, 92vw);
}
</style>
