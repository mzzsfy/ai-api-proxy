<script setup lang="ts">
import { h, ref } from 'vue'
import { useQuery } from '@tanstack/vue-query'
import {
  NButton, NDataTable, NForm, NFormItem, NInput, NModal, NPopconfirm, NTag, useMessage,
  type DataTableColumns,
} from 'naive-ui'
import { api } from '@/services/api'
import type { Subscription } from '@/services/types'

const message = useMessage()
const createOpen = ref(false)
const syncing = ref<Set<number>>(new Set())

const form = ref({ name: '', repo_url: '', user: '', path: '', token: '' })

const subsQuery = useQuery({ queryKey: ['subscriptions'], queryFn: api.subscriptions.list })

function resetForm() {
  form.value = { name: '', repo_url: '', user: '', path: '', token: '' }
}

async function submitCreate() {
  if (!form.value.name.trim() || !form.value.repo_url.trim()) {
    message.error('名称与仓库地址必填')
    return
  }
  try {
    await api.subscriptions.create({
      name: form.value.name.trim(),
      repo_url: form.value.repo_url.trim(),
      user: form.value.user.trim(),
      path: form.value.path.trim() || '.',
      token: form.value.token,
    })
    message.success('订阅已创建')
    createOpen.value = false
    resetForm()
    await subsQuery.refetch()
  } catch (err) {
    message.error(err instanceof Error ? err.message : '创建失败')
  }
}

async function syncSub(row: Subscription) {
  syncing.value.add(row.id)
  try {
    await api.subscriptions.sync(row.id)
    message.success(`${row.name} 同步完成`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '同步失败')
  } finally {
    syncing.value.delete(row.id)
    await subsQuery.refetch()
  }
}

async function removeSub(row: Subscription) {
  try {
    await api.subscriptions.remove(row.id)
    message.success(`${row.name} 已删除(导入的包已卸载)`)
  } catch (err) {
    message.error(err instanceof Error ? err.message : '删除失败')
  }
  await subsQuery.refetch()
}

const columns: DataTableColumns<Subscription> = [
  { title: '名称', key: 'name', width: 140 },
  { title: '仓库', key: 'repo_url', ellipsis: { tooltip: true } },
  { title: '路径', key: 'path', width: 130, ellipsis: { tooltip: true } },
  { title: '分支', key: 'branch', width: 100 },
  {
    title: 'commit',
    key: 'last_synced_commit',
    width: 110,
    render: (row) =>
      row.last_synced_commit
        ? h('span', { class: 'mono' }, row.last_synced_commit.slice(0, 8))
        : h('span', { style: 'opacity:0.4' }, '未同步'),
  },
  {
    title: '包数',
    key: 'package_count',
    width: 70,
    render: (row) => h(NTag, { size: 'small', type: row.package_count > 0 ? 'success' : 'default' }, { default: () => row.package_count }),
  },
  {
    title: '状态',
    key: 'last_error',
    width: 160,
    ellipsis: { tooltip: true },
    render: (row) =>
      row.last_error
        ? h(NTag, { size: 'small', type: 'error' }, { default: () => row.last_error })
        : h('span', { style: 'opacity:0.6' }, row.last_synced_at || '-'),
  },
  {
    title: '操作',
    key: 'actions',
    width: 190,
    render: (row) => [
      h(
        NButton,
        { size: 'small', loading: syncing.value.has(row.id), onClick: () => syncSub(row) },
        { default: () => '立即同步' },
      ),
      h(
        NPopconfirm,
        { onPositiveClick: () => removeSub(row) },
        {
          trigger: () =>
            h(NButton, { size: 'small', quaternary: true, type: 'error', style: 'margin-left:6px' }, { default: () => '删除' }),
          default: () => `删除订阅 ${row.name} 并卸载其导入的 ${row.package_count} 个包?`,
        },
      ),
    ],
  },
]
</script>

<template>
  <div class="page">
    <div class="page-header">
      <div>
        <h1>仓库订阅</h1>
        <p class="subtitle">从 git 仓库订阅插件包,手动同步导入</p>
      </div>
      <div>
        <NButton style="margin-right:8px" @click="subsQuery.refetch()">刷新</NButton>
        <NButton type="primary" @click="createOpen = true">新增订阅</NButton>
      </div>
    </div>

    <NDataTable
      :columns="columns"
      :data="subsQuery.data.value ?? []"
      :loading="subsQuery.isLoading.value"
      size="small"
    />

    <NModal v-model:show="createOpen" preset="card" title="新增订阅" class="sub-modal">
      <NForm label-placement="top">
        <NFormItem label="名称(唯一标识)">
          <NInput v-model:value="form.name" />
        </NFormItem>
        <NFormItem label="仓库地址(https,私有库填 PAT)">
          <NInput v-model:value="form.repo_url" class="mono" placeholder="https://git.example.com/org/repo" />
        </NFormItem>
        <NFormItem label="包路径(仓库内子目录,单包仓库填 .)">
          <NInput v-model:value="form.path" class="mono" placeholder="plugins/my-pkg" />
        </NFormItem>
        <NFormItem label="认证用户名(GitHub 任意值,Gitea 账号名,GitLab 填 oauth2)">
          <NInput v-model:value="form.user" />
        </NFormItem>
        <NFormItem label="访问令牌 PAT(公有库留空)">
          <NInput v-model:value="form.token" type="password" show-password-on="click" />
        </NFormItem>
        <NButton type="primary" block @click="submitCreate">创建</NButton>
      </NForm>
    </NModal>
  </div>
</template>

<style scoped>
.sub-modal {
  width: min(480px, 92vw);
}
</style>
