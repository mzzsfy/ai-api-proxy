<script setup lang="ts">
import { computed, inject, ref, type Component, type Ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NDropdown, NDrawer, NDrawerContent, NIcon, NLayout, NLayoutContent, NLayoutHeader, NLayoutSider, useMessage } from 'naive-ui'
import {
  AnalyticsOutline,
  CloudDownloadOutline,
  ExtensionPuzzleOutline,
  GitNetworkOutline,
  LayersOutline,
  LogOutOutline,
  MenuOutline,
  MoonOutline,
  PulseOutline,
  SunnyOutline,
} from '@vicons/ionicons5'
import { useAuth } from '@/stores/auth'

type NavItem = { to: { name: string; params?: Record<string, string> }; label: string; icon: Component }

const route = useRoute()
const router = useRouter()
const message = useMessage()
const { logout } = useAuth()
const mobileMenuOpen = ref(false)

type ThemePreference = 'light' | 'dark' | 'system'
const themeCtx = inject<{ preference: Ref<ThemePreference>; theme: Ref<'light' | 'dark'> }>('theme')!

const themeOptions = [
  { label: '亮色', key: 'light' },
  { label: '暗色', key: 'dark' },
  { label: '跟随系统', key: 'system' },
]

function onThemeSelect(key: string) {
  themeCtx.preference.value = key as ThemePreference
}

const navItems: NavItem[] = [
  { to: { name: 'monitor' }, label: '监控', icon: PulseOutline },
  { to: { name: 'models' }, label: '渠道管理', icon: LayersOutline },
  { to: { name: 'packages' }, label: '插件管理', icon: ExtensionPuzzleOutline },
  { to: { name: 'history' }, label: '请求历史', icon: AnalyticsOutline },
  { to: { name: 'transports' }, label: '传输通道', icon: GitNetworkOutline },
  { to: { name: 'subscriptions' }, label: '仓库订阅', icon: CloudDownloadOutline },
]

const pageTitle = computed(() => {
  const name = route.name as string | undefined
  const settingsMatch = name === 'package-settings'
  return settingsMatch ? '插件配置' : navItems.find((item) => item.to.name === name)?.label ?? 'ai-api-proxy'
})

async function signOut() {
  try {
    await logout()
    await router.push({ name: 'login' })
  } catch {
    message.error('退出登录失败')
  }
}

function closeMobileMenu() {
  mobileMenuOpen.value = false
}
</script>

<template>
  <NLayout has-sider class="app-shell">
    <NLayoutSider bordered :width="232" class="desktop-sidebar">
      <div class="brand-block">
        <div class="brand-mark">A</div>
        <div>
          <div class="brand-name">ai-api-proxy</div>
          <div class="brand-caption">API 网关管理台</div>
        </div>
      </div>
      <nav class="side-nav" aria-label="主导航">
        <RouterLink
          v-for="item in navItems"
          :key="item.to.name"
          :to="item.to"
          class="nav-link"
        >
          <NIcon :component="item.icon" :size="19" />
          <span>{{ item.label }}</span>
        </RouterLink>
      </nav>
    </NLayoutSider>

    <NLayout>
      <NLayoutHeader bordered class="topbar">
        <div class="topbar-left">
          <NButton class="mobile-menu-button" quaternary circle aria-label="打开导航" @click="mobileMenuOpen = true">
            <template #icon><NIcon :component="MenuOutline" /></template>
          </NButton>
          <h1>{{ pageTitle }}</h1>
        </div>
        <div class="topbar-actions">
          <NDropdown :options="themeOptions" trigger="click" @select="onThemeSelect">
            <NButton quaternary circle :aria-label="`主题: ${themeOptions.find((o) => o.key === themeCtx.preference.value)?.label ?? ''}`">
              <template #icon>
                <NIcon :component="themeCtx.theme.value === 'dark' ? MoonOutline : SunnyOutline" />
              </template>
            </NButton>
          </NDropdown>
          <NButton quaternary circle aria-label="退出登录" @click="signOut">
            <template #icon><NIcon :component="LogOutOutline" /></template>
          </NButton>
        </div>
      </NLayoutHeader>

      <NLayoutContent class="content-wrap">
        <main class="page-content" tabindex="-1">
          <RouterView />
        </main>
      </NLayoutContent>
    </NLayout>
  </NLayout>

  <NDrawer v-model:show="mobileMenuOpen" placement="left" :width="272">
    <NDrawerContent title="ai-api-proxy" closable>
      <nav class="mobile-nav" aria-label="移动端主导航">
        <RouterLink
          v-for="item in navItems"
          :key="item.to.name"
          :to="item.to"
          class="nav-link"
          @click="closeMobileMenu"
        >
          <NIcon :component="item.icon" :size="19" />
          <span>{{ item.label }}</span>
        </RouterLink>
      </nav>
    </NDrawerContent>
  </NDrawer>
</template>

<style scoped>
.app-shell {
  height: 100vh;
}

.brand-block {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 18px 18px 14px;
}

.brand-mark {
  display: grid;
  place-items: center;
  width: 36px;
  height: 36px;
  border-radius: 10px;
  background: var(--brand);
  color: #fff;
  font-weight: 700;
  font-size: 17px;
}

.brand-name {
  font-weight: 650;
  font-size: 15px;
}

.brand-caption {
  font-size: 12px;
  opacity: 0.55;
}

.side-nav,
.mobile-nav {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding: 8px 10px;
}

.nav-link {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 12px;
  border-radius: 8px;
  color: inherit;
  text-decoration: none;
  font-size: 14px;
  transition: background 0.15s;
}

.nav-link:hover {
  background: rgba(128, 128, 128, 0.14);
}

.nav-link.router-link-active {
  background: rgba(99, 102, 241, 0.16);
  color: var(--brand-hover);
  font-weight: 550;
}

.topbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  height: var(--topbar-height);
  padding: 0 24px;
}

.topbar-left {
  display: flex;
  align-items: center;
  gap: 10px;
}

.topbar-left h1 {
  margin: 0;
  font-size: 17px;
  font-weight: 600;
}

.topbar-actions {
  display: flex;
  align-items: center;
  gap: 10px;
}

.content-wrap {
  height: calc(100vh - var(--topbar-height));
}

.page-content {
  height: 100%;
  overflow-y: auto;
}

.mobile-menu-button {
  display: none;
}

@media (max-width: 768px) {
  .desktop-sidebar {
    display: none;
  }

  .mobile-menu-button {
    display: inline-flex;
  }
}
</style>
