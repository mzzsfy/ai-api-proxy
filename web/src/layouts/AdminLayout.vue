<script setup lang="ts">
import { computed, inject, ref, type Component, type Ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NDrawer, NDrawerContent, NIcon, NLayout, NLayoutContent, NLayoutHeader, NLayoutSider, useMessage } from 'naive-ui'
import {
  AnalyticsOutline,
  CubeOutline,
  ExtensionPuzzleOutline,
  GitNetworkOutline,
  LogOutOutline,
  MenuOutline,
  MoonOutline,
  PulseOutline,
  SettingsOutline,
  SunnyOutline,
} from '@vicons/ionicons5'
import { useAuth } from '@/stores/auth'

type NavItem = { to: { name: string; params?: Record<string, string> }; label: string; icon: Component }

const route = useRoute()
const router = useRouter()
const message = useMessage()
const { principal, logout } = useAuth()
const mobileMenuOpen = ref(false)

const themeCtx = inject<{ mode: Ref<'light' | 'dark'>; toggle: () => void }>('theme')!
const themeMode = themeCtx.mode
const toggleTheme = themeCtx.toggle

const navItems: NavItem[] = [
  { to: { name: 'monitor' }, label: '监控', icon: PulseOutline },
  { to: { name: 'models' }, label: '模型管理', icon: CubeOutline },
  { to: { name: 'packages' }, label: '插件管理', icon: ExtensionPuzzleOutline },
  { to: { name: 'history' }, label: '请求历史', icon: AnalyticsOutline },
  { to: { name: 'transports' }, label: '传输通道', icon: GitNetworkOutline },
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
      <div class="sidebar-footer">
        <button class="theme-toggle" :aria-label="themeMode === 'dark' ? '切换亮色' : '切换暗色'" @click="toggleTheme()">
          <NIcon :component="themeMode === 'dark' ? SunnyOutline : MoonOutline" :size="17" />
          <span>{{ themeMode === 'dark' ? '亮色模式' : '暗色模式' }}</span>
        </button>
      </div>
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
          <div class="user-chip">
            <span class="user-avatar">{{ (principal?.user ?? 'A').slice(0, 1).toUpperCase() }}</span>
            <span>{{ principal?.user ?? 'admin' }}</span>
          </div>
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

.sidebar-footer {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  padding: 12px;
  border-top: 1px solid rgba(128, 128, 128, 0.2);
}

.theme-toggle {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;
  padding: 8px 10px;
  border: none;
  border-radius: 8px;
  background: transparent;
  color: inherit;
  font-size: 13px;
  cursor: pointer;
}

.theme-toggle:hover {
  background: rgba(128, 128, 128, 0.14);
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

.user-chip {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
  opacity: 0.85;
}

.user-avatar {
  display: grid;
  place-items: center;
  width: 26px;
  height: 26px;
  border-radius: 50%;
  background: rgba(99, 102, 241, 0.22);
  color: var(--brand-hover);
  font-size: 12px;
  font-weight: 650;
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
