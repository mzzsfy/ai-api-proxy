<script setup lang="ts">
import { computed, provide, ref, watchEffect } from 'vue'
import {
  NConfigProvider,
  NDialogProvider,
  NMessageProvider,
  darkTheme,
  type GlobalThemeOverrides,
} from 'naive-ui'

type ThemePreference = 'light' | 'dark' | 'system'
const themeKey = 'aap-theme'

const stored = localStorage.getItem(themeKey) as ThemePreference | null
const preference = ref<ThemePreference>(stored ?? 'system')

const systemDark = ref(window.matchMedia('(prefers-color-scheme: dark)').matches)
window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
  systemDark.value = e.matches
})

const theme = computed(() =>
  preference.value === 'system' ? (systemDark.value ? 'dark' : 'light') : preference.value,
)

watchEffect(() => {
  localStorage.setItem(themeKey, preference.value)
  document.documentElement.dataset.theme = theme.value
})

provide('theme', { preference, theme })

const overrides = computed<GlobalThemeOverrides>(() => ({
  common: {
    borderRadius: '10px',
    primaryColor: '#6366f1',
    primaryColorHover: '#818cf8',
    primaryColorPressed: '#4f46e5',
    primaryColorSuppl: '#6366f1',
  },
}))
</script>

<template>
  <NConfigProvider
    :theme="theme === 'dark' ? darkTheme : null"
    :theme-overrides="overrides"
    class="app-root"
  >
    <NMessageProvider placement="top">
      <NDialogProvider>
        <RouterView />
      </NDialogProvider>
    </NMessageProvider>
  </NConfigProvider>
</template>

<style scoped>
.app-root {
  min-height: 100vh;
}
</style>
