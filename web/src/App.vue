<script setup lang="ts">
import { computed, provide, ref, watchEffect } from 'vue'
import {
  NConfigProvider,
  NDialogProvider,
  NMessageProvider,
  darkTheme,
  type GlobalThemeOverrides,
} from 'naive-ui'

type ThemeMode = 'light' | 'dark'
const themeKey = 'aap-theme'
const stored = localStorage.getItem(themeKey) as ThemeMode | null
const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
const theme = ref<ThemeMode>(stored ?? (prefersDark ? 'dark' : 'light'))

watchEffect(() => {
  localStorage.setItem(themeKey, theme.value)
  document.documentElement.dataset.theme = theme.value
})

provide('theme', {
  mode: theme,
  toggle: () => {
    theme.value = theme.value === 'dark' ? 'light' : 'dark'
  },
})

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
