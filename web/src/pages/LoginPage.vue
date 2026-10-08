<script setup lang="ts">
import { ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { NButton, NCard, NForm, NFormItem, NIcon, NInput, useMessage } from 'naive-ui'
import { LockClosedOutline, PersonOutline } from '@vicons/ionicons5'
import { useAuth } from '@/stores/auth'

const route = useRoute()
const router = useRouter()
const message = useMessage()
const { login } = useAuth()

const user = ref('admin')
const password = ref('')
const submitting = ref(false)

async function submit() {
  if (!user.value || !password.value || submitting.value) return
  submitting.value = true
  try {
    await login(user.value, password.value)
    await router.push((route.query.redirect as string) ?? { name: 'monitor' })
  } catch (err) {
    message.error(err instanceof Error ? err.message : '登录失败')
  } finally {
    submitting.value = false
  }
}
</script>

<template>
  <div class="login-page">
    <NCard class="login-card" :bordered="true">
      <div class="brand">
        <div class="brand-mark">A</div>
        <h1>ai-api-proxy</h1>
        <p>API 网关管理台</p>
      </div>
      <NForm label-placement="top" @keyup.enter="submit">
        <NFormItem label="用户名">
          <NInput v-model:value="user" placeholder="admin" :input-props="{ autocomplete: 'username' }">
            <template #prefix><NIcon :component="PersonOutline" /></template>
          </NInput>
        </NFormItem>
        <NFormItem label="密码">
          <NInput
            v-model:value="password"
            type="password"
            show-password-on="click"
            placeholder="密码"
            :input-props="{ autocomplete: 'current-password' }"
          >
            <template #prefix><NIcon :component="LockClosedOutline" /></template>
          </NInput>
        </NFormItem>
        <NButton type="primary" block attr-type="submit" :loading="submitting" @click="submit">
          登录
        </NButton>
      </NForm>
    </NCard>
  </div>
</template>

<style scoped>
.login-page {
  display: grid;
  place-items: center;
  min-height: 100vh;
  padding: 24px;
}

.login-card {
  width: min(380px, 100%);
}

.brand {
  text-align: center;
  margin-bottom: 20px;
}

.brand-mark {
  display: grid;
  place-items: center;
  width: 48px;
  height: 48px;
  margin: 0 auto 10px;
  border-radius: 12px;
  background: var(--brand);
  color: #fff;
  font-weight: 700;
  font-size: 22px;
}

.brand h1 {
  margin: 0;
  font-size: 18px;
}

.brand p {
  margin: 4px 0 0;
  font-size: 13px;
  opacity: 0.6;
}
</style>
