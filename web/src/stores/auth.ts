import { ref } from 'vue'
import { setUnauthorizedHandler } from '@/services/http'

export type Principal = { user: string }

const principal = ref<Principal | null>(null)
let probe: Promise<boolean> | null = null

function reset() {
  principal.value = null
  probe = null
}
setUnauthorizedHandler(reset)

// 会话为 cookie, 前端只负责探测与跳转, 状态无需持久化
export function useAuth() {
  async function load(): Promise<boolean> {
    if (principal.value) return true
    probe ??= fetch('/admin/api/me', { credentials: 'same-origin' })
      .then(async (res) => {
        if (!res.ok) return false
        const body: unknown = await res.json()
        principal.value = body as Principal
        return true
      })
      .catch(() => false)
    return probe
  }

  async function login(user: string, password: string): Promise<void> {
    const res = await fetch('/admin/api/login', {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user, password }),
    })
    if (!res.ok) throw new Error(`登录失败 (${res.status})`)
    principal.value = { user }
  }

  async function logout(): Promise<void> {
    await fetch('/admin/api/logout', { method: 'POST', credentials: 'same-origin' })
    reset()
  }

  return { principal, load, login, logout }
}
