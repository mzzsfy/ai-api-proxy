export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message)
  }
}

// 401 时由 auth 模块注册的清理回调(避免 http 层反向依赖 store)
let onUnauthorized = () => {}
export function setUnauthorizedHandler(fn: () => void) {
  onUnauthorized = fn
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/admin/api${path}`, {
    credentials: 'same-origin',
    ...init,
    headers: init?.body instanceof FormData
      ? init?.headers
      : { 'Content-Type': 'application/json', ...init?.headers },
  })

  if (res.status === 401) {
    onUnauthorized()
    location.hash = `#/login?redirect=${encodeURIComponent(location.hash.slice(1))}`
    throw new ApiError(401, '会话已过期, 请重新登录')
  }
  if (!res.ok) {
    const detail = await res.text().catch(() => '')
    throw new ApiError(res.status, detail || `请求失败 (${res.status})`)
  }
  if (res.status === 204) return undefined as T
  const body: unknown = await res.json()
  return body as T
}

export function get<T>(path: string): Promise<T> {
  return request<T>(path)
}

export function post<T>(path: string, body?: unknown): Promise<T> {
  return request<T>(path, {
    method: 'POST',
    body: body instanceof FormData ? body : JSON.stringify(body ?? {}),
  })
}

// 二进制上传走 octet-stream(后端按原始字节读取,JSON 化会破坏包体)
export function postRaw<T>(path: string, bytes: ArrayBuffer): Promise<T> {
  return request<T>(path, {
    method: 'POST',
    body: bytes,
    headers: { 'Content-Type': 'application/octet-stream' },
  })
}

export function put<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, { method: 'PUT', body: JSON.stringify(body) })
}

export function del(path: string): Promise<void> {
  return request<void>(path, { method: 'DELETE' })
}
