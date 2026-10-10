import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { del, get, post, postRaw, put, setUnauthorizedHandler } from './http'

// fetch/Response 构造辅助:status + json/text 形态
function resOf(status: number, body: { json?: unknown; text?: string }) {
  return {
    status,
    ok: status >= 200 && status < 300,
    text: async () => body.text ?? '',
    json: async () => body.json,
  } as Response
}

describe('http request', () => {
  beforeEach(() => {
    vi.stubGlobal('location', { hash: '#/dashboard' })
    vi.stubGlobal('fetch', vi.fn())
  })
  afterEach(() => {
    vi.unstubAllGlobals()
    setUnauthorizedHandler(() => {})
  })

  it('401 触发未授权回调并跳转登录', async () => {
    const onUnauthorized = vi.fn()
    setUnauthorizedHandler(onUnauthorized)
    vi.mocked(fetch).mockResolvedValue(resOf(401, {}))

    await expect(get('/x')).rejects.toMatchObject({ status: 401 })
    expect(onUnauthorized).toHaveBeenCalledOnce()
    expect(location.hash).toBe('#/login?redirect=%2Fdashboard')
  })

  it('非 2xx 抛出携带响应正文的 ApiError', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(500, { text: 'boom' }))

    await expect(get('/x')).rejects.toMatchObject({ status: 500, message: 'boom' })
  })

  it('非 2xx 且空正文时回退默认文案', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(502, {}))

    await expect(get('/x')).rejects.toMatchObject({ status: 502, message: '请求失败 (502)' })
  })

  it('204 返回 undefined 不解析 json', async () => {
    vi.mocked(fetch).mockResolvedValue({ status: 204, ok: true } as Response)

    await expect(del('/x')).resolves.toBeUndefined()
  })

  it('JSON post 自动补 Content-Type 并序列化 body', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(200, { json: { ok: 1 } }))

    await expect(post('/x', { a: 1 })).resolves.toEqual({ ok: 1 })
    expect(fetch).toHaveBeenCalledWith('/admin/api/x', expect.objectContaining({
      method: 'POST',
      body: '{"a":1}',
      credentials: 'same-origin',
    }))
    const init = vi.mocked(fetch).mock.calls[0]![1]!
    expect(init.headers).toMatchObject({ 'Content-Type': 'application/json' })
  })

  it('FormData 不注入 JSON Content-Type', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(200, { json: {} }))
    const fd = new FormData()

    await post('/x', fd)
    const init = vi.mocked(fetch).mock.calls[0]![1]!
    expect(init.body).toBe(fd)
    expect(init.headers ?? {}).not.toHaveProperty('Content-Type')
  })

  it('postRaw 以 octet-stream 发送原始字节', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(200, { json: {} }))
    const bytes = new ArrayBuffer(3)

    await postRaw('/x', bytes)
    const init = vi.mocked(fetch).mock.calls[0]![1]!
    expect(init.body).toBe(bytes)
    expect(init.headers).toMatchObject({ 'Content-Type': 'application/octet-stream' })
  })

  it('get 不携带 body 且路径带 /admin/api 前缀', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(200, { json: {} }))

    await get('/models')
    expect(fetch).toHaveBeenCalledWith('/admin/api/models', expect.objectContaining({
      credentials: 'same-origin',
    }))
    const init = vi.mocked(fetch).mock.calls[0]![1]!
    expect(init.body).toBeUndefined()
  })

  it('put 以 JSON 方法与 body 发送', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(200, { json: {} }))

    await put('/x', { a: 1 })
    const init = vi.mocked(fetch).mock.calls[0]![1]!
    expect(init.method).toBe('PUT')
    expect(init.body).toBe('{"a":1}')
    expect(init.headers).toMatchObject({ 'Content-Type': 'application/json' })
  })

  it('401 抛出会话过期文案', async () => {
    vi.mocked(fetch).mockResolvedValue(resOf(401, {}))

    await expect(get('/x')).rejects.toMatchObject({ message: '会话已过期, 请重新登录' })
  })
})
