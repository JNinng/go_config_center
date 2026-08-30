// 控制台 API 封装（ADR-0012）：admin_token 存 localStorage，写请求经 Authorization 头
// 携带；对 Follower 的写请求按 307 语义由浏览器自动重发到 Leader（服务端已附 CORS 头）。
import { ref } from 'vue'

const TOKEN_KEY = 'gcc_admin_token'

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? ''
}

export function setToken(token: string): void {
  localStorage.setItem(TOKEN_KEY, token)
  unauthorized.value = false
}

/** 任一请求收到 401 时置位，由 App 层展示 token 输入；setToken 后复位。 */
export const unauthorized = ref(false)

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly currentVersion?: number

  constructor(status: number, code: string, message: string, currentVersion?: number) {
    super(message)
    this.status = status
    this.code = code
    this.currentVersion = currentVersion
  }
}

export async function api<T>(
  method: string,
  path: string,
  body?: unknown,
  opts?: { auth?: boolean },
): Promise<T> {
  const headers: Record<string, string> = {}
  const token = getToken()
  if ((opts?.auth ?? method !== 'GET') && token) {
    headers['Authorization'] = `Bearer ${token}`
  }
  if (body !== undefined) {
    headers['Content-Type'] = 'application/json'
  }
  const resp = await fetch(path, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  })
  const text = await resp.text()
  const data = text ? (JSON.parse(text) as unknown) : null
  if (!resp.ok) {
    const err = data as { error?: string; message?: string; current_version?: number } | null
    if (resp.status === 401) {
      unauthorized.value = true
    }
    throw new ApiError(
      resp.status,
      err?.error ?? `http_${resp.status}`,
      err?.message ?? err?.error ?? `HTTP ${resp.status}`,
      err?.current_version,
    )
  }
  return data as T
}

export interface EntryMeta {
  namespace: string
  data_id: string
  data_type: string
  version: number
  tags: Record<string, string>
  created_at: string
  updated_at: string
}

export interface Entry extends EntryMeta {
  content: string
}

export interface HistoryRecord {
  version: number
  op: string // publish | delete
  data_type: string | null
  content: string | null
  tags: Record<string, string> | null
  actor: string | null
  created_at: string
}

export interface Member {
  node_id: string
  addr: string
  suffrage: string
}

export interface ClusterStatus {
  node_id: string
  role: string
  term: number
  leader_id: string
  leader_addr: string
  leader_http_addr: string
  last_applied_index: number
  restoring: boolean
  members: Member[]
}

export function listNamespaces(): Promise<{ namespaces: string[] }> {
  return api('GET', '/v1/namespaces')
}

export function deleteNamespace(ns: string): Promise<{ status: string; entries: number }> {
  return api('DELETE', `/v1/namespaces/${encodeURIComponent(ns)}`)
}

export function listConfigs(
  ns: string,
  limit?: number,
  afterId?: string,
): Promise<{ entries: EntryMeta[]; next_after_id?: string }> {
  const q = new URLSearchParams()
  if (limit !== undefined) q.set('limit', String(limit))
  if (afterId) q.set('after_id', afterId)
  const suffix = q.size ? `?${q}` : ''
  return api('GET', `/v1/namespaces/${encodeURIComponent(ns)}/configs${suffix}`)
}

export function getEntry(ns: string, dataId: string): Promise<Entry> {
  return api('GET', `/v1/namespaces/${encodeURIComponent(ns)}/configs/${encodeURIComponent(dataId)}`)
}

export function publish(
  ns: string,
  dataId: string,
  payload: { content: string; data_type: string; tags?: Record<string, string>; expected_version?: number },
): Promise<{ version: number; created: boolean }> {
  return api('PUT', `/v1/namespaces/${encodeURIComponent(ns)}/configs/${encodeURIComponent(dataId)}`, payload)
}

export function removeEntry(ns: string, dataId: string, expectedVersion: number): Promise<{ version: number }> {
  return api(
    'DELETE',
    `/v1/namespaces/${encodeURIComponent(ns)}/configs/${encodeURIComponent(dataId)}?expected_version=${expectedVersion}`,
  )
}

export function rollback(ns: string, dataId: string, toVersion: number): Promise<{ version: number }> {
  return api('POST', `/v1/namespaces/${encodeURIComponent(ns)}/configs/${encodeURIComponent(dataId)}/rollback`, {
    to_version: toVersion,
  })
}

export function listHistory(
  ns: string,
  dataId: string,
  limit?: number,
  beforeVersion?: number,
): Promise<HistoryRecord[]> {
  const q = new URLSearchParams()
  if (limit !== undefined) q.set('limit', String(limit))
  if (beforeVersion !== undefined) q.set('before_version', String(beforeVersion))
  const suffix = q.size ? `?${q}` : ''
  return api(
    'GET',
    `/v1/namespaces/${encodeURIComponent(ns)}/configs/${encodeURIComponent(dataId)}/versions${suffix}`,
  )
}

export function clusterStatus(): Promise<ClusterStatus> {
  return api('GET', '/v1/cluster/status', undefined, { auth: true })
}

/** 移除成员（DESIGN §5.2）：Leader 执行前做 quorum 护栏校验，409 = 护栏拒绝。 */
export function removePeer(nodeId: string): Promise<{ status: string }> {
  return api('DELETE', `/v1/cluster/peers/${encodeURIComponent(nodeId)}`, undefined, { auth: true })
}

export function fmtTime(iso: string): string {
  return new Date(iso).toLocaleString()
}

/** data_type=JSON 时本地格式校验；返回错误信息，合法返回空串。 */
export function validateContent(dataType: string, content: string): string {
  if (dataType === 'JSON' && content.trim()) {
    try {
      JSON.parse(content)
    } catch (e) {
      return `JSON 格式不合法：${(e as Error).message}`
    }
  }
  return ''
}

/** 基于 CAS 的写请求撞上版本冲突（409）时的统一提示；非冲突错误返回 null。 */
export function conflictMessage(e: unknown): string | null {
  if (e instanceof ApiError && e.code === 'version_conflict') {
    return `版本冲突：当前已是 version=${e.currentVersion}（你基于的版本已过期）。请刷新后重试。`
  }
  return null
}
