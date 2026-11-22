<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { ApiError, clusterStatus, removePeer, type ClusterStatus, type Member } from '../api'

const status = ref<ClusterStatus | null>(null)
const error = ref('')
const notice = ref('')
const loading = ref(false)
const removing = ref<string | null>(null)

// load 不清 notice（移除成功等提示要存活）；错误每次轮询重置，瞬态故障自动消隐。
// 进行中的请求期间跳过轮询，避免并发刷新。
async function load(): Promise<void> {
  if (loading.value) return
  error.value = ''
  loading.value = true
  try {
    status.value = await clusterStatus()
  } catch (e) {
    if (e instanceof ApiError && e.status === 401) {
      error.value = '读取 /v1/cluster/status 需要 admin_token，请在左侧提供。'
    } else {
      error.value = (e as Error).message
    }
  } finally {
    loading.value = false
  }
}

let pollTimer: number | undefined

onMounted(() => {
  load()
  // 状态页 5s 轮询：Raft 角色/Leader 变更无需手动刷新。
  pollTimer = window.setInterval(load, 5000)
})
onUnmounted(() => window.clearInterval(pollTimer))

// 投票权徽章只看 suffrage（本节点角色徽章已在状态卡单独展示）。
function roleBadge(suffrage: string): string {
  if (suffrage === 'Nonvoter') return 'badge'
  return 'badge accent'
}

// last_contact_ms 语义见 api.go：-1=从未收到心跳；0（Leader 自身或未知）经 omitempty 到前端均为 undefined，同样按 — 展示。
function lastContact(ms?: number): string {
  if (ms === undefined || ms === 0) return '—'
  if (ms < 0) return '从未'
  return ms < 1000 ? `${ms}ms 前` : `${(ms / 1000).toFixed(1)}s 前`
}

// 复制滞后 = 日志末端 - 已提交；FSM 滞后 = 已提交 - 已应用。任一端未知则无法计算。
function lag(from?: number, to?: number): string {
  return from === undefined || to === undefined ? '—' : String(from - to)
}

function onlineBadge(m: Member): string {
  if (m.online === undefined) return 'badge'
  return m.online ? 'badge ok' : 'badge danger'
}

// 移除成员（DESIGN §4.5）：写门闸将请求 307 到 Leader 执行；
// 护栏不满足返回 409，被移除节点停止接收复制并在约 2s 内以退出码 42 退出。
async function removeMember(m: Member): Promise<void> {
  const hints: string[] = []
  if (m.node_id === status.value?.leader_id) {
    hints.push('该节点是 Leader：移除后集群将重新选举。')
  }
  if (m.node_id === status.value?.node_id) {
    hints.push('这是本节点：移除后本页面所在进程将退出。')
  }
  const hintText = hints.length ? hints.join('\n') + '\n' : ''
  if (
    !confirm(
      `移除节点 ${m.node_id}（${m.addr}）？\n` +
        '该节点将停止接收复制，进程约 2 秒内以退出码 42 退出（不自动重启）。\n' +
        hintText +
        '恢复上线需清空该节点 data_dir 后重新 Join（DESIGN §4.5）。',
    )
  ) {
    return
  }
  removing.value = m.node_id
  try {
    await removePeer(m.node_id)
    await load()
    notice.value = `已移除节点 ${m.node_id}`
  } catch (e) {
    if (e instanceof ApiError && e.code === 'quorum_guard') {
      error.value = '护栏拒绝：移除后剩余在线 voter 将不足 quorum（DESIGN §4.5）。请先恢复离线节点再试。'
    } else if (e instanceof ApiError && e.code === 'not_member') {
      error.value = '该节点已不在成员表中'
      await load()
    } else {
      error.value = (e as Error).message
    }
  } finally {
    removing.value = null
  }
}
</script>

<template>
  <h2>集群实例与状态</h2>

  <div v-if="error" class="error">{{ error }}</div>
  <div v-if="notice" class="notice" style="margin-bottom: 10px">{{ notice }}</div>

  <div v-if="status" class="panel">
    <div class="actions" style="margin-bottom: 8px">
      <span class="badge" :class="status.role === 'leader' ? 'ok' : 'accent'">本节点角色：{{ status.role }}</span>
      <span class="badge">Term {{ status.term }}</span>
      <span v-if="status.restoring" class="badge warn">全量恢复中（读请求 503）</span>
      <span class="hint">每 5 秒自动刷新</span>
      <button style="margin-left: auto" :disabled="loading" @click="load">刷新</button>
    </div>
    <div class="table-wrap">
      <table>
        <tbody>
          <tr><th>本节点 Node ID</th><td class="mono">{{ status.node_id }}</td></tr>
          <tr><th>Leader</th><td class="mono">{{ status.leader_id || '—' }}（{{ status.leader_addr || '地址未知' }}）</td></tr>
          <tr><th>Leader HTTP</th><td class="mono">{{ status.leader_http_addr || '—' }}</td></tr>
          <tr><th>last_applied_index</th><td class="num">{{ status.last_applied_index }}</td></tr>
          <tr><th>last_log_index</th><td class="num">{{ status.last_log_index ?? '—' }}</td></tr>
          <tr><th>commit_index</th><td class="num">{{ status.commit_index ?? '—' }}</td></tr>
          <tr><th>最近心跳（→ Leader）</th><td>{{ lastContact(status.last_contact_ms) }}</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div v-if="status" class="panel flex-fill">
    <div class="actions" style="margin-bottom: 8px">
      <strong>成员表</strong>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Node ID</th>
            <th>Raft 地址</th>
            <th>投票权</th>
            <th>状态</th>
            <th class="num">Term</th>
            <th class="num">已应用</th>
            <th class="num">已提交</th>
            <th class="num">日志末端</th>
            <th class="num">复制滞后</th>
            <th class="num">FSM 滞后</th>
            <th>最近心跳</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="m in status.members" :key="m.node_id">
            <td class="mono">
              {{ m.node_id }}
              <span v-if="m.node_id === status.leader_id" class="badge ok">Leader</span>
              <span v-if="m.node_id === status.node_id" class="badge">本节点</span>
            </td>
            <td class="mono">{{ m.addr }}</td>
            <td><span :class="roleBadge(m.suffrage)">{{ m.suffrage }}</span></td>
            <td>
              <span :class="onlineBadge(m)">{{ m.online === undefined ? '未知' : m.online ? '在线' : '无响应' }}</span>
              <span v-if="m.role" class="badge" style="margin-left: 4px">{{ m.role }}</span>
            </td>
            <td class="num">{{ m.term || '—' }}</td>
            <td class="num">{{ m.applied_index ?? '—' }}</td>
            <td class="num">{{ m.commit_index ?? '—' }}</td>
            <td class="num">{{ m.last_log_index ?? '—' }}</td>
            <td class="num">{{ lag(m.last_log_index, m.commit_index) }}</td>
            <td class="num">{{ lag(m.commit_index, m.applied_index) }}</td>
            <td>{{ lastContact(m.last_contact_ms) }}</td>
            <td class="cell-actions">
              <button class="danger btn-sm" :disabled="removing !== null" @click="removeMember(m)">
                {{ removing === m.node_id ? '移除中…' : '移除' }}
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
