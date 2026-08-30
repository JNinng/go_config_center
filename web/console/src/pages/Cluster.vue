<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { ApiError, clusterStatus, removePeer, type ClusterStatus, type Member } from '../api'

const status = ref<ClusterStatus | null>(null)
const error = ref('')
const notice = ref('')
const loading = ref(false)
const removing = ref<string | null>(null)

async function load(): Promise<void> {
  error.value = ''
  notice.value = ''
  loading.value = true
  try {
    status.value = await clusterStatus()
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    loading.value = false
  }
}

onMounted(load)

function roleBadge(role: string, suffrage: string): string {
  if (role === 'leader') return 'badge ok'
  if (suffrage === 'Nonvoter') return 'badge'
  return 'badge accent'
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
    notice.value = `已移除节点 ${m.node_id}`
    await load()
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

  <div v-if="error" class="error">{{ error }}（读取 /v1/cluster/status 需要 admin_token）</div>
  <div v-if="notice" class="hint" style="margin-bottom: 10px">{{ notice }}</div>

  <div v-if="status" class="panel">
    <div class="actions" style="margin-bottom: 8px">
      <span class="badge" :class="status.role === 'leader' ? 'ok' : 'accent'">本节点角色：{{ status.role }}</span>
      <span class="badge">Term {{ status.term }}</span>
      <span v-if="status.restoring" class="badge warn">全量恢复中（读请求 503）</span>
      <button :disabled="loading" @click="load">刷新</button>
    </div>
    <table>
      <tbody>
        <tr><th>本节点 Node ID</th><td class="mono">{{ status.node_id }}</td></tr>
        <tr><th>Leader</th><td class="mono">{{ status.leader_id || '—' }}（{{ status.leader_addr || '地址未知' }}）</td></tr>
        <tr><th>Leader HTTP</th><td class="mono">{{ status.leader_http_addr || '—' }}</td></tr>
        <tr><th>last_applied_index</th><td>{{ status.last_applied_index }}</td></tr>
      </tbody>
    </table>
  </div>

  <div v-if="status" class="panel">
    <strong>成员表</strong>
    <table style="margin-top: 8px">
      <thead>
        <tr>
          <th>Node ID</th>
          <th>Raft 地址</th>
          <th>投票权</th>
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
          <td><span :class="roleBadge(status.role, m.suffrage)">{{ m.suffrage }}</span></td>
          <td class="actions">
            <button class="danger" :disabled="removing !== null" @click="removeMember(m)">
              {{ removing === m.node_id ? '移除中…' : '移除' }}
            </button>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>
