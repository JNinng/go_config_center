<script setup lang="ts">
import { onMounted, ref } from 'vue'
import { clusterStatus, type ClusterStatus } from '../api'

const status = ref<ClusterStatus | null>(null)
const error = ref('')
const loading = ref(false)

async function load(): Promise<void> {
  error.value = ''
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
</script>

<template>
  <h2>集群实例与状态</h2>

  <div v-if="error" class="error">{{ error }}（读取 /v1/cluster/status 需要 admin_token）</div>

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
        </tr>
      </tbody>
    </table>
  </div>
</template>
