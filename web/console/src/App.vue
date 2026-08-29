<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { getToken, setToken, unauthorized, listNamespaces } from './api'
import { route } from './router'
import ConfigList from './pages/ConfigList.vue'
import ConfigDetail from './pages/ConfigDetail.vue'
import Cluster from './pages/Cluster.vue'

const namespaces = ref<string[]>([])
const nsError = ref('')
const tokenInput = ref('')

async function refreshNamespaces(): Promise<void> {
  nsError.value = ''
  try {
    namespaces.value = (await listNamespaces()).namespaces
  } catch (e) {
    nsError.value = String((e as Error).message ?? e)
  }
}

onMounted(refreshNamespaces)
// 从详情页返回等场景下 namespace 清单可能已变化，简单起见每次路由切换后静默刷新。
watch(route, refreshNamespaces)

function saveToken(): void {
  if (tokenInput.value.trim()) {
    setToken(tokenInput.value.trim())
    tokenInput.value = ''
    refreshNamespaces()
  }
}

const isListActive = (ns: string): boolean =>
  (route.value.name === 'list' && route.value.ns === ns) ||
  (route.value.name === 'detail' && route.value.ns === ns)
</script>

<template>
  <nav class="sidebar">
    <h1>配置中心控制台</h1>
    <div class="side-label">Namespace</div>
    <a
      v-for="ns in namespaces"
      :key="ns"
      :href="`#/ns/${encodeURIComponent(ns)}`"
      :class="{ active: isListActive(ns) }"
      >{{ ns }}</a
    >
    <div v-if="nsError" class="error" style="margin: 8px 16px">{{ nsError }}</div>
    <div class="side-label">集群</div>
    <a href="#/cluster" :class="{ active: route.name === 'cluster' }">实例与状态</a>
  </nav>

  <main class="main">
    <div v-if="!getToken() || unauthorized" class="panel">
      <div v-if="unauthorized" class="error">认证失败或 token 缺失（401），请重新提供 admin_token。</div>
      <label>admin_token（写操作与集群状态必需，保存在浏览器 localStorage）</label>
      <div class="token-bar">
        <input v-model="tokenInput" type="password" placeholder="admin_token" @keyup.enter="saveToken" />
        <button class="primary" @click="saveToken">保存</button>
      </div>
    </div>

    <ConfigList v-if="route.name === 'list'" :ns="route.ns" />
    <ConfigDetail v-else-if="route.name === 'detail'" :ns="route.ns" :data-id="route.dataId" />
    <Cluster v-else-if="route.name === 'cluster'" />
    <div v-else class="panel hint">从左侧选择 Namespace 浏览配置；Namespace 随首次 Publish 自动创建。</div>
  </main>
</template>
