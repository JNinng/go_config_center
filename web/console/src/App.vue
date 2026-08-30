<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { clearToken, hasToken, setToken, unauthorized, deleteNamespace, listNamespaces } from './api'
import { route } from './router'
import ConfigList from './pages/ConfigList.vue'
import ConfigDetail from './pages/ConfigDetail.vue'
import Cluster from './pages/Cluster.vue'

const namespaces = ref<string[]>([])
const nsError = ref('')
// 侧栏操作的成功反馈与错误分通道展示，成功不冒充报错样式。
const nsNotice = ref('')
const tokenInput = ref('')
const showNewNs = ref(false)
const newNs = ref('')

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

// 新增 Namespace：Namespace 为隐式创建（首次 Publish 生效，DESIGN §5.1），
// 此处仅校验命名并跳转到新列表页，由"新建配置"创建第一条。
function createNs(): void {
  const ns = newNs.value.trim()
  if (!ns) return
  if (ns.length > 64) {
    nsError.value = 'Namespace 长度不能超过 64'
    return
  }
  if (/[/\\\s]/.test(ns)) {
    nsError.value = 'Namespace 不能包含空白、/ 或 \\'
    return
  }
  newNs.value = ''
  showNewNs.value = false
  location.hash = `#/ns/${encodeURIComponent(ns)}`
}

// 删除 Namespace：原子删除其下全部存活条目（各写 Tombstone，可逐条回滚）。
// 输入 Namespace 名二次确认，防止误删。
async function removeNs(ns: string): Promise<void> {
  const typed = prompt(
    `删除 Namespace「${ns}」将删除其下全部配置（各写 Tombstone，History 保留可逐条回滚）。\n输入 Namespace 名确认：`,
    '',
  )
  if (typed === null) return
  if (typed.trim() !== ns) {
    nsError.value = '输入不一致，已取消'
    return
  }
  nsNotice.value = ''
  try {
    const res = await deleteNamespace(ns)
    if ((route.value.name === 'list' || route.value.name === 'detail') && route.value.ns === ns) {
      location.hash = '#/'
    }
    await refreshNamespaces()
    nsNotice.value = `已删除「${ns}」（${res.entries} 条，可回滚）`
  } catch (e) {
    nsError.value = (e as Error).message
  }
}

function saveToken(): void {
  if (tokenInput.value.trim()) {
    setToken(tokenInput.value.trim())
    tokenInput.value = ''
    refreshNamespaces()
  }
}

// 更换 token：清除已保存值，主区重新出现输入面板。
function resetToken(): void {
  clearToken()
  tokenInput.value = ''
}

const isListActive = (ns: string): boolean =>
  (route.value.name === 'list' && route.value.ns === ns) ||
  (route.value.name === 'detail' && route.value.ns === ns)
</script>

<template>
  <nav class="sidebar">
    <h1>配置中心控制台</h1>
    <div class="side-label">
      Namespace
      <button class="side-add" title="新建 Namespace" @click="showNewNs = !showNewNs">＋</button>
    </div>
    <div v-if="showNewNs" class="side-new">
      <input v-model="newNs" placeholder="新 Namespace 名" @keyup.enter="createNs" />
      <button class="primary" @click="createNs">创建</button>
    </div>
    <div v-for="ns in namespaces" :key="ns" class="ns-row">
      <a :href="`#/ns/${encodeURIComponent(ns)}`" :class="{ active: isListActive(ns) }">{{ ns }}</a>
      <button class="ns-del" title="删除 Namespace" @click="removeNs(ns)">×</button>
    </div>
    <div v-if="nsError" class="error" style="margin: 8px 16px">{{ nsError }}</div>
    <div v-if="nsNotice" class="hint" style="margin: 8px 16px; overflow-wrap: anywhere">{{ nsNotice }}</div>
    <div class="side-label">集群</div>
    <a href="#/cluster" :class="{ active: route.name === 'cluster' }">实例与状态</a>
    <div class="side-label">管理</div>
    <button class="side-token" title="清除已保存的 token 并重新输入" @click="resetToken">更换 admin_token</button>
  </nav>

  <main class="main">
    <div v-if="!hasToken || unauthorized" class="panel">
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
