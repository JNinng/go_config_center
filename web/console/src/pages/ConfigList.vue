<script setup lang="ts">
import { onMounted, ref, watch } from 'vue'
import { ApiError, fmtTime, listConfigs, publish, validateContent, type EntryMeta } from '../api'
import { goDetail } from '../router'

const props = defineProps<{ ns: string }>()

const entries = ref<EntryMeta[]>([])
const nextAfter = ref<string | undefined>(undefined)
const error = ref('')
const notice = ref('')
const loading = ref(false)

const PAGE = 100

async function load(reset: boolean): Promise<void> {
  error.value = ''
  notice.value = ''
  loading.value = true
  try {
    const page = await listConfigs(props.ns, PAGE, reset ? undefined : nextAfter.value)
    if (reset) {
      entries.value = page.entries
    } else {
      entries.value = entries.value.concat(page.entries)
    }
    nextAfter.value = page.next_after_id
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    loading.value = false
  }
}

onMounted(() => load(true))
watch(
  () => props.ns,
  () => {
    showCreate.value = false
    load(true)
  },
)

// 新建配置：expected_version=0 即 create-if-absent，已存在时 409。
const showCreate = ref(false)
const newDataId = ref('')
const newDataType = ref('TEXT')
const newContent = ref('')
const creating = ref(false)
const createError = ref('')

async function create(): Promise<void> {
  createError.value = ''
  const invalid = validateContent(newDataType.value, newContent.value)
  if (invalid) {
    createError.value = invalid
    return
  }
  if (!newDataId.value.trim()) {
    createError.value = 'DataId 不能为空'
    return
  }
  creating.value = true
  try {
    const id = newDataId.value.trim()
    const dataType = newDataType.value
    const res = await publish(props.ns, id, {
      content: newContent.value,
      data_type: dataType,
      expected_version: 0,
    })
    showCreate.value = false
    newDataId.value = ''
    newContent.value = ''
    notice.value = `已创建 version=${res.version}`
    await load(true)
    // 读己之写（ADR-0003）：Follower 本机列表可能未追平，以写响应把新条目并入列表，
    // 避免"创建成功却看不到"。
    if (!entries.value.some((e) => e.data_id === id)) {
      entries.value.push({
        namespace: props.ns,
        data_id: id,
        data_type: dataType,
        version: res.version,
        tags: {},
        created_at: new Date().toISOString(),
        updated_at: new Date().toISOString(),
      })
      entries.value.sort((a, b) => a.data_id.localeCompare(b.data_id))
    }
  } catch (e) {
    if (e instanceof ApiError && e.code === 'version_conflict') {
      createError.value = `条目已存在（当前版本 ${e.currentVersion}）。更新请从条目详情页操作。`
    } else {
      createError.value = (e as Error).message
    }
  } finally {
    creating.value = false
  }
}

</script>

<template>
  <h2>Namespace「{{ ns }}」的配置</h2>

  <div v-if="error" class="error">{{ error }}</div>
  <div v-if="notice" class="hint">{{ notice }}</div>

  <div class="panel">
    <div class="actions">
      <button class="primary" @click="showCreate = !showCreate">{{ showCreate ? '收起新建' : '新建配置' }}</button>
      <button :disabled="loading" @click="load(true)">刷新</button>
      <span v-if="loading" class="hint">加载中…</span>
    </div>

    <div v-if="showCreate" style="margin-top: 12px">
      <label>DataId</label>
      <input v-model="newDataId" placeholder="如 db.password" />
      <label>数据类型</label>
      <select v-model="newDataType">
        <option>TEXT</option>
        <option>JSON</option>
        <option>YAML</option>
      </select>
      <label>内容</label>
      <textarea v-model="newContent" placeholder="配置内容"></textarea>
      <div v-if="createError" class="error">{{ createError }}</div>
      <div class="actions" style="margin-top: 10px">
        <button class="primary" :disabled="creating" @click="create">{{ creating ? '发布中…' : '发布（create-if-absent）' }}</button>
        <span class="hint">以 expected_version=0 创建，条目已存在时会被拒绝</span>
      </div>
    </div>
  </div>

  <div class="panel">
    <table>
      <thead>
        <tr>
          <th>DataId</th>
          <th>类型</th>
          <th>Version</th>
          <th>标签</th>
          <th>更新时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="e in entries" :key="e.data_id">
          <td>
            <a href="#" class="mono" @click.prevent="goDetail(ns, e.data_id)">{{ e.data_id }}</a>
          </td>
          <td>{{ e.data_type }}</td>
          <td>{{ e.version }}</td>
          <td>
            <span v-for="(v, k) in e.tags" :key="k" class="badge" style="margin-right: 4px">{{ k }}={{ v }}</span>
          </td>
          <td class="hint">{{ fmtTime(e.updated_at) }}</td>
        </tr>
        <tr v-if="!entries.length && !loading">
          <td colspan="5" class="hint">
            此 Namespace 暂无存活条目。全新 Namespace 请点击「新建配置」创建第一条
            （Namespace 随首次 Publish 隐式创建）；已删除的条目可在 History 中追溯。
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="nextAfter" class="actions" style="margin-top: 10px">
      <button @click="load(false)">加载更多</button>
    </div>
  </div>
</template>
