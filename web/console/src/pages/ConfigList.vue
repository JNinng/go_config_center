<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { ApiError, fmtRelTime, fmtTime, listConfigs, publish, validateContent, type EntryMeta } from '../api'

const props = defineProps<{ ns: string }>()

const entries = ref<EntryMeta[]>([])
const nextAfter = ref<string | undefined>(undefined)
const error = ref('')
const notice = ref('')
const loading = ref(false)

const PAGE = 100

// load 不清 notice：写操作后紧跟 reload 以读己之写校准列表，成功提示必须存活。
async function load(reset: boolean): Promise<void> {
  error.value = ''
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
    notice.value = ''
    showCreate.value = false
    newDataId.value = ''
    newDataType.value = 'TEXT'
    newContent.value = ''
    createError.value = ''
    load(true)
  },
)

// 按 DataId 子串过滤（不区分大小写），只作用于已加载条目。
const filter = ref('')
const filtered = computed(() => {
  const q = filter.value.trim().toLowerCase()
  if (!q) return entries.value
  return entries.value.filter((e) => e.data_id.toLowerCase().includes(q))
})

// 新建配置：expected_version=0 即 create-if-absent，已存在时 409。
const showCreate = ref(false)
const newDataId = ref('')
const newDataType = ref('TEXT')
const newContent = ref('')
const creating = ref(false)
const createError = ref('')

async function create(): Promise<void> {
  createError.value = ''
  const id = newDataId.value.trim()
  if (!id) {
    createError.value = 'DataId 不能为空'
    return
  }
  const invalid = validateContent(newDataType.value, newContent.value)
  if (invalid) {
    createError.value = invalid
    return
  }
  creating.value = true
  try {
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
  <div v-if="notice" class="notice">{{ notice }}</div>

  <div class="panel">
    <div class="actions">
      <button class="primary" @click="showCreate = !showCreate">{{ showCreate ? '收起新建' : '新建配置' }}</button>
      <button :disabled="loading" @click="load(true)">刷新</button>
      <span v-if="loading" class="hint">加载中…</span>
    </div>

    <div v-if="showCreate" class="form" style="margin-top: 12px">
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

  <div class="panel flex-fill">
    <div class="actions" style="margin-bottom: 8px">
      <input v-model="filter" placeholder="按 DataId 过滤（仅已加载条目）" style="flex: 1; max-width: 320px" />
      <span v-if="filter" class="hint">{{ filtered.length }} / {{ entries.length }} 条</span>
    </div>
    <div class="table-wrap">
      <table class="table-hover">
        <thead>
          <tr>
            <th>DataId</th>
            <th>类型</th>
            <th class="num">Version</th>
            <th>标签</th>
            <th>更新时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="e in filtered" :key="e.data_id">
            <td>
              <a :href="`#/ns/${encodeURIComponent(ns)}/${encodeURIComponent(e.data_id)}`" class="mono cell-link">{{ e.data_id }}</a>
            </td>
            <td>{{ e.data_type }}</td>
            <td class="num">{{ e.version }}</td>
            <td>
              <span v-for="(v, k) in e.tags" :key="k" class="badge" style="margin-right: 4px">{{ k }}={{ v }}</span>
            </td>
            <td>
              {{ fmtRelTime(e.updated_at) }}
              <span class="hint">{{ fmtTime(e.updated_at) }}</span>
            </td>
          </tr>
          <tr v-if="!filtered.length && !loading">
            <td colspan="5" class="hint">
              <template v-if="entries.length">没有匹配过滤条件的条目。</template>
              <template v-else>
                此 Namespace 暂无存活条目。全新 Namespace 请点击「新建配置」创建第一条
                （Namespace 随首次 Publish 隐式创建）；已删除的条目可在 History 中追溯。
              </template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="nextAfter" class="actions" style="margin-top: 10px">
      <button @click="load(false)">加载更多</button>
    </div>
  </div>
</template>
