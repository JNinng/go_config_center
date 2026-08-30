<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import {
  ApiError,
  conflictMessage,
  fmtRelTime,
  fmtTime,
  getEntry,
  listHistory,
  publish,
  removeEntry,
  rollback,
  validateContent,
  type Entry,
  type HistoryRecord,
} from '../api'
import { diffLines } from 'diff'

const props = defineProps<{ ns: string; dataId: string }>()

const entry = ref<Entry | null>(null)
const error = ref('')
const notice = ref('')
const loading = ref(false)

// —— 编辑与发布（CAS：expected_version 取载入时的当前版本，冲突时提示刷新） ——
const content = ref('')
const dataType = ref('TEXT')
const tagRows = ref<{ key: string; value: string }[]>([])
const saving = ref(false)
const saveError = ref('')

// loadEntry 不清 notice：发布/回滚后紧跟 reload 以读己之写校准，成功提示必须存活；
// 切换条目时由 watch 显式清掉旧提示。
function loadEntry(): Promise<void> {
  error.value = ''
  loading.value = true
  return getEntry(props.ns, props.dataId)
    .then((e) => {
      entry.value = e
      content.value = e.content
      dataType.value = e.data_type
      tagRows.value = Object.entries(e.tags ?? {}).map(([key, value]) => ({ key, value }))
    })
    .catch((e) => {
      error.value = (e as Error).message
    })
    .finally(() => {
      loading.value = false
    })
}

onMounted(loadEntry)
watch(
  () => [props.ns, props.dataId],
  () => {
    notice.value = ''
    showHistoryView.value = false
    viewVersion.value = null
    showHistoryPanel.value = false
    loadEntry()
  },
)

// 键序无关的标签签名：dirty 比较不因行顺序变化误报。
function stableTags(tags: Record<string, string>): string {
  return JSON.stringify(Object.keys(tags).sort().map((k) => [k, tags[k]]))
}

function collectTags(): Record<string, string> {
  const tags: Record<string, string> = {}
  for (const row of tagRows.value) {
    if (row.key.trim()) tags[row.key.trim()] = row.value
  }
  return tags
}

const dirty = computed(
  () =>
    entry.value !== null &&
    (content.value !== entry.value.content ||
      dataType.value !== entry.value.data_type ||
      stableTags(collectTags()) !== stableTags(entry.value.tags ?? {})),
)

async function save(): Promise<void> {
  saveError.value = ''
  if (!entry.value) return
  const invalid = validateContent(dataType.value, content.value)
  if (invalid) {
    saveError.value = invalid
    return
  }
  const keys = tagRows.value.map((r) => r.key.trim()).filter(Boolean)
  const dup = keys.find((k, i) => keys.indexOf(k) !== i)
  if (dup) {
    saveError.value = `标签键重复：${dup}`
    return
  }
  saving.value = true
  try {
    const saved = { content: content.value, data_type: dataType.value, tags: collectTags() }
    const res = await publish(props.ns, props.dataId, {
      ...saved,
      expected_version: entry.value.version,
    })
    notice.value = `已发布 version=${res.version}`
    // 读己之写（ADR-0003）：Follower 本机 GET 允许落后，若未追平本次写入，
    // 以写响应 Version 校准 CAS 基线并恢复刚发布的内容，避免下一次发布撞旧版本冲突。
    await loadEntry()
    if (entry.value && res.version > entry.value.version) {
      entry.value = { ...entry.value, ...saved, version: res.version }
      content.value = saved.content
      dataType.value = saved.data_type
      tagRows.value = Object.entries(saved.tags).map(([key, value]) => ({ key, value }))
    }
    await loadHistory(true)
  } catch (e) {
    saveError.value = conflictMessage(e) ?? (e as Error).message
  } finally {
    saving.value = false
  }
}

async function remove(): Promise<void> {
  saveError.value = ''
  if (!entry.value) return
  if (!confirm(`删除 ${props.ns}/${props.dataId}？将写入 Tombstone 并消耗版本号（可回滚重建）。`)) return
  try {
    const res = await removeEntry(props.ns, props.dataId, entry.value.version)
    notice.value = `已删除（Tombstone version=${res.version}）`
    entry.value = null
    await loadHistory(true)
  } catch (e) {
    saveError.value = conflictMessage(e) ?? (e as Error).message
  }
}

// —— History、diff 与回滚 ——
const history = ref<HistoryRecord[]>([])
const histLoading = ref(false)
const beforeCursor = ref<number | undefined>(undefined)
const PAGE = 50

const selected = ref<number[]>([])

// History 表格面板显隐：收起时编辑区独占整页；首次展开时若尚未加载则自动拉取。
const showHistoryPanel = ref(false)

async function toggleHistoryPanel(): Promise<void> {
  showHistoryPanel.value = !showHistoryPanel.value
  if (showHistoryPanel.value && !history.value.length) await loadHistory(true)
}

async function loadHistory(reset: boolean): Promise<void> {
  histLoading.value = true
  try {
    const records = await listHistory(props.ns, props.dataId, PAGE, reset ? undefined : beforeCursor.value)
    if (reset) {
      history.value = records
    } else {
      history.value = history.value.concat(records)
    }
    const last = history.value[history.value.length - 1]
    beforeCursor.value = last ? last.version : undefined
  } catch (e) {
    error.value = (e as Error).message
  } finally {
    histLoading.value = false
  }
}

// entry 为空（Tombstone/不存在）时以 History 最新记录近似当前版本（History 倒序），
// 回滚确认框的新版本号才准确。
const currentVersion = computed(() => entry.value?.version ?? history.value[0]?.version ?? 0)

function toggleSelect(version: number): void {
  const i = selected.value.indexOf(version)
  if (i >= 0) {
    selected.value.splice(i, 1)
  } else {
    if (selected.value.length >= 2) selected.value.shift()
    selected.value.push(version)
  }
}

const diffResult = ref<{ added: boolean; removed: boolean; value: string }[] | null>(null)
const diffError = ref('')

const contentByVersion = computed(() => {
  const m = new Map<number, string>()
  for (const r of history.value) {
    if (r.content !== null) m.set(r.version, r.content)
  }
  if (entry.value) m.set(entry.value.version, entry.value.content)
  return m
})

// —— 历史内容快览（页首右侧入口）：免滚到下方 History 表即可查看任一版本内容 ——
const showHistoryView = ref(false)
const viewVersion = ref<number | null>(null)

const viewableVersions = computed(() => history.value.filter((r) => r.content !== null))
const viewRecord = computed(() => history.value.find((r) => r.version === viewVersion.value) ?? null)
const viewContent = computed(() =>
  viewVersion.value === null ? null : (contentByVersion.value.get(viewVersion.value) ?? null),
)

async function toggleHistoryView(): Promise<void> {
  showHistoryView.value = !showHistoryView.value
  if (!showHistoryView.value) return
  // History 惰性加载：首次打开时拉取；已选版本不可查看（Tombstone/未加载）时定位到最新可查看版本。
  if (!history.value.length) await loadHistory(true)
  if (!viewableVersions.value.some((r) => r.version === viewVersion.value)) {
    viewVersion.value = viewableVersions.value[0]?.version ?? null
  }
}

function showDiff(): void {
  diffError.value = ''
  diffResult.value = null
  if (selected.value.length !== 2) {
    diffError.value = '先在列表中勾选两个版本（最多选两个）'
    return
  }
  const [a, b] = [...selected.value].sort((x, y) => x - y)
  const ca = contentByVersion.value.get(a)
  const cb = contentByVersion.value.get(b)
  if (ca === undefined || cb === undefined) {
    diffError.value = '所选版本缺少内容（Tombstone 不可作为对比内容），请先加载更多历史'
    return
  }
  diffResult.value = diffLines(ca, cb).map((part) => ({
    added: part.added ?? false,
    removed: part.removed ?? false,
    value: part.value,
  }))
}

async function doRollback(toVersion: number): Promise<void> {
  saveError.value = ''
  if (!confirm(`回滚到 version=${toVersion}？将以该版本内容发布一个新版本（version=${currentVersion.value + 1}）。`)) return
  try {
    const res = await rollback(props.ns, props.dataId, toVersion)
    notice.value = `已回滚，新版本 version=${res.version}`
    selected.value = []
    diffResult.value = null
    // 读己之写（ADR-0003）：Follower 本机 GET 可能未追平；回滚即把 to_version
    // 的 History 记录作为新版本发布（DESIGN §4.2），未追平时以该记录与写响应校准视图。
    await loadEntry()
    if (entry.value && res.version > entry.value.version) {
      const rec = history.value.find((r) => r.version === toVersion)
      const tags = rec?.tags ?? {}
      entry.value = {
        ...entry.value,
        data_type: rec?.data_type ?? entry.value.data_type,
        content: rec?.content ?? entry.value.content,
        tags,
        version: res.version,
      }
      content.value = entry.value.content
      dataType.value = entry.value.data_type
      tagRows.value = Object.entries(tags).map(([key, value]) => ({ key, value }))
    }
    await loadHistory(true)
  } catch (e) {
    if (e instanceof ApiError && e.code === 'invalid_rollback') {
      saveError.value = `回滚目标无效（当前版本 ${e.currentVersion}）。请刷新后重试。`
    } else {
      saveError.value = (e as Error).message
    }
  }
}

function opBadge(op: string): string {
  return op === 'delete' ? 'badge danger' : 'badge accent'
}
</script>

<template>
  <div class="detail-head">
    <h2 class="crumbs">
      <a :href="`#/ns/${encodeURIComponent(ns)}`">{{ ns }}</a>
      <span class="crumb-sep">/</span>
      <span class="mono">{{ dataId }}</span>
    </h2>
    <button style="margin-left: auto" :disabled="loading" @click="toggleHistoryView">历史内容</button>
    <button :disabled="loading" @click="toggleHistoryPanel">历史记录</button>
  </div>

  <div v-if="error" class="error">{{ error }}</div>
  <div v-if="notice" class="notice" style="margin-bottom: 10px">{{ notice }}</div>

  <div v-if="!entry && !loading" class="panel hint">
    条目不存在或已删除（Tombstone）。删除后可在 History 中选择删除前的版本回滚重建。
    <div class="actions" style="margin-top: 10px"><button @click="loadHistory(true)">加载 History</button></div>
  </div>

  <div v-if="entry || showHistoryView" class="detail-body">
    <div v-if="entry" class="panel form">
      <div class="actions" style="margin-bottom: 8px">
        <span class="badge" :class="dirty ? 'warn' : 'ok'">{{ dirty ? '有未发布修改' : '与当前版本一致' }}</span>
        <span class="badge">当前 Version {{ entry.version }}</span>
        <span class="badge" :title="fmtTime(entry.created_at)">创建于 {{ fmtRelTime(entry.created_at) }}</span>
        <span class="badge" :title="fmtTime(entry.updated_at)">更新于 {{ fmtRelTime(entry.updated_at) }}</span>
        <button class="danger" style="margin-left: auto" @click="remove">删除…</button>
      </div>

      <label>数据类型</label>
      <select v-model="dataType">
        <option>TEXT</option>
        <option>JSON</option>
        <option>YAML</option>
      </select>

      <label>内容</label>
      <textarea v-model="content"></textarea>

      <label>标签（Publish 为全量替换，留空即清除全部标签）</label>
      <div v-for="(row, i) in tagRows" :key="i" class="actions tag-row" style="margin-bottom: 6px">
        <input v-model="row.key" placeholder="键" />
        <input v-model="row.value" placeholder="值" />
        <button @click="tagRows.splice(i, 1)">移除</button>
      </div>
      <button @click="tagRows.push({ key: '', value: '' })">添加标签</button>

      <div v-if="saveError" class="error">{{ saveError }}</div>
      <div class="actions" style="margin-top: 12px">
        <button class="primary" :disabled="saving || !dirty" @click="save">
          {{ saving ? '发布中…' : `发布（expected_version=${entry.version}）` }}
        </button>
        <button :disabled="!dirty" @click="loadEntry">放弃修改</button>
      </div>
    </div>

    <div v-if="showHistoryView" class="panel detail-side">
      <div class="actions" style="margin-bottom: 8px">
        <strong>历史配置内容</strong>
        <select v-model="viewVersion">
          <option v-for="r in viewableVersions" :key="r.version" :value="r.version">version={{ r.version }}（{{ r.op }}）</option>
        </select>
        <span v-if="viewRecord" class="hint">{{ viewRecord.data_type ?? '—' }} · {{ fmtRelTime(viewRecord.created_at) }}（{{ fmtTime(viewRecord.created_at) }}） · 操作者 {{ viewRecord.actor ?? '—' }}</span>
        <button style="margin-left: auto" @click="showHistoryView = false">收起</button>
      </div>
      <pre v-if="viewContent !== null" class="content">{{ viewContent }}</pre>
      <div v-else class="hint">该版本无内容（Tombstone 记录）。</div>
    </div>
  </div>

  <div v-if="showHistoryPanel" class="panel flex-fill">
    <div class="actions" style="margin-bottom: 8px">
      <strong>History</strong>
      <span class="hint">勾选两个版本后「对比」；倒序分页，含 Tombstone</span>
      <button style="margin-left: auto" :disabled="histLoading" @click="loadHistory(true)">刷新</button>
      <button :disabled="selected.length !== 2" @click="showDiff">对比选中版本</button>
      <button @click="showHistoryPanel = false">收起</button>
    </div>

    <div class="table-wrap">
      <table class="table-hover">
        <thead>
          <tr>
            <th>选</th>
            <th class="num">Version</th>
            <th>操作</th>
            <th>类型</th>
            <th>操作者</th>
            <th>时间</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in history" :key="r.version">
            <td class="cell-select">
              <label>
                <input type="checkbox" :checked="selected.includes(r.version)" :disabled="r.content === null" @change="toggleSelect(r.version)" />
              </label>
            </td>
            <td class="num cell-version" :class="{ clickable: r.content !== null }" @click="r.content !== null && toggleSelect(r.version)">{{ r.version }}</td>
            <td><span :class="opBadge(r.op)">{{ r.op }}</span></td>
            <td>{{ r.data_type ?? '—' }}</td>
            <td>{{ r.actor ?? '—' }}</td>
            <td>
              {{ fmtRelTime(r.created_at) }}
              <span class="hint">{{ fmtTime(r.created_at) }}</span>
            </td>
            <td class="cell-actions">
              <button v-if="r.content !== null" class="btn-sm" @click="doRollback(r.version)">回滚到此版</button>
            </td>
          </tr>
          <tr v-if="!history.length && !histLoading">
            <td colspan="7" class="hint">暂无记录</td>
          </tr>
        </tbody>
      </table>
    </div>
    <div v-if="history.length >= PAGE && beforeCursor" class="actions" style="margin-top: 10px">
      <button :disabled="histLoading" @click="loadHistory(false)">加载更多</button>
    </div>

    <div v-if="diffError" class="error">{{ diffError }}</div>
    <div v-if="diffResult" style="margin-top: 12px">
      <strong>对比 version={{ [...selected].sort((a, b) => a - b)[0] }} → version={{ [...selected].sort((a, b) => a - b)[1] }}</strong>
      <pre class="content"><span
        v-for="(part, i) in diffResult"
        :key="i"
      ><span
        v-for="(line, j) in part.value.split('\n').filter((_, idx, arr) => idx < arr.length - 1 || arr[arr.length - 1] !== '')"
        :key="j"
        class="diff-line"
        :class="{ 'diff-add': part.added, 'diff-del': part.removed }"
      >{{ part.added ? '+' : part.removed ? '-' : ' ' }}{{ line }}
</span></span></pre>
    </div>
  </div>
</template>
