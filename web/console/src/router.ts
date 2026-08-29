// hash 路由：#/ns/{ns} 列表、#/ns/{ns}/{dataId} 详情、#/cluster 集群。
// 不引路由库——三个页面用一个可辨识联合足够，也免去服务端 history fallback 配合。
import { ref } from 'vue'

export type Route =
  | { name: 'list'; ns: string }
  | { name: 'detail'; ns: string; dataId: string }
  | { name: 'cluster' }
  | { name: 'home' }

export const route = ref<Route>({ name: 'home' })

export function goDetail(ns: string, dataId: string): void {
  location.hash = `#/ns/${encodeURIComponent(ns)}/${encodeURIComponent(dataId)}`
}

function parse(): Route {
  const parts = location.hash.replace(/^#\/?/, '').split('/').map(decodeURIComponent)
  if (parts[0] === 'cluster') return { name: 'cluster' }
  if (parts[0] === 'ns' && parts[1]) {
    if (parts[2]) return { name: 'detail', ns: parts[1], dataId: parts[2] }
    return { name: 'list', ns: parts[1] }
  }
  return { name: 'home' }
}

// 由 main.ts 调用一次；页面导航用原生 href（#/...），不另设跳转函数。
export function initRouter(): void {
  const update = () => {
    route.value = parse()
  }
  window.addEventListener('hashchange', update)
  update()
}
