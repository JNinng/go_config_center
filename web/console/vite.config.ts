import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 构建产物由 scripts/build-console.sh 拷入 internal/console/dist 内嵌（ADR-0012）。
// 开发时 vite dev server 将 API 代理到本机节点，控制台自身经 hash 路由，无 history fallback。
export default defineConfig({
  plugins: [vue()],
  server: {
    proxy: {
      '/v1': 'http://127.0.0.1:8080',
      '/healthz': 'http://127.0.0.1:8080',
      '/readyz': 'http://127.0.0.1:8080',
    },
  },
})
