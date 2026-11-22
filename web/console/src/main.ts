import { createApp } from 'vue'
import App from './App.vue'
import { initRouter } from './router'
import './style.css'

// initRouter 先于挂载：同步解析当前 hash 作为初始路由，首帧即渲染目标页。
initRouter()
createApp(App).mount('#app')
