import { createApp } from 'vue'
import App from './App.vue'
import { initRouter } from './router'
import './style.css'

initRouter()
createApp(App).mount('#app')
