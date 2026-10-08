import { createRouter, createWebHashHistory } from 'vue-router'
import { useAuth } from '@/stores/auth'
import AdminLayout from '@/layouts/AdminLayout.vue'
import LoginPage from '@/pages/LoginPage.vue'
import ModelsPage from '@/pages/ModelsPage.vue'
import PackagesPage from '@/pages/PackagesPage.vue'
import PackageSettingsPage from '@/pages/PackageSettingsPage.vue'
import MonitorPage from '@/pages/MonitorPage.vue'
import HistoryPage from '@/pages/HistoryPage.vue'
import TransportsPage from '@/pages/TransportsPage.vue'

const router = createRouter({
  history: createWebHashHistory('/admin/'),
  routes: [
    { path: '/login', name: 'login', component: LoginPage, meta: { public: true } },
    {
      path: '/',
      component: AdminLayout,
      children: [
        { path: '', redirect: { name: 'monitor' } },
        { path: 'models', name: 'models', component: ModelsPage },
        { path: 'packages', name: 'packages', component: PackagesPage },
        { path: 'packages/:name/settings', name: 'package-settings', component: PackageSettingsPage },
        { path: 'monitor', name: 'monitor', component: MonitorPage },
        { path: 'history', name: 'history', component: HistoryPage },
        { path: 'transports', name: 'transports', component: TransportsPage },
      ],
    },
  ],
})

router.beforeEach(async (to) => {
  if (to.meta.public) return true
  const auth = useAuth()
  if (await auth.load()) return true
  return { name: 'login', query: { redirect: to.fullPath } }
})

export default router
