import { lazy, Suspense } from 'react';
import { Routes, Route } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext';
import Layout from './components/Layout';
import ProtectedRoute from './components/ProtectedRoute';
import ErrorBoundary, { PageErrorFallback } from './components/ErrorBoundary';
import Dashboard from './pages/Dashboard';
import LoginPage from './pages/LoginPage';
import SignupPage from './pages/SignupPage';

const PlanDetail = lazy(() => import('./pages/PlanDetail'));
const ModelCatalog = lazy(() => import('./pages/ModelCatalog'));
const ModelDetail = lazy(() => import('./pages/ModelDetail'));
const BackfillPage = lazy(() => import('./pages/BackfillPage'));
const RunDetail = lazy(() => import('./pages/RunDetail'));
const UsageDashboard = lazy(() => import('./pages/UsageDashboard'));
const BillingPage = lazy(() => import('./pages/BillingPage'));
const Environments = lazy(() => import('./pages/Environments'));
const OnboardingPage = lazy(() => import('./pages/OnboardingPage'));
const SettingsPage = lazy(() => import('./pages/SettingsPage'));
const AdminDashboard = lazy(() => import('./pages/AdminDashboard'));
const ReportsPage = lazy(() => import('./pages/ReportsPage'));

// BL-114: Per-route ErrorBoundary helper — wraps a single route element so
// an uncaught error in one page does not unmount the entire application shell.
function RouteErrorBoundary({ children }: { children: React.ReactNode }) {
  return (
    <ErrorBoundary fallback={<PageErrorFallback />}>
      {children}
    </ErrorBoundary>
  );
}

function App() {
  return (
    // App-level boundary catches errors outside of any route (e.g. AuthProvider).
    <ErrorBoundary>
      <AuthProvider>
        <Suspense
          fallback={
            <div className="flex min-h-screen items-center justify-center bg-surface">
              <div className="h-8 w-8 animate-spin rounded-full border-4 border-ironlayer-500/20 border-t-ironlayer-500" role="status" aria-label="Loading page" />
            </div>
          }
        >
          <Routes>
            {/* Public routes — no per-route boundary needed; errors bubble to app-level */}
            <Route path="/login" element={<LoginPage />} />
            <Route path="/signup" element={<SignupPage />} />

            {/* Protected routes — each page gets its own ErrorBoundary (BL-114).
                An error in PlanDetail will not unmount Dashboard, ModelCatalog, etc. */}
            <Route element={<ProtectedRoute />}>
              <Route
                path="/onboarding"
                element={<RouteErrorBoundary><OnboardingPage /></RouteErrorBoundary>}
              />
              <Route element={<Layout />}>
                <Route
                  path="/"
                  element={<RouteErrorBoundary><Dashboard /></RouteErrorBoundary>}
                />
                <Route
                  path="/plans/:id"
                  element={<RouteErrorBoundary><PlanDetail /></RouteErrorBoundary>}
                />
                <Route
                  path="/models"
                  element={<RouteErrorBoundary><ModelCatalog /></RouteErrorBoundary>}
                />
                <Route
                  path="/models/:name"
                  element={<RouteErrorBoundary><ModelDetail /></RouteErrorBoundary>}
                />
                <Route
                  path="/backfills"
                  element={<RouteErrorBoundary><BackfillPage /></RouteErrorBoundary>}
                />
                <Route
                  path="/runs/:id"
                  element={<RouteErrorBoundary><RunDetail /></RouteErrorBoundary>}
                />
                <Route
                  path="/usage"
                  element={<RouteErrorBoundary><UsageDashboard /></RouteErrorBoundary>}
                />
                <Route
                  path="/billing"
                  element={<RouteErrorBoundary><BillingPage /></RouteErrorBoundary>}
                />
                <Route
                  path="/environments"
                  element={<RouteErrorBoundary><Environments /></RouteErrorBoundary>}
                />
                <Route
                  path="/settings"
                  element={<RouteErrorBoundary><SettingsPage /></RouteErrorBoundary>}
                />
                <Route
                  path="/admin"
                  element={<RouteErrorBoundary><AdminDashboard /></RouteErrorBoundary>}
                />
                <Route
                  path="/admin/reports"
                  element={<RouteErrorBoundary><ReportsPage /></RouteErrorBoundary>}
                />
              </Route>
            </Route>
          </Routes>
        </Suspense>
      </AuthProvider>
    </ErrorBoundary>
  );
}

export default App;
