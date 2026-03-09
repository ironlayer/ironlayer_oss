/**
 * Lightweight error reporting service.
 *
 * When VITE_ERROR_REPORTING_URL is set, errors are POSTed as JSON to that
 * endpoint (e.g. a Sentry relay, Datadog intake, or custom collector).
 * When unset, errors are logged to the console only.
 *
 * No external dependencies — works with any backend that accepts JSON.
 */

const REPORTING_URL = import.meta.env.VITE_ERROR_REPORTING_URL as
  | string
  | undefined;

interface ErrorReport {
  message: string;
  stack?: string;
  componentStack?: string;
  url: string;
  timestamp: string;
  userAgent: string;
}

/** Best-effort POST — never throws, never blocks the UI. */
function sendReport(report: ErrorReport): void {
  if (!REPORTING_URL) return;

  try {
    // Use sendBeacon when available (survives page unload); fall back to fetch.
    const payload = JSON.stringify(report);
    const sent =
      typeof navigator.sendBeacon === 'function' &&
      navigator.sendBeacon(REPORTING_URL, payload);

    if (!sent) {
      fetch(REPORTING_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: payload,
        keepalive: true,
      }).catch(() => {
        /* swallow — reporting must never crash the app */
      });
    }
  } catch {
    /* swallow */
  }
}

/** Report a caught Error (e.g. from ErrorBoundary or try/catch). */
export function reportError(
  error: Error,
  componentStack?: string,
): void {
  console.error('[ErrorReporting]', error);

  sendReport({
    message: error.message,
    stack: error.stack,
    componentStack,
    url: window.location.href,
    timestamp: new Date().toISOString(),
    userAgent: navigator.userAgent,
  });
}

/**
 * Install global handlers for uncaught errors and unhandled rejections.
 * Call once at app startup (e.g. in main.tsx).
 */
export function installGlobalErrorHandlers(): void {
  window.addEventListener('error', (event) => {
    if (event.error instanceof Error) {
      reportError(event.error);
    }
  });

  window.addEventListener('unhandledrejection', (event) => {
    const reason =
      event.reason instanceof Error
        ? event.reason
        : new Error(String(event.reason));
    reportError(reason);
  });
}
