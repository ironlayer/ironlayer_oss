/**
 * Health check E2E tests (BL-157).
 * Verify that the API health and readiness endpoints respond correctly.
 */

import { test, expect, request } from '@playwright/test';
import { API_URL } from '../playwright.config';

test.describe('API Health', () => {
  test('GET /api/v1/health returns 200', async () => {
    const ctx = await request.newContext({ baseURL: API_URL });
    const response = await ctx.get('/api/v1/health');
    expect(response.status()).toBe(200);
    const body = await response.json();
    expect(body).toHaveProperty('status');
    await ctx.dispose();
  });

  test('GET /ready returns 200 or 503', async () => {
    const ctx = await request.newContext({ baseURL: API_URL });
    const response = await ctx.get('/ready');
    // 200 = fully ready, 503 = starting up (acceptable in CI).
    expect([200, 503]).toContain(response.status());
    await ctx.dispose();
  });
});

test.describe('Frontend health', () => {
  test('root page loads without crashing', async ({ page }) => {
    const response = await page.goto('/');
    expect(response?.status()).toBeLessThan(400);
    // The page should render the React app root.
    await expect(page.locator('#root')).toBeAttached();
  });
});
