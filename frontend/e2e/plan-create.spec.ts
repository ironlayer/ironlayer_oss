/**
 * Plan creation flow E2E tests (BL-157).
 * These tests use environment variables for credentials and are gated on
 * E2E_EMAIL / E2E_PASSWORD being set.  Without credentials they only
 * verify that the plans page redirects unauthenticated users to /login.
 */

import { test, expect } from '@playwright/test';

const E2E_EMAIL = process.env.E2E_EMAIL ?? '';
const E2E_PASSWORD = process.env.E2E_PASSWORD ?? '';
const HAS_CREDS = !!(E2E_EMAIL && E2E_PASSWORD);

test.describe('Plans page — unauthenticated', () => {
  test('navigating to /plans redirects to /login', async ({ page }) => {
    await page.goto('/plans');
    await page.waitForLoadState('networkidle');

    // Expect to land on the login page (redirect or rendered component).
    await expect(page).toHaveURL(/login/, { timeout: 10_000 });
  });
});

test.describe('Plan creation — authenticated', () => {
  test.skip(!HAS_CREDS, 'Set E2E_EMAIL and E2E_PASSWORD to run authenticated E2E tests');

  test.beforeEach(async ({ page }) => {
    // Log in before each test.
    await page.goto('/login');
    await page.waitForLoadState('networkidle');
    await page.locator('input[type="email"], input[name="email"]').first().fill(E2E_EMAIL);
    await page.locator('input[type="password"]').first().fill(E2E_PASSWORD);
    await page.locator('button[type="submit"]').first().click();
    // Wait for redirect to dashboard/plans.
    await page.waitForURL(/dashboard|plans/, { timeout: 15_000 });
  });

  test('plans list page renders', async ({ page }) => {
    await page.goto('/plans');
    await page.waitForLoadState('networkidle');
    // Page should not show a hard error boundary.
    await expect(page.locator(':text("Something went wrong")')).not.toBeVisible();
    await expect(page.locator(':text("Page failed to load")')).not.toBeVisible();
  });

  test('create plan button is present on plans page', async ({ page }) => {
    await page.goto('/plans');
    await page.waitForLoadState('networkidle');
    const createBtn = page.locator('button:has-text("Create"), button:has-text("New plan"), a:has-text("New plan")');
    await expect(createBtn.first()).toBeVisible({ timeout: 10_000 });
  });
});
