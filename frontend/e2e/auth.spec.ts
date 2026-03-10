/**
 * Authentication flow E2E tests (BL-157).
 * Covers login page rendering and basic session checks.
 * Full credential-based login is handled in integration tests using the
 * test fixture account; these E2E tests focus on UI presence.
 */

import { test, expect } from '@playwright/test';

test.describe('Login flow', () => {
  test('login page renders with email and password fields', async ({ page }) => {
    await page.goto('/login');
    // Wait for the React SPA to hydrate.
    await page.waitForLoadState('networkidle');

    // Email field must be present.
    const emailField = page.locator('input[type="email"], input[name="email"], input[placeholder*="email" i]');
    await expect(emailField.first()).toBeVisible({ timeout: 10_000 });

    // Password field must be present.
    const passwordField = page.locator('input[type="password"]');
    await expect(passwordField.first()).toBeVisible({ timeout: 5_000 });
  });

  test('submit button is present on login page', async ({ page }) => {
    await page.goto('/login');
    await page.waitForLoadState('networkidle');

    const submitBtn = page.locator('button[type="submit"], button:has-text("Sign in"), button:has-text("Log in")');
    await expect(submitBtn.first()).toBeVisible({ timeout: 10_000 });
  });

  test('invalid credentials show an error message', async ({ page }) => {
    await page.goto('/login');
    await page.waitForLoadState('networkidle');

    const emailField = page.locator('input[type="email"], input[name="email"]').first();
    const passwordField = page.locator('input[type="password"]').first();
    const submitBtn = page.locator('button[type="submit"]').first();

    await emailField.fill('invalid@example.com');
    await passwordField.fill('wrongpassword');
    await submitBtn.click();

    // Some visible error indicator should appear (text, role=alert, etc.).
    const errorIndicator = page.locator(
      '[role="alert"], .error, [data-testid*="error"], :text("Invalid"), :text("incorrect"), :text("failed")'
    );
    await expect(errorIndicator.first()).toBeVisible({ timeout: 10_000 });
  });
});
