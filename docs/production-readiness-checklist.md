# Production Readiness Checklist

Use this checklist before deploying the IronLayer API (control plane) to **staging** or **production**. The application will **refuse to start** if required settings are missing in those environments.

---

## Required environment variables (staging / production)

| Variable | Description | Notes |
|---------|-------------|--------|
| `JWT_SECRET` | Secret used to sign and verify JWT tokens. | High-entropy value; store in a secrets manager. |
| `API_PLATFORM_ENV` | One of `staging`, `production` (or `dev` for local). | Must be set to `staging` or `production` for production deployments. |
| `CREDENTIAL_ENCRYPTION_KEY` or `credential_encryption_key` | Key for encrypting stored credentials (e.g. LLM API keys, PATs). | **Must not** be the default `ironlayer-dev-secret-change-in-production`. Set a strong, unique value. |

## Required when billing is enabled

If `API_BILLING_ENABLED` (or `billing_enabled`) is `true`:

| Variable | Description |
|---------|-------------|
| Stripe secret key | Set via config (e.g. `stripe_secret_key`). Must be non-empty. |
| `stripe_webhook_secret` | Webhook signing secret for Stripe events. Must be non-empty. |

The API will **fail startup** if billing is enabled and either Stripe secret or webhook secret is missing.

## Optional but recommended

| Variable | Description |
|---------|-------------|
| `API_ALLOWED_REPO_BASE` | Base directory for `repo_path` validation (default `/workspace`). Set to the actual workspace root in production. |
| `API_DATABASE_URL` | PostgreSQL connection string for production. Do not use SQLite in production. |
| `API_MAX_REQUEST_BODY_SIZE` | Max request body size in bytes (default 1048576). Tune if needed for large payloads. |
| Structured logging | Set `API_STRUCTURED_LOGGING=true` for JSON logs (SIEM / log aggregation). |

## Security reminders

- **Credential encryption key:** In staging/production, never leave the default value. Rotate periodically and store in a secrets manager.
- **JWT secret:** Rotate with care; existing tokens will be invalidated. Use a long, random value (e.g. 256 bits).
- **CORS:** Set `API_CORS_ORIGINS` to explicit origins. Do not use `*` when credentials are enabled.

## Quick verification

After deployment, confirm:

1. Health endpoint returns 200: `GET /api/v1/health` (or your configured path).
2. Login/signup and protected endpoints require a valid token.
3. If billing is enabled, Stripe webhook delivery succeeds and subscription flows work.

See [release-verification.md](release-verification.md) for release and deploy verification, and [deployment.md](deployment.md) for full deployment options.
