# Release & Deployment Verification Guide

How to confirm that open-source packages are published and live.

---

## 1. PyPI (ironlayer & ironlayer-core)

### How it gets published

- **Trigger:** Pushing a **version tag** (`v*`, e.g. `v0.3.0`) to the repo.
- **Workflow:** `.github/workflows/publish.yml` builds **Rust wheels** via maturin (multi-platform) and publishes to PyPI.

### How to verify PyPI

1. **Check that the workflow ran**
   - GitHub → repo → **Actions** → workflow **"Publish to PyPI"**.
   - Find the run for your tag (e.g. `v0.3.0`). Both jobs (`Publish ironlayer-core`, `Publish ironlayer`) should be green.

2. **Check package pages**
   - **ironlayer-core:** https://pypi.org/project/ironlayer-core/
   - **ironlayer:** https://pypi.org/project/ironlayer/
   - Confirm the **version** you tagged appears in "Release history".

3. **Smoke test install**
   ```bash
   pip index versions ironlayer
   pip index versions ironlayer-core
   pip install ironlayer==<your-version> --dry-run
   ```

4. **Required secrets**
   - GitHub Environment **pypi** with trusted publisher (OIDC) **or** `PYPI_API_TOKEN` in repo/org secrets.

---

## 2. Documentation

- **In-repo markdown:** `docs/` directory contains quickstart, CLI reference, architecture, API reference, and deployment guides.
- **On the web:** Documentation is served from the project website.

---

## 3. Document review (open-source cleanliness)

Before tagging a release, review docs and visible repo content for:

- **Proprietary/internal names** — Use generic wording so the public repo does not imply non-public info.
- **Personal data** — No personal emails, internal URLs, or private GitHub usernames in docs.
- **Non-open-source requirements** — Remove or generalize any dependency on unreleased/internal packages.

---

## 4. Quick checklist after a release

| What | Where to check |
|------|------------------|
| PyPI `ironlayer` | https://pypi.org/project/ironlayer/ — version in release history |
| PyPI `ironlayer-core` | https://pypi.org/project/ironlayer-core/ — version + wheel files |
| Publish workflow | GitHub Actions → "Publish to PyPI" for tag `vX.Y.Z` |
| Docs on the web | Project website docs — quickstart, CLI ref, architecture |

---

## 5. One-off verification commands

```bash
# PyPI: list published versions
pip index versions ironlayer
pip index versions ironlayer-core

# Optional: install a specific version and run a command
pip install ironlayer==0.3.0
ironlayer --version
ironlayer check --help   # if check engine was included in that release
```
