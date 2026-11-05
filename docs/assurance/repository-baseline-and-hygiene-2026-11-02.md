# Repository Baseline and CI/CD Hygiene Audit

**Audit timestamp:** 2026-11-02
**Repository:** `kj111422/engineering-challenge-template`
**Current `main`:** `3da31e84cb883c78c2104c839d55a718c3891088`
**Architecture charter baseline:** `df87cb1c207a06e87e29a2cae8cd3b77e110a67e`

> **Integration note (2026-11-05):** This is a historical audit snapshot. Both obsolete branches were deleted after ancestry and open-PR checks. Only `main` remains. Branch protection now requires the Rust CI, Architecture CI and FOSSA contexts with strict up-to-date checks, linear history, conversation resolution, no force pushes and no deletion. Repository merge settings are squash-only with automatic head-branch deletion. No tags or releases exist.

## 1. Branch inventory

| Branch | SHA | Relation to `main` | Protected | Disposition |
|---|---|---:|---|---|
| `main` | `3da31e84cb883c78c2104c839d55a718c3891088` | Current | Yes | Permanent |
| `polymarket-continuity-failure` | `275f78f6a3cda4812cf76136aa66d5d6066fe63f` | 2 behind, 0 ahead | No | Delete after local check |
| `journal-failure-hardening` | `5924eb6e3861d551a0f62c58eb1dc2e689625390` | 5 behind, 0 ahead | No | Delete after local check |

Both obsolete branches are ancestors of `main`; the compare API reports no commits unique to either branch. No open pull requests remain. PR #4 merged on 2026-11-02; PR #2 is merged. PR #1 is closed without merge.

The GitHub branch list reports `main` as protected. The repository has no GitHub rulesets. The branch-protection detail endpoint was unavailable to the connected integration, so exact settings must be checked in GitHub repository settings. Protection being enabled does not by itself prove that required status checks, linear history, PR requirements, or force-push restrictions are configured.

### Local deletion procedure

Run from the maintainer's checkout after `git fetch origin --prune`:

```bash
git status --short
git branch -vv
gh pr list --state open --json number,title,headRefName,baseRefName

git merge-base --is-ancestor origin/polymarket-continuity-failure origin/main
git merge-base --is-ancestor origin/journal-failure-hardening origin/main

# Delete local and remote refs only after both checks pass.
git branch -d polymarket-continuity-failure
git push origin --delete polymarket-continuity-failure
git branch -d journal-failure-hardening
git push origin --delete journal-failure-hardening
```

If any ancestry or PR check fails, stop and inspect the ref. Do not use `-D`, force-push or history rewriting.

## 2. CI/CD workflow inventory

The current workflows already have the intended branch-scoped topology. The previous suggestion to narrow unfiltered push/PR triggers is obsolete for this SHA.

| Workflow | Trigger | Job | Runner | Gate |
|---|---|---|---|---|
| `.github/workflows/ci.yml` | Push to `main`, PR targeting `main`, manual dispatch | Rust CI | `ubuntu-24.04` | fmt, Clippy `-D warnings`, locked tests, doc tests, release build |
| `.github/workflows/architecture.yml` | Push to `main`, PR targeting `main`, manual dispatch | Architecture docs | `ubuntu-24.04` | Python architecture tests, source/test inventory, Mermaid validation |
| `.github/workflows/live.yml` | Manual dispatch only | Public Polymarket integration | `ubuntu-latest` | Opt-in public discovery/WebSocket tests, no credentials |

Both mandatory workflows use concurrency groups that cancel superseded PR runs, `contents: read`, `persist-credentials: false`, full-SHA action pins, and job timeouts. Keep live public-feed tests separate from deterministic CI. Do not turn network-dependent tests into required checks for every PR.

## 3. Exact CI evidence on current `main`

- Rust CI: [run 41468148370](https://github.com/kj111422/engineering-challenge-template/actions/runs/41468148370) — completed, success, exact SHA `3da31e84cb883c78c2104c839d55a718c3891088`.
- Architecture documentation: [run 41468148207](https://github.com/kj111422/engineering-challenge-template/actions/runs/41468148207) — completed, success, exact SHA `3da31e84cb883c78c2104c839d55a718c3891088`.
- FOSSA contexts on the same SHA: License Compliance, Dependency Quality and Security Analysis — success.

These remote statuses do not substitute for a local full-tree prose audit or unrun operational tests.

## 4. Tags and releases

The GitHub tag-ref query returned an empty list. The GitHub Releases query also returned an empty list. No historical tag cleanup is required at this snapshot.

Policy:
- Keep the tag namespace empty until a release passes the release gate.
- Use annotated semantic-version tags, such as `v0.1.0`, for approved releases.
- Protect `v*` against updates and deletion after the first release.
- Do not tag intermediate milestones or failed builds.
- Do not delete published release tags; supersede them with a new version and a release note.

## 5. Linear history

The repository already contains historical merge commits. Preserve them as part of the provenance of the integrated work. Do not rewrite published history to make the graph visually linear.

For future changes, configure and verify:
- PRs required for `main`.
- Squash-only merge.
- Linear history required.
- Force pushes disabled.
- Branch deletion disabled for `main`.
- Required CI and Architecture checks.
- Conversation resolution.
- Up-to-date branch requirement where supported.
- Auto-delete merged topic branches.

Check the exact protection rule in GitHub settings because the available API response only exposed `protected: true`, not the rule details.

## 6. Documentation baseline drift

`docs/production-roadmap-polymarket.md` still identifies `main` at `9ab6cd5` dated 2026-10-26 in its header. The document may preserve that historical state, but the heading must label it as the initial 2026-10-26 baseline and add a separate current verified baseline at `3da31e8` dated 2026-11-02. Preserve all historical test counts and run links under their original commit context. Do not replace old evidence with current results.

## 7. Recommended final Git topology

```text
main  (protected; required checks; squash-only)
  |
  +-- short-lived fix/*
  +-- short-lived test/*
  +-- short-lived feat/*
  +-- short-lived docs/*

v* tags (created only for approved releases)
```

No long-lived integration branch is necessary while `main` can hold the reviewed integration baseline. Create an integration branch only when there is a documented need for parallel release trains or a multi-branch stabilization window.
