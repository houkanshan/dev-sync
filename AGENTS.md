# AGENTS.md

## Project

`dev-sync` is a Rust CLI that continuously syncs one local Git worktree to a remote directory over SSH.

## Commands

The public CLI is intentionally limited to:

- `dev-sync start`
- `dev-sync status`
- `dev-sync stop`
- `dev-sync flush`

Do not require `dev-sync` on the remote. The remote may only be assumed to have SSH, a POSIX shell, and `tar`.

## Source of truth

- Git determines which files are eligible: `git ls-files --cached --others --exclude-standard`.
- `.dev-syncignore` adds sync-specific exclusions using Git ignore syntax.
- Local files are authoritative; this is not bidirectional sync.

## Development

After every code or configuration change, run:

```bash
./scripts/rebuild
```

The rebuild script formats, lints, tests, and creates `bin/dev-sync`.
