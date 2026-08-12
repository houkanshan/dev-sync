# dev-sync

Fast, one-way development sync from a local Git worktree to a remote directory over SSH.

## Configuration

Create `.dev-sync.toml` in the Git worktree:

```toml
remote = "user@example.com"
remote_path = "/workspace/project"
```

Optional `.dev-syncignore` rules use Git ignore syntax and add exclusions on top of Git's standard ignore rules.

## Commands

```console
dev-sync start
dev-sync start --foreground  # stay attached and stream timestamped sync logs
dev-sync status
dev-sync flush
dev-sync stop
```

The remote needs SSH, a POSIX shell, and `tar`; it does not need `dev-sync` installed.
