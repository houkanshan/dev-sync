# devsync

Fast, one-way development sync from a local Git worktree to a remote directory over SSH.

## Configuration

Create `.devsync.toml` in the Git worktree:

```toml
remote = "user@example.com"
remote_path = "/workspace/project"
```

Optional `.devsyncignore` rules use Git ignore syntax and add exclusions on top of Git's standard ignore rules.

## Commands

```console
devsync start
devsync start --foreground  # stay attached and stream timestamped sync logs
devsync status
devsync flush
devsync stop
```

Persistent agent sessions receive a heartbeat every 15 seconds between sync batches.
The agent releases its state lock if a read waits 10 minutes for client input,
including when a half-open SSH connection never delivers EOF. Local planning and
payload preparation must not leave the agent waiting longer than this; remote disk
work is outside the read timeout. A disconnected session is re-established on the
next sync. This timeout does not bound blocked remote writes or disk operations.

The remote needs SSH, a POSIX shell, and `tar`; it does not need `devsync` installed.
`devsync` probes the remote platform and atomically deploys a matching bundled
`devsync-agent` into the remote user's cache. Startup, `flush`, Watchman recrawls,
and ignore-rule changes fully validate eligible files but transfer only whole files
whose remote content differs. Normal Watchman batches inspect only dirty literal
paths and skip Git-ignored / `.git` noise.
