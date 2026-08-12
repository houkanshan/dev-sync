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

The remote needs SSH, a POSIX shell, and `tar`; it does not need `devsync` installed.
`devsync` probes the remote platform and atomically deploys a matching bundled
`devsync-agent` into the remote user's cache. Startup and `flush` fully validate
eligible files but transfer only whole files whose remote content differs; normal
Watchman batches inspect only dirty literal paths.
