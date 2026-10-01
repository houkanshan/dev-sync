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
devsync tail                # print the last 20 background log lines and exit
devsync tail -n 100 -f      # print 100 lines, then follow until interrupted
devsync flush
devsync restart             # stop safely, then start with current configuration
devsync stop
```

`status` reports activity, pending paths, last successful sync, uptime, and the last
sync error. It exits nonzero when this worktree has no running daemon.

`restart` also works when stopped. For a running daemon, it waits for the current
sync and requires a successful stop reply, then waits up to 65 seconds for shutdown
cleanup before starting. A stop error or cleanup timeout aborts the restart.

`tail` uses the local `tail` utility and reads only this worktree's background log,
even after stopping. `--lines` / `-n` controls the line count; `--follow` / `-f` keeps
streaming. Background startup replaces the previous log; `start --foreground`
writes to the terminal instead and does not update that log.

Persistent agent sessions receive a heartbeat every 15 seconds between sync batches.
The agent releases its state lock if a read waits 10 minutes for client input,
including when a half-open SSH connection never delivers EOF. Local planning and
payload preparation must not leave the agent waiting longer than this; remote disk
work is outside the read timeout. A disconnected session is re-established on the
next sync. This timeout does not bound blocked remote writes or disk operations.

The remote needs SSH, a POSIX shell, and `tar`; it does not need `devsync` installed.
`devsync` probes the remote platform and atomically deploys a matching bundled
`devsync-agent` into the remote user's cache. Startup, `flush`, Watchman recrawls,
and eligibility changes fully validate eligible files but transfer only whole files
whose remote content differs. Normal Watchman batches inspect only dirty literal
paths and skip Git-ignored / `.git` noise. Index-only events full-validate only when
`git ls-files` membership changes (`git add -f` / `git rm --cached`).
