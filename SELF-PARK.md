# self-park: full-capability GoAkt fork branch

This branch is `github.com/StringKe/goakt` fork's long-lived integration branch: upstream GoAkt (`github.com/Tochemey/goakt`, v4.3.0, which ships four capabilities we contributed: cluster single-fire cron, schedule introspection, LeaderChanged, TopicStats) plus the remaining eight fork capabilities this team needs for single-app / multi-replica deployments. Everything here is implemented additively (new options, new packages, zero changed upstream signatures, zero new dependencies) and is intended to be offered upstream; until merged there, self-park is the source of truth.

Upstream sync policy: `upstream/main` is merged in (never rebased). Feature branches `feat/01`..`feat/12` hold the upstream-clean cut of each capability for future PRs; note that post-integration fixes live on self-park only until cherry-picked back.

## Capability index

| Capability | Entry point | Docs |
| --- | --- | --- |
| Persistent scheduler queue (schedules survive restarts) | `actor.WithSchedulerJobQueue(queue, locker)` | docs/actor/scheduling.mdx ("Persistent Scheduling") |
| Cluster single-fire cron (one node fires per tick) | **upstream-native** since #1242: intrinsic in cluster mode, explicit `WithReference` required | docs/actor/scheduling.mdx |
| Scheduler introspection | **upstream-native** since #1241: `ActorSystem.ListSchedules()` -> `ScheduleInfo{Reference, Path}` | docs/actor/scheduling.mdx |
| Cluster KV + distributed lock | `ActorSystem.KV()` -> `kv.Store` (Get/Put/PutIfAbsent/TTL/TryLock) | docs/clustering/kv-store.mdx |
| Leader status + change events | **upstream-native** since #1239: `ActorSystem.IsLeader(ctx)`/`Leader(ctx)`, `LeaderChanged` eventstream event | docs/clustering/clustered.mdx |
| Cluster-wide rate limiting | `ratelimit.New(store, limit, window)` | docs/clustering/rate-limiting.mdx |
| Crash relocation (kill -9 recovery via placement journal) | `actor.WithPlacementJournal(store)` | docs/actor/crash-relocation.mdx |
| Non-actor pub/sub subscriptions | `ActorSystem.SubscribeTopic(topic, handler)` | docs/advanced/pubsub-bridge.mdx |
| Topic statistics | **upstream-native** since #1246: `ActorSystem.TopicStats(ctx, topic, timeout)` -> local subscriber count + cluster instance count | docs/advanced/pubsub.mdx ("Topic statistics") |
| Ephemeral high-churn actors | pattern: `WithRelocationDisabled()` + `WithPassivationStrategy(passivation.NewLongLivedStrategy())` (upstream already had both; our sugar option was removed after upstream review) | docs/actor/ephemeral-actors.mdx |
| Durable jobs: at-least-once, retry/DLQ, fan-out/fan-in, inspector | package `jobs` (`jobs.NewEngine`) | docs/advanced/jobs.mdx |
| Gateway: cluster-shared TLS (Cloudflare Origin CA), WS/SSE connection registry, two-tier delivery, shutdown draining | package `gateway` (`gateway.NewServer`, `gateway.NewRegistry`) | docs/advanced/gateway.mdx |

Runnable samples: `playground/jobs-demo/`, `playground/gateway-echo/`.

## Consuming this branch from another Go project

The module path is unchanged (`github.com/tochemey/goakt/v4`), so consumption goes through a `replace` directive in the application's go.mod (applies to main modules, which is exactly the app-side use case):

```bash
go mod edit -require=github.com/tochemey/goakt/v4@v4.3.0
go mod edit -replace=github.com/tochemey/goakt/v4=github.com/StringKe/goakt/v4@v4.3.1-sp.2
go mod tidy
```

Version convention: tags `v4.3.1-sp.N` (upstream v4.3.0 base; earlier snapshots were `v4.3.0-sp.N`) on this fork mark verified snapshots of self-park (upstream base + all capabilities, full lint + race suite green). Prefer a tag over `@self-park` so builds stay reproducible; bump to the next `-sp.N` after each verified integration round. If the fork is private to your org, also set `GOPRIVATE=github.com/StringKe/*`.

## Where to find context (humans and AI assistants)

1. This file - capability inventory and entry points.
2. `docs/*.mdx` pages listed above - semantics, guarantees, and worked examples per capability (Mintlify sources; readable as plain markdown, or `mint dev` to browse).
3. Package godoc - every exported type/option documents its contract; start at `go doc github.com/tochemey/goakt/v4/jobs` and `go doc github.com/tochemey/goakt/v4/gateway`.
4. `playground/jobs-demo` and `playground/gateway-echo` - minimal, runnable end-to-end wiring.
5. Tests as specification: `actor/scheduler_persistent_test.go`, `jobs/*_test.go`, `gateway/*_test.go`, and the `testkit/*_test.go` multi-node suites pin the exact cluster semantics (single-fire, lease takeover, single issuance, presence).

## Known limitations (current round)

- `jobs`: delivery targets actors only (grain delivery is a planned follow-up); the lease fencing token is enforced by the in-memory store but not yet part of the persisted job envelope proto.
- `gateway`: ACME issuance is an interface slot only (static files and Cloudflare Origin CA are implemented); `Inspector.Retry` currently allows retry from any state.
- Cluster single-fire is cron-only (interval/one-shot schedules stay node-local); claim entries are reclaimed by TTL alone, and a node reaching a tick later than the claim TTL skips it (upstream semantics).
