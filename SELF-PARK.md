# self-park: minimal core patches on top of upstream GoAkt

This branch is `github.com/StringKe/goakt` fork's long-lived integration branch. It tracks upstream GoAkt (`github.com/Tochemey/goakt`, v4.3.1) and carries **only the capabilities that cannot be built outside the actor core**. Everything else has moved out: six contributions were merged upstream, and the runtime-shaped subsystems now live in standalone satellite libraries built on upstream's public API.

Upstream sync policy: `upstream/main` is merged in (never rebased).

## What is left in this fork (2 core patches)

| Capability | Entry point | Why it cannot live outside core |
| --- | --- | --- |
| Persistent scheduler queue (schedules survive restarts) | `actor.WithSchedulerJobQueue(queue, locker)` | It replaces the scheduler's internal go-quartz job queue and persists a delivery-intent envelope. No public API reaches that far. |
| Placement journal (crash relocation at `replicaCount=1`) | `actor.WithPlacementJournal(store)` | The record hook must sit on the spawn path and the delete hooks on relocation's finalization points. Nothing outside core can inject them. Proposed upstream in [discussion #1259](https://github.com/Tochemey/goakt/discussions/1259). |

Both are additive: unconfigured, behavior is byte-for-byte upstream.

## Satellite libraries (built on upstream GoAkt, no fork dependency)

| Library | What it does |
| --- | --- |
| [goakt-gateway](https://github.com/StringKe/goakt-gateway) | Cluster-aware ingress: WebSocket/SSE connection registry with two-tier delivery (local socket write on a hit, cluster routing only cross-node), plus cluster-shared TLS termination (pluggable issuers incl. Cloudflare Origin CA, optional Authenticated Origin Pulls). Coordination (issuance lock, certificate distribution) runs on a `Coordinator` interface with in-memory and Redis implementations. |
| [goakt-jobs](https://github.com/StringKe/goakt-jobs) | Durable at-least-once task execution: lease-based delivery with fencing tokens, retry with backoff, dead-lettering, an inspector API, and cluster fan-out/fan-in. Jobs are delivered as ordinary actor messages. |

Both depend on `github.com/tochemey/goakt/v4` only - no fork, no internal packages, no `replace` directive - so they work for any GoAkt user.

## Merged upstream (no longer fork-only)

Cluster single-fire cron (#1242), scheduler introspection (#1241), `LeaderChanged` event (#1239), `TopicStats` (design we drove, #1246), remoting start-context fix (#1253), cluster bootstrap retry (#1258).

## Deliberately not implemented

**Cluster KV and distributed lock.** The embedded Olric store is PA/EC with last-write-wins and no consensus; its own README states its lock is "recommended for efficiency, not correctness". Building a certificate-issuance lock or a rate limiter on it publishes a guarantee the substrate cannot honor. Use Redis (`SET NX PX` plus a compare-and-delete release) for anything that needs real mutual exclusion - that is what goakt-gateway does.

**Cluster-wide rate limiting.** Same reason: an eventually-consistent counter admits the full budget on each side of a partition. Use Redis.

**Non-actor pub/sub subscription bridge.** Upstream declined it, and it turned out to be unnecessary: a satellite can spawn its own bridge actor and subscribe with the public `actor.Subscribe` message to `ActorSystem.TopicActor()`, which is exactly what goakt-gateway does.

## Consuming this branch

The module path is unchanged (`github.com/tochemey/goakt/v4`), so consumption goes through a `replace` directive in the application's go.mod:

```bash
go mod edit -require=github.com/tochemey/goakt/v4@v4.3.1
go mod edit -replace=github.com/tochemey/goakt/v4=github.com/StringKe/goakt/v4@v4.3.1-sp.7
go mod tidy
```

Tags `v4.3.1-sp.N` mark verified snapshots (upstream base plus the core patches above, full lint and race suite green). Prefer a tag over `@self-park` so builds stay reproducible.

Applications that also use the satellites just add them normally - they resolve against upstream GoAkt, and the `replace` above transparently points that at this fork.

## Where to find context

1. This file - what is here and what deliberately is not.
2. `docs/actor/scheduling.mdx` ("Persistent Scheduling") and `docs/actor/crash-relocation.mdx` - the two remaining capabilities.
3. Satellite READMEs for everything that moved out.

## Known limitations

- Placement journal: the record hook journals relocatable actors and eager grains; lazy grain directory entries follow upstream's own relocation semantics.
- Persistent scheduler queue: the journal envelope carries the delivery intent, so a rebuilt schedule resolves its target by name at fire time (the actor need not be the same process instance).
