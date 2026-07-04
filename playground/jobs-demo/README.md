# jobs-demo

Runnable sample for the `jobs` package: a cron schedule (the actor system's
existing `ScheduleWithCron`) periodically kicks off a fan-out/fan-in
computation run through `jobs.Engine` -- the business case the feature was
built for: computing an agent's total reward across a fixed set of "downline"
members.

## What it demonstrates

- **Cron composing with jobs**: every 2 seconds, a `cronTrigger` actor
  receives a cron tick and calls `engine.FanOut` to start a new round.
- **Fan-out**: each round splits into one child job per downline agent
  (`reward-worker` computes each one independently).
- **Fan-in**: once every child completes, `reward-aggregator` receives a
  `*jobs.FanInEnvelope` with all three results and sums them.
- **Inspector**: `engine.Inspector().QueueDepths` is printed at the end.

Since the set of downline agent IDs never changes, the aggregated total is
deterministic across every round (`700 + 800 + 900 = 2400`), which is what
makes this sample self-validating.

## Run it

```
go run ./playground/jobs-demo
```

Expected output: at least two completed rounds within the 7-second run
window, each totalling `2400`, followed by `PASS`. The process exits non-zero
and prints `FAIL: ...` if either condition is not met.

## Files

- `main.go` -- the sample, self-contained (single node, in-memory `Store`).
