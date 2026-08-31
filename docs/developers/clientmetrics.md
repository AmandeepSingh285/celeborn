---
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

# Client Metrics

Celeborn clients can report their own metrics to the master, which aggregates them across
every reporting application and publishes them on its own Prometheus endpoint. Reports ride
on the existing application heartbeat rather than requiring a separate channel or a scrape
endpoint on every client.

This page describes how that aggregation stays correct over a transport that can duplicate,
drop, and reorder reports.

## Enabling

| Configuration | Default | Description |
| --- | --- | --- |
| `celeborn.client.metrics.enabled` | `false` | Report client metrics on the application heartbeat. Also requires `celeborn.metrics.enabled`. |
| `celeborn.client.metrics.appLabels` | (empty) | Labels attached to this client's series. Emission is skipped entirely when empty. |
| `celeborn.metrics.master.clientMetrics.enabled` | `false` | Accept and aggregate client metrics on the master. |
| `celeborn.metrics.master.clientMetrics.removedApp.retention` | `5min` | How long a removed application is remembered so late heartbeats are rejected. |
| `celeborn.metrics.master.clientMetrics.seriesCardinality.warnThreshold` | `1000` | Warn once the number of tracked series exceeds this. |

Series are keyed by metric name plus `appLabels`, **not** by application id, so all applications
sharing a label set collapse into one series. Keep these labels low-cardinality (`env`, `team`)
— a per-application value grows master memory without bound.

## The problem

The heartbeat transport is at-least-once and lossy. `MasterClient.askSync` retries across
master endpoints, and `ApplicationHeartbeater` swallows failures and simply sends a fresh
snapshot on the next tick. So a heartbeat the client saw time out may well have been applied,
and a heartbeat that was never applied is never replayed.

That means the master's receive path must not be a read-modify-write. If a client reported
`+42 since my last heartbeat`, the master would apply `sum += 42`, and a retry would apply it
twice — silently and permanently wrong, with no way to detect or repair it afterwards.

## The principle

**Report state, not operations.**

A delta is an *operation*: operations must be delivered exactly once, in order, or the result
is wrong. An absolute value is *state*: state-based replication needs only that updates
eventually arrive, in any order, any number of times.

> Per source, **replace** with an absolute snapshot. Across sources, **aggregate** at export
> time. Never mutate in place on receive.

So a client reports its counter's cumulative total *since that client process started*, never
a since-last-heartbeat delta. Dropwizard's `Counter.getCount` is already cumulative, so
`CelebornClientSource.getMetricsSnapshot` can report it directly.

## How each failure is handled

Take one application reporting `ClientBytesWritten`. The master holds `1000` for it; the client
has since written 500 more.

**A heartbeat times out but was applied.** The master stored `1500` and adjusted its running
total by `1500 - 1000`. The retry stores `1500` again and adjusts by `1500 - 1500 = 0`. The
retry is a no-op. Note that the client never has to discover whether its report landed — the
ambiguity is dissolved rather than resolved.

**A heartbeat is lost.** The next heartbeat carries the full absolute value and the master is
correct again. One sample of resolution is lost; no counted events are. At a 10s heartbeat
interval against a 300s application timeout there are ~30 heartbeats of slack.

**Heartbeats are reordered.** Absolute values alone are not enough here: replacing `1800` with
a delayed `1500` would move the counter backwards. Each client stamps a monotonically
increasing `metricsSeq`, and the master drops any report that does not advance it. This guard
exists *only* for reordering — duplicates were already handled by replace.

**A client process restarts under the same application id.** Its counters restart at zero, and
a plain replace would step the aggregate down. Each client process mints a `clientInstanceId`
(a UUID) at startup. When the master sees a new instance id for a known application, it banks
the previous process's value into a tombstone and tracks the new process from zero. Sequence
numbers restart with the process, so an instance change always wins over the sequence check.

**An application is evicted after its heartbeat timeout.** What happens depends on the metric
type, and this is the one behaviour that genuinely differs between them — see below.

**The master leader fails over.** Metric state is deliberately *not* in the Raft state machine:
the write volume is far too high, and it does not need to be. A new leader starts empty and
every live client repopulates it within one heartbeat interval, because every report carries
complete absolute state. The exported counter does restart from zero across a failover, which
downstream reads correctly only if the master instance is a label on the scrape so the series
identity changes.

## Aggregation and eviction

`TrackedMetric` (in `AbstractSource`) holds one exported series:

- `perAppValues`: each application's latest `(instanceId, value)`
- `liveSum`: the incrementally maintained sum over live applications, so export stays O(1)
- `evictedTotal`: banked contributions of applications that have gone away
- the metric type, which declares the eviction policy

Updates run inside `ConcurrentHashMap.compute`, so the per-application entry and the running
totals move together and concurrent reports for one application cannot interleave into a wrong
sum.

Eviction is the only behaviour that varies by metric type, so it is declared on `MetricType` as
an `EvictionPolicy` rather than branched on at each call site:

| Policy | Used by | On eviction | Why |
| --- | --- | --- | --- |
| `Subtract` | Gauge | Drop the contribution | A departed application has no active shuffles; it should stop contributing. |
| `Retain` | Counter | Bank it into `evictedTotal` | The counted events really happened. A decreasing counter makes every downstream `rate()` misread the drop as a counter reset. |

A `Retain` series therefore stays published even with no live reporters — unregistering it
would restart its counter from zero.

## Adding a new metric type

The transport, instance-reset handling, sequence guard, and tombstones are all type-agnostic.
Adding a type means declaring how it aggregates and how it evicts:

| Type | Per-source value | Aggregate | Eviction | Status |
| --- | --- | --- | --- | --- |
| Gauge | scalar | sum | `Subtract` | supported |
| Counter | cumulative scalar | sum | `Retain` | supported |
| Meter | cumulative count | sum | `Retain` | not wired up; report `count` only, derive rates downstream |
| Histogram / Timer | cumulative buckets + sum + count | pointwise sum | `Retain` | blocked, see below |

### Why histograms and timers are not supported yet

Two properties of the current implementation are structurally incompatible with reporting
them, independent of anything in the heartbeat path:

1. **Destructive read.** `CelebornHistogram` and `CelebornTimer` are backed by
   `ResettableSlidingWindowReservoir`, which is reset after each emission. A destructive read
   is inherently operation-based: each scrape consumes observations that then exist nowhere,
   so a dropped heartbeat loses them permanently.
2. **Percentiles do not aggregate.** Timer output is precomputed percentiles. No function
   combines one client's p99 with another's into a fleet p99. The same applies to `mean`
   without weights, and to a Meter's EWMA rates. Only `count` and `sum` are mergeable as-is.

Both require the same change: report cumulative bucket counts plus cumulative sum and count,
never reset, with percentiles computed on the master after merging. Bucket boundaries must be
shared across clients for buckets to be pointwise-mergeable. That is a change to the metric
primitives, not to the heartbeat protocol.

## Compatibility

Metric types are carried as a protobuf enum, and unknown values are logged and dropped during
deserialization, so a master older than a client ignores metric types it does not understand
rather than mis-applying them. A `metricsSeq` of `0` means "this client does not sequence its
reports", and such reports are always accepted, so a client older than a master behaves as it
did before sequencing existed.

## Known limitations

- **Concurrent processes under one application id.** If two client processes ever report the
  same application id at the same time, their instance ids alternate and each change banks the
  previous value into `evictedTotal`, inflating the counter. This requires two live
  `LifecycleManager`s for one application, which is already pathological. Carrying the process
  start time and rejecting reports from an older instance would close it.
- **Counter series are never reclaimed.** A non-zero `evictedTotal` keeps a series published
  indefinitely. Bounded by the number of distinct `appLabels` sets rather than by application
  count, so this is safe as long as those labels are low-cardinality.
