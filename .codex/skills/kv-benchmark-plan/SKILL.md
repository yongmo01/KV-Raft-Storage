---
name: kv-benchmark-plan
description: Use when the user wants a minimal, benchmark-oriented retrofit plan for this KV-Raft project, including exactly which files to change, what to change in each file, what not to change, and how to run comparable performance tests without reshaping the project architecture.
---

# KV Benchmark Plan

This skill is for turning the current KV-Raft repository into something that can be benchmarked credibly without turning it into a different system.

Use this skill when the user asks for:
- a benchmark retrofit plan
- a minimal performance-testing modification list
- file-by-file changes for benchmarkability
- concrete benchmark commands for this repository
- comparison planning against etcd, braft, or TiKV

## Scope

Keep the project recognizable. Prefer:
- build-mode fixes
- logging controls
- benchmark-only client additions
- lightweight metrics
- benchmark-only control paths such as `noop`

Avoid in the first pass:
- Multi-Raft
- MVCC
- RPC framework replacement
- large lock refactors
- RocksDB replacement

## Workflow

1. Read [references/minimal-benchmark-retrofit.md](references/minimal-benchmark-retrofit.md) for the file-by-file plan.
2. Read [references/benchmark-commands.md](references/benchmark-commands.md) for the command set.
3. If the user wants only planning, summarize the phases and stop.
4. If the user wants implementation, apply changes in this order:
   - P0 build and logging credibility
   - P1 benchmark client and reporting
   - P2 Raft-vs-storage split benchmark path
   - P3 fault-injection and observability helpers
5. After each phase, validate with the smallest relevant compile/run command instead of waiting until the end.

## Guardrails

- Do not change protocol semantics unless the change is benchmark-only and isolated, such as a `noop` operation.
- Do not remove the existing demo path. Add benchmark-specific code alongside it.
- Do not mix feature evaluation dimensions. Establish a fixed matrix first:
  - baseline: `TTL=OFF, LRU=OFF, LSM=OFF`
  - cache: `TTL=OFF, LRU=ON, LSM=OFF`
  - lsm: `TTL=OFF, LRU=OFF, LSM=ON`
  - full: `TTL=ON, LRU=ON, LSM=ON`
- When discussing “Raft performance”, separate:
  - `noop` commit path
  - real `put` path
- Be explicit when results are end-to-end rather than pure-Raft.

## Output Shape

When answering from this skill, prefer this structure:

1. `P0 Must change`
2. `P1 Strongly recommended`
3. `P2 Optional but useful`
4. `Do not change yet`
5. `Commands`

## Repository-Specific Notes

- Current benchmark-relevant executables:
  - `raftCoreRun`
  - `callerMain`
  - `storage_cache_test`
  - `storage_lsm_test`
- Current feature switches already exposed in top-level CMake:
  - `ENABLE_READ_INDEX`
  - `ENABLE_KEY_TTL`
  - `ENABLE_TTL_ACTIVE_EXPIRE`
  - `ENABLE_THREAD_POOL`
  - `ENABLE_METRICS`
  - `ENABLE_LRU_CACHE`
  - `ENABLE_LSM_TREE`
- Current `callerMain` is a sequential loop, so it is not a serious benchmark client yet.
- Current `LSMTreeEngine` in `KvServer` is used without WAL/manifest path wiring and without starting the background worker. Benchmark conclusions must reflect that.
