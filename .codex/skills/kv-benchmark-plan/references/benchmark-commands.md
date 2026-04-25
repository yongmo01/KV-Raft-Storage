# Benchmark Commands

These commands match the current repository layout and the retrofit plan above.

## 1. Build Matrices

### Baseline

```bash
mkdir -p build-baseline
cd build-baseline
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=OFF -DENABLE_LSM_TREE=OFF
make -j"$(nproc)"
cd ..
```

### Cache

```bash
mkdir -p build-cache
cd build-cache
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=ON -DENABLE_LSM_TREE=OFF
make -j"$(nproc)"
cd ..
```

### LSM

```bash
mkdir -p build-lsm
cd build-lsm
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_KEY_TTL=OFF -DENABLE_LRU_CACHE=OFF -DENABLE_LSM_TREE=ON
make -j"$(nproc)"
cd ..
```

### Full

```bash
mkdir -p build-full
cd build-full
cmake .. -DCMAKE_BUILD_TYPE=Release -DENABLE_KEY_TTL=ON -DENABLE_LRU_CACHE=ON -DENABLE_LSM_TREE=ON
make -j"$(nproc)"
cd ..
```

For low-end 2-vCPU machines, add `-DTHREAD_POOL_SIZE=1` or `-DTHREAD_POOL_SIZE=2` to the CMake command.

## 2. Current Repository Commands

These work against the current executable naming:

### Start cluster

```bash
./bin/raftCoreRun -n 3 -f bench.conf
```

### Existing single-loop client

```bash
./bin/callerMain -c 10000 -o put -k bench -f bench.conf
./bin/callerMain -c 10000 -o get -k bench -f bench.conf
./bin/callerMain -c 10000 -o both -k bench -f bench.conf
```

### External multi-process pressure

Different keys:

```bash
seq 1 32 | xargs -I{} -P 32 bash -c './bin/callerMain -c 2000 -o put -k key_{} -f bench.conf >/tmp/kv_put_{}.log 2>&1'
```

Hot key:

```bash
seq 1 32 | xargs -I{} -P 32 bash -c './bin/callerMain -c 2000 -o put -k hot -f bench.conf >/tmp/kv_hot_{}.log 2>&1'
```

Read pressure:

```bash
./bin/callerMain -c 1000 -o put -k readbench -f bench.conf
seq 1 32 | xargs -I{} -P 32 bash -c './bin/callerMain -c 5000 -o get -k readbench -f bench.conf >/tmp/kv_get_{}.log 2>&1'
```

## 3. After Adding `benchMain`

Recommended benchmark patterns:

### Pure write

```bash
./bin/benchMain -c 100000 -j 8 -o put -m unique -p bench -s 256 -f bench.conf --rate 1000 --json
```

### Pure read

```bash
./bin/benchMain -c 100000 -j 8 -o get -m hot -k bench -f bench.conf --rate 1000 --json
```

### Mixed read/write

```bash
./bin/benchMain -c 100000 -j 8 -o both -m range -p mix -r 80 -s 256 -f bench.conf --rate 1000 --json
```

### Pure Raft commit path after adding `noop`

```bash
./bin/benchMain -c 100000 -j 8 -o noop -m unique -p noop -f bench.conf --rate 1000 --json
```

## 4. etcd Comparison Commands

Use etcd as the first external baseline. It is the most straightforward comparison target for a 3-node strongly consistent KV.

Official references:
- https://etcd.io/docs/v3.6/op-guide/performance/
- https://etcd.wordword.xyz/en/docs/v3.2/op-guide/performance/

### Example 3-node local cluster

Node 1:

```bash
etcd --name s1 \
  --data-dir /tmp/etcd/s1 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --initial-cluster s1=http://127.0.0.1:2380,s2=http://127.0.0.1:3380,s3=http://127.0.0.1:4380 \
  --initial-cluster-token bench \
  --initial-cluster-state new
```

Node 2:

```bash
etcd --name s2 \
  --data-dir /tmp/etcd/s2 \
  --listen-client-urls http://127.0.0.1:3379 \
  --advertise-client-urls http://127.0.0.1:3379 \
  --listen-peer-urls http://127.0.0.1:3380 \
  --initial-advertise-peer-urls http://127.0.0.1:3380 \
  --initial-cluster s1=http://127.0.0.1:2380,s2=http://127.0.0.1:3380,s3=http://127.0.0.1:4380 \
  --initial-cluster-token bench \
  --initial-cluster-state new
```

Node 3:

```bash
etcd --name s3 \
  --data-dir /tmp/etcd/s3 \
  --listen-client-urls http://127.0.0.1:4379 \
  --advertise-client-urls http://127.0.0.1:4379 \
  --listen-peer-urls http://127.0.0.1:4380 \
  --initial-advertise-peer-urls http://127.0.0.1:4380 \
  --initial-cluster s1=http://127.0.0.1:2380,s2=http://127.0.0.1:3380,s3=http://127.0.0.1:4380 \
  --initial-cluster-token bench \
  --initial-cluster-state new
```

### Health check

```bash
export ETCDCTL_API=3
etcdctl --endpoints=http://127.0.0.1:2379,http://127.0.0.1:3379,http://127.0.0.1:4379 endpoint status -w table
```

### Example etcd benchmark

```bash
benchmark --endpoints=http://127.0.0.1:2379 --target-leader --conns=1 --clients=1 \
  put --key-size=8 --sequential-keys --total=10000 --val-size=256
```

```bash
benchmark --endpoints=http://127.0.0.1:2379,http://127.0.0.1:3379,http://127.0.0.1:4379 \
  --conns=32 --clients=32 \
  put --key-size=8 --sequential-keys --total=100000 --val-size=256
```

## 5. Result Table Template

Use a simple table:

| Case | Build | TTL | LRU | LSM | Workload | Concurrency | Value Size | QPS | Avg | P95 | P99 |
|---|---|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|
| baseline put | Release | off | off | off | put | 32 | 256 |  |  |  |  |
| cache read | Release | off | on | off | get | 32 | 256 |  |  |  |  |
| lsm put | Release | off | off | on | put | 32 | 256 |  |  |  |  |
| full mixed | Release | on | on | on | both | 32 | 256 |  |  |  |  |
| noop raft | Release | off | off | off | noop | 32 | 0 |  |  |  |  |

## 6. What To Say About Results

When reporting:
- Call it “end-to-end KV performance” unless it is the `noop` path.
- For LSM latency spikes, check whether foreground flush counters moved.
- When comparing to etcd, note that your client and RPC stack are different, so absolute numbers are less meaningful than:
  - scaling trend
  - latency curve
  - failover recovery behavior
  - feature-cost deltas
