# A Stream Processing System with State Disaggregation

This is a simple Java-based prototype that implements a multi-thread dataflow.

## Build and Run the project

### Build

use `mvn clean && mvn package` to build the project.

Expect `ControlPlane-jar-with-dependencies.jar` and `TaskManager-jar-with-dependencies.jar` in `target` directory. 

### Run

The following instructions are for running the system on a one-node deployment, then extending it to two nodes 
and attempting to scale the bottleneck (The server count operator).

#### Note

- make sure `config.json` is in the root directory of the project**
- the following operations *MUST* happen in order, `cwd` should be project root.**
- To configure TaskManagers to use different state storage strategies, please see "Configuration Options" section below.

1. Run ControlPlane `java -jar target/ControlPlane-jar-with-dependencies.jar`
2. Run TaskManager1 `java -jar target/TaskManager-jar-with-dependencies.jar` 
3. Wait for the 5 demo operators to get deployed on TaskManager1
4. Run TaskManager2 `java -jar target/TaskManager-jar-with-dependencies.jar -port=8028`
5. Wait for TaskManager2 to join the cluster
6. Run curl to request a scale operation on the 3rd stage: `curl http://localhost:9008/scale?stage=3`
7. Expect TaskManager2 to receive some load from TaskManager1

### Workload generation

The default workload is too small for observing any meaningful outcome (it finishes too fast). To get meaningful results,
please use one of the following methods to generate a larger workload.

1. Edit `controller/App.java`, modify `periodMillis` parameter in `WikipediaSource` constructor to a larger value, so 
that data generates slower; Optionally also modify `loadMillis` in stage 2 or `cpuLoad` in stage 3 to consume fewer CPU cycles.
2. Edit and run `workgen.py` in project root, and modify `config.json` to properly reflect the desired data source. This 
generates simulated workload of arbitrary size, and can be used to test the system's scalability, or performance.

## Configuration Options

Please see `config.json` for the configuration options. 

### ControlPlane

- `etcd_endpoints`: deprecated, we were planning to use etcd to store the routing table, but we ended up using memory instead
- `cp_grpc_port`: the port on which the ControlPlane listens for TaskManagers
- `watermark_interval`: the interval between each watermark sent
- `out_of_order_grace_period`: the grace period for out-of-order events

### TaskManager

- `cp_host`: the host of the ControlPlane
- `cp_port`: the port of the ControlPlane
- `tm_port`: the port on which the TaskManager grpc server listens
- `operator_quota`: the number of slots on each TM
- `operator_bufferSize`: the buffer size of each slot
- `useHybrid`: whether the TaskManager uses a hybrid (remote+local) key-value store
- `useCache`: whether the TaskManager caches the routing table
- `useMigrate`: whether the TaskManager is allowed to do partial state migration
- `rocksDBPath`: the path to the RocksDB state backend
- `batch_size`: the batch size of the RocksDB state backend
- `batch_timeout_ms`: the timeout before a batch message is emitted

### Prometheus

- `prometheus_port`: the port on which the Prometheus server listens
- `prometheus_host`: the host of the Prometheus server
- `pushgateway_port`: the port on which the Pushgateway server listens
- `pushgateway_host`: the host of the Pushgateway server
- `job_name`: the name of the job in Prometheus

### MISC

- `fileName`: the name of the file from which the source reads

## System Evaluation

The SinkOperator collects latency metrics and writes them to a file `source*.txt` in `pwd`. Each line of the file contains
the ingestion time and the latency of a single event. The latency is calculated as the difference between the ingestion 
time and the time it reaches the sink.

### Experimental Setup

The system was evaluated on a 16-core 32GB machine with 2 TaskManagers, each with 5 slots. The persistent storage is an NVMe SSD,
and the OS is Windows 10. The system was run on a single node, and the TaskManagers were started on different ports.

### Data collection

The original data are stored in the `zout` directory, where each configuration of the system was evaluated under the same
1000-line random input file. The specific configuration is described in `plot.py`.

The specific configs for each test has the following format: `source=9, count=12, replicas=2, hybridkv, junk=5, 
cache=yes, migrate=yes, migrate2=1682546002468`
- `source` is the interval in milliseconds between each event sent by the source
- `count` is the length of mandatory CPU time consumed by the count operator
- `replicas` is the number of replicas of the count operator (dynamically scaled)
- `hybridkv/localkv` indicates whether the test uses a hybrid or local key-value store
- `junk` is the relative amount of junk data written into the key-value store, to get a heavier state
- `cache` indicates whether the routing-table is cached
- `migrate` indicates whether the TMs are allowed to do partial state-migrates.
- `migrate2` is the timestamp at which a scale request was triggered. 

### Plot generation

To generate the plots, run `python3 plot.py` in the project root. The plots will be generated in the `plots` directory.

# Project Post-Mortem

## System Architecture

We designed and implemented a system that was able to start TaskManagers which could dynamically get assigned operators by the ControlPlane and scale up or down.

The design decisions were arguably correct for a proper system in production, but since we were only making a prototype, 
many of the parts really should have been simplified in hindsight. Nevertheless, everyone on the team learned a lot about
the challenges of building a distributed system. The JVM object model itself is quite interesting in that when actually designing
the deployment scheme, we find ourselves facing similar problems k8s and other container orchestration systems were dealing with. 
While we didn't have to actually address these issues, it was still a good learning experience looking at how Flink does it.

We made some parts of the system more complex than it should have, which caused problems in the later stages of the project.

### Generic Stream Operations

Our APIs for building a query plan as well as making stateful/stateless operators was expressive enough to support ANY stream operators (including window operators, which we made a demo of, but wasn't included in the benchmark script). Theoretically, users can migrate any Flink application to our system with corresponding modifications.

## System Evaluation

The system evaluation was done in an ad-hoc manner, and the results were not very conclusive. Looking backwards, we should
most probably have chosen the other way rather than implementing this prototype: simply modify Flink's state backend. We 
argued back then that the code was too complex to modify, but it's not like none of us had experience with such a large codebase.
In contrast to the prototype which had latency spikes all over the place, Flink's performance is much more stable and predictable,
which would have made the evaluation make much more sense. Additionally, Flink has mature built-in metrics collections that would
make things much easier. During our system evaluation, we spent most of the time attempting to arrive at a stable configuration,
and the results were still not very conclusive: as an example, we found one critical bug that caused our state backend to suffer severe 
performance degradation, rendering previous test results useless.

It turned out that since there were so many knobs to turn in such a stream processing system, it wasn't easy to find a
configuration that would work well for all the different stages. We ended up having to tune the parameters for a 
meaningful operator scale to take place and even then the results were not very conclusive.

## Team Takeaways

The JVM object model is quite interesting, and it's not hard to see why it's so popular. It's much more flexible than we
had in mind.

The team looked into various fields of distributed systems building-blocks: scheduling, container orchestration, embedded KV store, 
compute & storage separation, consistent hashing, watermarks, mini-batching, metrics monitoring with prometheus and so on.
These are valuable knowledge that we can apply to future projects.
