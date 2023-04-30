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

## System Evaluation

The SinkOperator collects latency metrics and writes them to a file `source*.txt` in `pwd`. Each line of the file contains
the ingestion time and the latency of a single event. The latency is calculated as the difference between the ingestion 
time and the time it reaches the sink.

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

The design decisions were arguably correct for a proper system in production, but since we were only making a prototype, 
many of the parts really should have been simplified in hindsight. Nevertheless, everyone on the team learned a lot about
the challenges of building a distributed system. The JVM object model itself is quite interesting in that when actually designing
the deployment scheme, we find ourselves facing similar problems k8s and other container orchestration systems were dealing with. 
While we didn't have to actually address these issues, it was still a good learning experience looking at how Flink does it.

We made some parts of the system more complex than it should have, which caused problems in the later stages of the project.

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



[//]: # (## Intro)

[//]: # ()
[//]: # (In this prototype, we implemented a simple dataflow application that continuously read from Wikipedia edit history stream &#40;either from a URL or a local file&#41;, and count the number of events on each server name.)

[//]: # ()
[//]: # (Each operator is extended from a [SingleInputOperator]&#40;./dataflow/src/main/java/operators/SingleInputOperator.java&#41; abstract class. Each task is executed by a thread, and will communicate through ConcurrentLinkedQueue.)

[//]: # ()
[//]: # (- The source operator could be either [WikipediaFileSource]&#40;./dataflow/src/main/java/WikipediaFileSource.java&#41; or [WikipediaSource]&#40;./dataflow/src/main/java/WikipediaSource.java&#41;.)

[//]: # (- Then [JSonParserOperator]&#40;./dataflow/src/main/java/JSonParserOperator.java&#41; will parse the json records and key-by on `server_name` field.)

[//]: # (- Then [CountOperator]&#40;./dataflow/src/main/java/CountOperator.java&#41; count the number of records for each server.)

[//]: # ()
[//]: # (## How to extend)

[//]: # ()
[//]: # (Here is an example of the guideline to this project. But you're encouraged to come up with a different design :&#41;)

[//]: # ()
[//]: # (- Modify it so that operators can be executed by different processes that may be local or distributed. )

[//]: # (  - For example, you may start a TaskManager process on each worker, run the tasks that are scheduled on this worker, and communicate with the control plane in order to know where to forward state/messages.)

[//]: # (- Use a pub-sub system to serve as a control plane. Support scheduling between control plane and tasks.)

[//]: # (- Support parallelism to enable scale up / scale down.)

[//]: # (- Implement some basic operators &#40;like data source, data sink, map, filter, window&#41;.)

[//]: # (  - You can add more abstract class to abstract the common parts if necessary.)

[//]: # (- Enable stateful operators, e.g. by connecting your system with a key-value store.)

[//]: # (- Support event time and windows, if time allows. )

