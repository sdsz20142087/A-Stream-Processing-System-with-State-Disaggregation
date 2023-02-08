# A Stream Processing System with State Disaggregation

This is a simple Java-based prototype that implements a multi-thread dataflow.

## Intro

In this prototype, we implemented a simple dataflow application that continuously read from Wikipedia edit history stream (either from a URL or a local file), and count the number of events on each server name.

Each operator is extended from a [SingleInputOperator](./dataflow/src/main/java/operators/SingleInputOperator.java) abstract class. Each task is executed by a thread, and will communicate through ConcurrentLinkedQueue.

- The source operator could be either [WikipediaFileSource](./dataflow/src/main/java/WikipediaFileSource.java) or [WikipediaSource](./dataflow/src/main/java/WikipediaSource.java).
- Then [JSonParserOperator](./dataflow/src/main/java/JSonParserOperator.java) will parse the json records and key-by on `server_name` field.
- Then [CountOperator](./dataflow/src/main/java/CountOperator.java) count the number of records for each server.

## How to extend

Here is an example of the guideline to this project. But you're encouraged to come up with a different design :)

- Modify it so that operators can be executed by different processes that may be local or distributed. 
  - For example, you may start a TaskManager process on each worker, run the tasks that are scheduled on this worker, and communicate with the control plane in order to know where to forward state/messages.
- Use a pub-sub system to serve as a control plane. Support scheduling between control plane and tasks.
- Support parallelism to enable scale up / scale down.
- Implement some basic operators (like data source, data sink, map, filter, window).
  - You can add more abstract class to abstract the common parts if necessary.
- Enable stateful operators, e.g. by connecting your system with a key-value store.
- Support event time and windows, if time allows. 

