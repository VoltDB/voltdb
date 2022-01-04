# Using the Client2 API for asynchrononous applications

## Introduction

VoltDB V11 introduces an updated version of the Java client API, referred to as Client2. The goals of Client2 include a focus on asynchronous operation with use of familiar Java programming constructs. This document describes the recommended way of using Client2.

The Client2 API has synchronous and asynchronous variants of most calls, clearly distinguished by the suffix 'Sync' or 'Async'. Here we're primarily concerned with the latter.

This HOWTO is best read in conjunction with the example program that can be found in file [AsyncFlowControl.java](../client2/AsyncFlowControl.java), and which illustrates the points made in the present document.


## Use of CompletableFuture

Asynchronous methods of the Client2 API all return a Java [CompletableFuture](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html), which allows the calling program to then wait for completion, to get the result of the operation when it has completed, or specify some action to be taken on completion.

As an example, the following sequence would execute a VoltDB stored procedure and, on completion, hand off the response to a processing thread:

```
client.callProcedureAsync("myProc", "this", "that")
      .thenAcceptAsync((resp) -> processResponse(resp))
      .exceptionally((th) -> processException(th));
```

This is non-blocking; callProcedureAsync returns as soon as the procedure call has been queued in the Client2 API.

## Application flow control

Since the Client2 API queues requested VoltDB procedure calls internally and immediately returns to the application, it should be clear that the application can easily outrun the capacity, of the network connection and the VoltDB server, to process those requests. It is therefore essential that we have some way to control the flow of requests.

If the network connection indicates backpressure, this is handled within Client2, unlike in the older Java API. The application is not involved; instead, Client2 will hold requests in its own queues until network backpressure ceases.

However, the internal Client2 queues should not be allowed to grow without limit, since queued requests are consuming memory, and the increasing queue size will increase the end-to-end latency of subsequent requests. Client2 therefore introduces the notion of 'request backpressure'. We track the total number of requests that have been made (by callProcedureAsync) and which have not yet been completed. Two limits are imposed on this count, informally referred to as 'yellow' and 'red' limits. Both limits are configurable by the application.

If the yellow limit is exceeded, then the application will get a notification (a call to its backpressure handler), in effect telling it to slow down. The application should do whatever is appropriate for it. It can however continue to queue requests, which will be accepted and queued in the usual way. If the application continues to issue procedure calls to the extent that the red limit would be exceeded, then those requests will be rejected (with a specific exception type, RequestLimitException) rather than queued.

Request backpressure ceases, with the application being notified by another call to its backpressure handler, when the queue length falls sufficiently low. That level is also configurable by the application.

## Notifications

The client application can receive notifications of certain events by configuring 'handlers' for specific notification types. All such handlers are Java [functional interfaces](https://docs.oracle.com/javase/8/docs/api/java/util/function/package-summary.html#package.description), and are defined in class Client2Notification.

Because the handler is defined as a functional interface, the application can supply any method that takes the appropriate argument types. For the backpressure handler mentioned above, the method must take a single boolean argument and return nothing. Client2Notification defines the interface thus:

```
@FunctionalInterface
public static interface Client2Notification.RequestBackpressure {
    void accept(boolean slowdown);
}
```

The 'slowdown' argument will be true for notification of backpressure starting, and false for backpressure ending.

## Configuration

The configuration for a Client2 API instance is provided by a Client2Config object. The javadoc for the Client2Config class lists all settable values; this example shows those that are relevant to the above discussion of flow control.


```
// Configuration related to flow control. Backpressure
// begins at 800 outstanding requests, ends when the
// level falls to 200, and there is a hard limit at 1000.
Client2Config config = new Client2Config()
        .clientRequestBackpressureLevel(800, 200)
        .clientRequestLimit(1000)
        .requestBackpressureHandler(this::backpressure);
```

The configuration is used to create Client2 instances:

```
Client2 client = ClientFactory.createClient(config);
```

## Request priorities

The Client2 API supports setting the priority of a request. The priority is an integer from 1 (highest) to 8 (lowest). A default value can be set via the configuration, and if not explicitly set, will default to 4.

Additionally, an override of the default can be set as a per-call option; see below.

The priority value is used in two ways: firstly, to determine where a request is inserted into the internal Client2 request queue, and secondly, to determine the dispatch order at the VoltDB server. Server handling must be explicitly enabled at server initialization -- by default it ignores requested priorities.

## Timeouts

The Client2 API supports setting a timeout for a request. A default value can be set via the configuration, and if not explictly set, will default to 2 minutes, as with the older client API.

Additionally, an override of the default can be set as a per-call option:

```
Client2CallOptions opts = new Client2CallOptions().clientTimeout(12345, TimeUnit.MICROSECONDS)
                                                  .requestPriority(3);
future = callProcedureAsync(opts, "SomeProc", "someParam");
```

A request may be timed out either when the request is queued in the Client2 API, awaiting handover to the network code, or when the request is queued in the VoltDB server, awaiting dispatch. In both of those cases, it is known that no database changes have occurred. The response status -- CLIENT_REQUEST_TIMEOUT or GRACEFUL_FAILURE respectively -- indicates this.

Additionally, the response to the request may be timed out by the Client2 API, in the case that the procedure call has been handed over to the network code, but no response has been delivered from the server to the Client2 API. We cannot know at the timeout whether any action was taken on the request; the response status will be CLIENT_RESPONSE_TIMEOUT. The application may subsequently receive a 'late response notification' to indicate what happened.

## A note on latency statistics

Client2 supports the same ClientStats information as the older Java client (see the createStatsContext method). Be aware that the round-trip latency figures are end-to-end times. That is, they are measured from the time the procedure call enters the Client2 API from the application, until the time that the response is delivered to the application.

If the application is sending requests at a sufficiently high rate that queueing occurs in the API, then reported latencies include time spent waiting in the queue. This gives a true picture of the request-response time as seen by the application, but it does make it difficult to compare performance with the older API.
