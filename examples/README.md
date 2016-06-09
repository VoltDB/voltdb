# Examples Guide

VoltDB provides a number of examples to help people learn what is possible with VoltDB as well as how to use certain features. Some of these examples show specific patterns and individual features, while others are based on real-world applications. Many examples are simplified in the interest of making them better learning tools.

All of the examples in the download kit are written in Java. There are some examples that have JDBC client code in addition to VoltDB native client code, notably `voter` and `voltkv`. For examples in additional languages, download a client kit for a specific language.

These examples do not cover the complete set of VoltDB features or use cases; they are just a sample of what is possible with VoltDB. We do, however, expect to add additional examples and refinements over time. If you have feedback or suggestions related to examples, or any of the HOWTOs, send email to askanengineer@voltdb.com, we welcome your input.


HOWTOs
--------------------------

The HOWTOs directory provides short guides on how to use the examples to try different VoltDB features and configurations. For example, there are short guides to run an example in a three-node cluster and also with different durability settings. There is also a subdirectory of sample deployment files.


Example Projects
--------------------------

The following projects are included in the examples directory:

### adperformance ###

This application simulates a high velocity stream of impression, click-through, and conversions events that are enriched and ingested by VoltDB. Several materialized views maintain real-time aggregations on this data to provide a minutely summary for each advertiser as well as drill-down reports grouped by campaign to show detail-level metrics, costs and rates with real-time accuracy.

### bank-fraud ###

Ingest generated consumer purchase transaction data and use summary data from materialized views to evaluate against rules to detect possible fraud. The web dashboard shows recent alerts and their related transactions.

### bank-offers ###

Ingest simulated consumer purchase transaction data and find the best matching offer to present to the consumer at the point-of-sale.

Matching offers are found using a query that joins a summary view of the account activity with this vendor (merchant) against the available offers from that vendor. The best match is determined by the priority for the offers that was set by the vendor.

### callcenter ###

The Callcenter VoltDB application processes begin and end call events from a call center. Pair/join events in VoltDB create a definitive record of completed calls. Note that this app uses unrealistic call data -- the average call time is 5s by default -- to make the simulation interesting in a two-minute example run.

Use VoltDB's strong consistency and stored procedure logic to compute a running standard deviation on call length by agent. This is not a trivial thing to compute without strong consistency. Thankfully VoltDB provides strong consistency, making solving this problem much easier. The provided HTML dashboard shows a top-N list of agents by standard deviation.

### contentionmark ###

ContentionMark is a small VoltDB application that measures VoltDB throughput under extreme contention. By default, ContentionMark creates one row, then updates it as fast as VoltDB will let it, one transaction per update.

It's also the smallest example we have, for what it's worth!

### geospatial ###

This example demonstrates geospatial functionality that was added to VoltDB in version 6.0. The problem space for this demo is serving ads to many mobile device users based on their location, in real time.

### json-sessions ####

This example shows how to use flexible schema and JSON within a VoltDB application.

### metrocard ###

This application performs high velocity transaction processing for metro cards. These transactions include card swipes and new card generation.

### nbbo ###

NBBO is the National Best Bid and Offer, defined as the lowest available ask price and highest available bid price across the participating markets for a given security. Brokers should route trade orders to the market with the best price, and by law must guarantee customers the best available price.

The example includes a web dashboard that shows the real-time NBBO for a security and the latest available prices from each exchange. 

### positionkeeper ###

This application simulates a simple position keeper application that maintains the positions of portfolios that are updated frequently as trades and price changes occur.

### uniquedevices ###

This example counts unique device ids as they stream into VoltDB. It demonstrates VoltDB's stream processing power and the use of external libraries in Java stored procedures. It is loosely based on a real problem previously solved using the Lambda architecture.

### voltkv ###

This directory contains the sources for a "Key-Value" implementation in VoltDB along with a simple benchmark.

VoltKV includes synchronous, asynchronous and JDBC versions of the client code.

### voter ###
This example is a simulation of a telephone based voting process where callers are allowed a limited number of votes.

Voter includes synchronous, asynchronous and JDBC versions of the client code.

### windowing ###

This example shows an application that takes in time-series data, runs analytic queries over several time-windows, and deletes older data as it either ages out or passes a rowcount threshold.
 
### windowing-with-ddl ###

A modified version of the windowing example that demonstrates the V5 feature that lets users define how to age out old tuples using a special row-count constraint on the table definition. This example has no Java stored procedures.


Getting Started
--------------------------

Each example contains a README.md file with full instructions for running, building, etc.

The examples all contain a bash script file named "run.sh" that contains functions for doing most things. This is generally only strictly needed for running the client apps, but can be a useful reference for other operations.

The following steps will run an example:

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create --force" to start an empty, single-node VoltDB server.
3. Open a second terminal window in the same working directory.
4. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
5. Type "./run.sh client" to run the client code.

Typing "./run.sh help" will also show the available targets for each example.

When finished, hit Ctrl-C in the server terminal window to stop the VoltDB database.


