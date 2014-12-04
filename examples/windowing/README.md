Windowing Example Application
==============

This app does four simultaneous things on a single-table schema:

* Insert random, timestamped tuples at a high rate.
* Continuously delete tuples that are either too old or over a table size limit.
* Check for changes in the maximum value stored in the table.
* Periodically compute an average of values over various time windows.

It does this by creating task-focused classes that implement Runnable.
Each class has a specific job and is scheduled to run periodically in a
threadpool. All inter-task communication is done via the main instance of
this class.


Quickstart
--------------
VoltDB Examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

The default settings for the client have it keep 30 seconds worth of tuples, deleting older tuples as an ongoing process. See the section below on *run.sh Client Options* for how to run in other modes.

Note that the downloaded VoltDB kits include pre-compiled stored procedures and client code as jarfiles. To run the example from a source build, it may be necessary to compile the Java source code by typing "run.sh jars" before step 3 above. Note that this step requires a full Java JDK.


Other run.sh Actions
--------------
- *run.sh* : start the server
- *run.sh server* : start the server
- *run.sh init* : load the schema and stored procedures
- *run.sh jars* : compile all Java clients and stored procedures into two Java jarfiles
- *run.sh client* : start the client
- *run.sh clean* : remove compilation and runtime artifacts
- *run.sh cleanall* : remove compilation and runtime artifacts *and* the two included jarfiles


run.sh Client Options
--------------
Near the bottom of the run.sh bash script is the section run when you type `run.sh client`. In that section is the actual shell command to run the client code, reproduced below:

    java -classpath client:$CLIENTCLASSPATH -Dlog4j.configuration=file://$LOG4J \
        windowing.WindowingApp \
        --displayinterval=5 \              # how often to print the report
        --duration=120 \                   # how long to run for
        --servers=localhost:21212 \        # servers to connect to
        --maxrows=0 \                      # set to nonzero to limit by rowcount
        --historyseconds=30 \              # set to nonzero to limit by age
        --inline=false \                   # set to true to delete co-txn with inserts
        --deletechunksize=100 \            # target max number of rows to delete per txn
        --deleteyieldtime=100 \            # time to wait between non-inline deletes
        --ratelimit=15000                  # rate limit for random inserts


Changing these settings changes the behavior of the app. The three key options that change the *mode* the app runs in are *maxrows*, *historyseconds* and *inline*.

- If *maxrows* is non-zero, then the app will try to keep the most recent *maxrows* in the database by deleting older rows as newer rows are added.
- If *historyseconds* is non-zero, then the app will try to keep rows from the most recent *historyseconds* seconds in the database by deleting rows as they age out.
- The app won't start if both *historyseconds* and *maxrows* are non-zero. It wouldn't be too hard to support both constraints, but this functionality is omitted from this example avoid further complexity.
- If *inline* mode is enabled, the app will delete rows in the same transaction that it inserts them in. If not enabled, the app will delete rows as an independant process from inserting rows. Both operations could even be broken into separate unix processes and run separately without much additional work.


What do the Queries Do?
--------------

There are two primary kinds of read operations being run by this app.

### Tracking the Maximum Value ###

The first query computes the maximum value in the table every 10 milliseconds. If the value has changed since the previous check, a line is printed to the console announcing the new maximum. Since the data in the database represents a moving window of tuples, the maximum can go up when large values are added, or it can go down when large values are deleted.

There are two interesting things here. First, the query leverages an index to compute the maximum value without a scan. By default, indexes in VoltDB are tree-based and ordered. So "get the maximum value" is fast, but so is "get the top 10 values". Futhermore, there is support for ranking built into the index, so often "get the 1234th largest value" and "given value X, find the ten adjacent values higher and lower than X" are also fast queries without scans. Any time you add an index to a column in a VoltDB table, you can access queries based on the sorted column values cheaply. Because VoltDB is ACID-compiliant, you can have multiple indexes and they will always be perfectly in sync with updates to the base table.

Second, this query is a cross-partition query, asking for the maximum value across *all* rows, not just rows for a specific partition. What VoltDB is going to do under the covers is find the maximal value at each partition, then scan these collected values to find the largest. The actual computational work being done can likely still be measured in microseconds.

So what about the coordination overhead of a distributed, consistent read? We're happy to say that in VoltDB 4.0, we changed how distributed read transactions are coordinated. They still see an ACID, serializable view of the data, but there is no global block to start the transaction and there is no two-phase-commit to finish it. In this case, since the read can be satisifed in a single round trip to all partitions, there is no blocking anywhere during its execution. Since it needs to execute at all partitions, this query's performance won't scale much as you add nodes, but we've benchmarked this kind of query in the tens of thousands of transactional reads per second. The client's rate of 100 per second should have negligable impact on other work in the system.

### Computing the Average Value Over Windows of Time ###

The second kind of query computes the average values in the table over windows of time. Specifically, it looks at the last 1, 5, 10 and 30 seconds worth of values.

Lets say tuples are being inserted at a rate of 15k/sec and there are 4 partitions. To compute the average for the last 10 seconds, VoltDB would need to scan 150k rows. 150k is not outlandish for an in memory system, but it would likely still require many milliseconds of processing. Futhermore, these example numbers of 15k inserts/second and 10 second windows could easily be higher.

By adding a materialized view to the schema, we can reduce the amount of work this query has to do by several orders of magitude while still having flexibily to query over these differing window sizes. The view pre-aggregates the sum of values and count of rows for each second at each partition. Using the materialized view, the query can be answered by scanning 1 row for each of 10 seconds at N partitions. If you have 10 partitions, then this query will scan 100 rows instead of 150k. Here's the SQL to do that:

    SELECT SUM(sum_values) / SUM(count_values)
    FROM agg_by_second
    WHERE second_ts >= TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - ?);

Note that the reporting sends 5 different procedure calls to VoltDB to get the 5 different averages for the windows. Each of these calls are read-only, fast to execute, and require one round trip to all partitions, just like the max value query. Thus they benefit from the VoltDB 4.0 optimizations described in the previous section and can run at high rates with minimal impact to other work. In fact, all 5 queries could easily be bundled into the same round trip by building a Java procedure and making 5 calls to `voltQueueSQL(..)` followed by a single call to `voltExecuteSQL(..)`. That would send all 5 SQL statements to the partitions using the same round trip and would avoid any blocking.

Currently, the distribution of random data is pretty boring, just a gaussian with a mean of zero. Therefore the average for all windows is close to zero, with less variance as the window gets bigger. That may be improved as we improve this example.


How do the Deletes Work?
--------------

The easiest way to delete all rows that are older than 30 seconds would be to run the following query:

    DELETE FROM timedata
    WHERE SINCE_EPOCH(SECOND, update_ts) < (SINCE_EPOCH(SECOND, NOW) - 30);`

The issue with this query is that the number of rows to delete is unbounded.
A query that ended up deleting a half million rows might take hundreds of milliseconds, while using extra memory to store all of the necessary *undo* information to make that operation transactional.

A better way to achieve the same goal is to use the "Nibble" pattern: create a procedure that deletes a small number of rows that meet the deletion criteria, then call that procedure repeatedly until all desired rows are removed. This query will execute in bounded time and memory usage. The faster execution allows other operations to be run in between calls to the deleting procedure, allowing for workload concurrency. The easy way to do that might be add `LIMIT 1000` to the query above and call it repeatedly until it returns that it modified 0 tuples.

VoltDB doesn't currently support "LIMIT" in DML (SQL that modifies tuples). This is primarily to enforce determinism. Since all queries may be run multiple times, they need to do exactly the same thing each time they are run. This applies to synchronous intra-cluster replication, command-log replay for recovery and even for WAN replication.

To perform this operation deterministically, we break the delete into two steps. First, find the timestamp value for the 1000th oldest tuple. We do that with this query:

`SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET 1000 LIMIT 1;`

Then, we delete all rows with timestamps at least as old as the retrieved timestamp, and also older than original 30s age target:

    DELETE FROM timedata
    WHERE SINCE_EPOCH(SECOND, update_ts) < (SINCE_EPOCH(SECOND, NOW) - 30)
    AND update_ts <= ?;`

This will always delete oldest tuples first, and it will always delete an identical set of tuples. Note that it might delete more than 1000 tuples if the 1000th oldest timstamp is non-unique. In the worst case, this will delete all rows if all timestamps are identical. The alternative is to use strictly less than when comparing candidates to the 1000th oldest timstamp. That might delete 0 rows in the worst case. In this example we err on the side of making progress and consider millions of duplicate timestamps to be an outlier case.

So if you look at the provided *DeleteAfterDate* procedure, you will see essentially this code, though the logic is a bit more complex and there is additional error code.

### Making Deletes Partitionable ###

Here is the schema of the primary table:

    CREATE TABLE timedata
    (
      uuid VARCHAR(36) NOT NULL,
      val BIGINT NOT NULL,
      update_ts TIMESTAMP NOT NULL
    );
    PARTITION TABLE timedata ON COLUMN uuid;

Let's assume we have 10 partitions. With the table partitioned on the UUID column, deleting old tuples doesn't seem like it would partition well. The approach we're going to take is to delete the oldest 100 tuples at each of the partitions by sending 10 single-partition transactions. We call this the "Run Everywhere" pattern.

Typically, we direct procedure invocations to partitions by assuming a paramter is of the same type as a partitioning column. We hash the value and send it to the appropriate partition for this hash. If you just want to send a procedure invocation to a specific partition, you can use the system procedure "@GetPartitionKeys". This system procedure returns one row for each partition, and each row will contain a partition id integer and a partitioning dummy value. This dummy value can be used as a parameter to send a procedure to the associated partition.

So in this case, a call to "@GetPartitionKeys" will return 10 rows. We can call our nibbling delete procedure once for each returned row and its dummy partitioning value. In this way, we can run our procedure over all 10 partitions, but there is no cross-partition transactional overhead.

Note that this is safe to run even if the server's partition count is being elastically expanded. For a brief window, the new partition(s) may not be included, but if "@GetPartitionKeys" is called on a regular basis, that will only be temporary. The example code provided calls "@GetPartitionKeys" once per second.

There is more on this pattern here:
[Dev Center: Run Everywhere](http://voltdb.com/dev-center/cookbook/run-everywhere/)

### Inline vs Non-Inline Deleting ###

Inline mode deletes tuples co-transactionally with inserts. In a stored procedure, first the insert is performed, then any rows that have aged out or passed the maximum row target are deleted. With a regular stream of inserts, about 1 row should be deleted for every insert.

The alternative mode uses naive tuple insertion, and periodically calls a dedicated delete stored procedure.

Inline Pros:
* Maintaining row-count targets is a very good fit. Deletes should be close to 1-1, depending on how balanced the partitioning is.
* No concern about how often to run a delete process.
* No need to use the "Run Everywhere" pattern to avoid cross-partition writes.

Non-Inline Pros:
* Logically separates the insertion process and the age/rowcount maintenance. Inserts can be run by client apps while an administrator can maintain the deleting process. Inserts can even be coded in a different language. For example, inserts can use the CSV loader without any special configuration.
* Total amount of querying may be lower, as the delete process might run 10-100x less often than the insert process.
* Can continue to delete aged-out tuples even if the insert process is irregular or paused.
