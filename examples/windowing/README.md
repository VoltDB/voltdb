# Windowing Example Application #

***

## Overview ##

App that does four simultaneous things on a single-table schema:

* Insert random, timestamped tuples at a high rate.
* Continuously delete tuples that are either too old or over a table size limit.
* Check for changes in the maximum value stored in the table.
* Periodically compute an average of values over various time windows.

It does this by creating task-focused classes that implement Runnable.
Each class has a specific job and is scheduled to run periodically in a
threadpool. All inter-task communication is done via the main instance of
this class.

***

## How to Run ##

To run the VoltDB server with the Windowing application (schema & procedures) loaded, open a terminal, change to the windowing example directory and type `run.sh server`. You can also just type `run.sh` as "server" is the default target.

To run the Windowing client side code, open a second terminal in the Windowing example directory and type `run.sh client`.

The default settings for the client have it keep 30s worth of tuples, deleting older tuples as an ongoing process. See the section below on *run.sh Client Options* for how to run in other modes.

### run.sh Actions ###

    run.sh               : compile all Java clients and stored procedures,
                           build the catalog, and start the server
    run.sh srccompile    : compile all Java clients and stored procedures
    run.sh server        : start the server
    run.sh client        : start the client
    run.sh catalog       : build the catalog
    run.sh clean         : remove compiled files and artifacts

### run.sh Client Options ###

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
}

Changing these settings changes the behavior of the app. The three key options that change the *mode* the app runs in are *maxrows*, *historyseconds* and *inline*. 

* If *maxrows* is non-zero, then the app will try to keep the most recent *maxrows* in the system by deleting older rows as newer rows are added.
* If *historyseconds* is non-zero, then the app will try to keep row from the most recent *historyseconds* seconds in the system by deleting rows as they age-out.
* The app won't start if both *historyseconds* and *maxrows* are non-zero. It wouldn't be too hard to support both constraints, but this functionality is omitted from this example avoid further complexity.
* If *inline* mode is enabled, the app will delete rows in the same transaction that it inserts them in. If not enabled, the app will delete rows as an independant process from inserting rows. Both processes could even be broken into separate unix processes and run separately without much additional work.

***

## What do the Queries Do? ##

***

## How do the Deletes Work? ##

The easiest way to delete all rows that are older than 30s would be to run the following query:

`DELETE FROM timedata WHERE SINCE_EPOCH(SECOND, update_ts) < (SINCE_EPOCH(SECOND, NOW) - 30);`

The issue with this query is that the number of rows to delete is unbounded. 
A query that ended up deleted a half million rows might take hundreds of milliseconds, while using extra memory to store all of the necessary undo information to make that operation transactional.

A better way to achieve the same goal is to use the "Nibble" pattern. Create a procedure that deletes a small number of rows that meet the deletion criteria, then call that procedure repeatedly until all desired rows are removed. This query will execute in bounded time and memory usage. The faster execution allows other operations to be run inbetween calls to the deleting procedure, allowing for workload concurrency. The easy way to do that might be add `LIMIT 1000` to the query above and call it repeatedly until it returns that it modified 0 tuples. 

VoltDB doesn't currently support "LIMIT" in DML (SQL that modifies tuples). This is primarily to enforce determinism. Since all queries may be run multiple times, they need to do exactly the same thing each time they are run. This applies to synchronous intra-cluster replication, command-log replay for recovery and even for WAN replication.

To perform this operation deterministically, we break the delete into two steps. First, find the timestamp value for the 100th oldest tuple. We do that with this query:

`SELECT update_ts FROM timedata ORDER BY update_ts ASC OFFSET 1000 LIMIT 1;`

Then, we delete all rows with timestamps at least as old as the retrieved timestamp, and also older than original 30s age target:

`DELETE FROM timedata WHERE SINCE_EPOCH(SECOND, update_ts) < (SINCE_EPOCH(SECOND, NOW) - 30) AND update_ts <= ?;`

This will always delete oldest tuples first, and it will always delete an identical set of tuples. Note that it migth delete more than 1000 tuples if the 1000th oldest timstamp is non-unique. In the worst case, this will delete all rows if all timestamps are identical. The alternative is to use strictly less than when comparing candidates to the 1000th oldest timstamp. That might delete 0 rows in the worst case. In this example we err on the side of making progress and consider millions of duplicate timestamps to be an outlier case.

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

Let's assume we have 10 partitions. So if the table is partitioned on the UUID column, deleting old tuples doesn't seem like it would partition well. The approach we're going to take is to delete the oldest 100 tuples at each of the partitions by sending 10 single-partition transactions. We call this the "Run Everywhere" pattern.

Typically, we direct procedure invocations to partitions by assuming a paramter is of the same type as a partitioning column. We hash the value and send it to the appropriate partition for this hash. If you just want to send a procedure invocation to a specific partition, you can use the system procedure "@GetPartitionKeys". This system procedure returns one row for each partition, and each row will contain a partition id integer and a partitioning dummy value. This dummy value can be used as a parameter to send a procedure to the associated partition.

So in this case, a call to "@GetPartitionKeys" will return 10 rows. We can call our nibbling delete procedure once for each returned row and it's dummy partitioning value. In this way, we can run our procedure over all 10 partitions, but there is no cross-partition transactional overhead.

Note that this is safe to run even if the server's partition count is being elastically expanded. For a brief window, the new partition(s) may not be included, but if "@GetPartitionKeys" is called on a regular basis, that will only be temporary. The example code provided calls "@GetPartitionKeys" once per second.

There is more on this pattern here:
[Dev Center: Run Everywhere](http://voltdb.com/dev-center/cookbook/run-everywhere/)

### Inline vs Non-Inline Deleting ###

Inline mode deletes tuples co-transactionally with inserts. In a stored procedure, first the insert is performed, then any rows that have aged-out or passed the maximum row target are deleted. With a regular stream of inserts, about 1 row should be deleted for every insert.

The alternative mode using naive tuple insertion, and periodically calls a dedicated delete stored procedure.

Inline Pros:
* Maintaining row-count targets is a very good fit. Deletes should be close to 1-1, depending on how balanced the partitioning is.
* No concern about how often to run a delete process.
* No need to use the "Run Everywhere" pattern to avoid cross-partition writes.

Non-Inline Pros:
* Logically separates the insertion process and the age/rowcount maintenance. Inserts can be run by client apps while an administrator can maintain the deleting process. Inserts can be coded in a different language. Inserts can use the CSV loader without any special configuration.
* Total amount of querying may be lower, as the delete process might run 10-100x less often than the insert process.
* Can continue to delete aged-out tuples even if the insert process is irregular or paused.
