Windowing Example Using DDL
==============

This app is a modified form of the "windowing" app, also in the examples directory.  It does the following on a single-table schema:

* Insert random, timestamped tuples at a high rate.
* Each partition of the table is capped at a fixed number of tuples. When the tuple limit is met, space is automatically made by deleting the oldest tuples.
* Periodically compute an average over various time windows.
* Check for changes in the maximum value stored in the table.

Here's the difference between this app and the "windowing" app: here we use a new version 5.0 feature that lets users define how to age out old tuples by using a special constraint in the table definition.  No Java stored procedures are needed in this example.


Quickstart
--------------
VoltDB Examples come with a run.sh script that sets up some environment and saves some of the typing needed to work with Java clients. It should be fairly readable to show what is precisely being run to accomplish a given task.

1. Make sure "bin" inside the VoltDB kit is in your path.
2. Type "voltdb create" to start an empty, single-node VoltDB server.
3. Type "sqlcmd < ddl.sql" to load the schema and the jarfile of procedures into VoltDB.
4. Type "./run.sh client" to run the client code.

The default settings for the client have it running for 2 minutes, and inserting at rows at a rate such that the table can comfortably hold 30s of tuples without any inserts hitting the row limit and failing.

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
        ddlwindowing.WindowingApp \
        --displayinterval=5 \              # how often to print the report
        --duration=120 \                   # how long to run for
        --servers=localhost:21212 \        # servers to connect to
        --ratelimit=20000                  # rate limit for random inserts

Changing these settings changes the behavior of the app.


How does the EXECUTE action of a LIMIT PARTITION ROWS constraint work?
--------------

Rows are deleted automatically by a trigger defined in the LIMIT PARTITION ROWS constraint in the table definition:

    CREATE TABLE timedata
    (
      uuid VARCHAR(36) NOT NULL,
      val BIGINT NOT NULL,
      update_ts TIMESTAMP NOT NULL,
      CONSTRAINT update_ts_uuid_unique UNIQUE (update_ts, uuid),
      CONSTRAINT row_limit LIMIT PARTITION ROWS 82500
        EXECUTE (DELETE FROM timedata
                 WHERE update_ts
                       < TO_TIMESTAMP(SECOND, SINCE_EPOCH(SECOND, NOW) - 30)
                 ORDER BY update_ts, uuid LIMIT 1500)
    );
    PARTITION TABLE timedata ON COLUMN uuid;

The constraint caps each partition of the table to 82,500 rows, and also says to execute a DELETE statement if an insert would cause the table to exceed the cap.  In this case, the DELETE statement says to get rid of the oldest rows that are older than 30 seconds, but not more than 1,500 rows at one go.  Being able to define a DELETE statement that helps to enforce a LIMIT PARTITON ROWS constraint is a new feature in version 5.0.

If the insertion rate is 20k rows per second, then 600k rows are produced in 30 seconds.  A cluster with one host and 8 sites per host would need to store about 75k rows per partition.  We allow a little bit of leeway to ensure that we never fail to clear out space for new tuples and cap each partition of the table at 82,500 rows.  These settings (insertion rate of 20k per second and an 8-site configuration) are the default for this application.

It's important to note that the DELETE statement triggered by a LIMIT PARTITION ROWS constraint is executed within the context of a single partition.  Any individual invocation of the statement deletes the oldest rows on *one* partition only.  But since rows are being inserted at a high rate, and we expect the hashing function to distribute the rows evenly across all partitions, the net effect of the DELETE statement is to age out old rows in the table as a whole.

The DELETE statement makes use of an ORDER BY and LIMIT clause, which is also newly supported in version 5.0. In order to support VoltDB features like replication and command log replay, any data manipulation must be executed deterministically, such that the effect is identical across multiple executions of the statement.  Therefore, it's a requirement of DELETE with ORDER BY and LIMIT that the ORDER BY clause defines a unique ordering.

To address this requirement in this example, we've defined a UNIQUE constraint on `update_ts` and `uuid`.  The UNIQUE constraint is implemented as a tree index under the hood and achieves two things: it ensures that the DELETE is deterministic, and also provides an efficient way to evaluate the WHERE and ORDER BY of the DELETE without resorting to doing a sequential scan and sorting all of the rows.

The reason that we want to limit the number of rows deleted at once is that large deletes place a strain on resources: they take longer to execute, and they require extra memory to store all of the necessary *undo* information in case the transaction needs to be rolled back.

This way of purging old historical data from a table is easy for the user, since it doesn't require any application code to be written.  In addition, it can be done very fast because the DELETE is invoked directly from the execution engine as soon as it detects that more space is needed.

### What are the Implications of Insertion Rate? ###

Since the deletion of old rows is tied to the insertion of new ones, the rate at which new rows are inserted has implications that are worth discussing.

When the insertion rate is slow, there may be stale data in the table.  Older data is not purged from the table until the new data arrives to push it out.  This may not be desirable, but isn't the end of the world either; just be sure to select only the newer rows when reading the table.

When the insertion rate becomes faster than is appropriate for the current row limit, two things may happen, depending on deletion criteria.  If only old rows are deleted, then inserts may begin to fail with a constraint violation if space cannot be made for new rows---new data arrives, but none of the existing tuples are yet old enough to be aged out.  If we make the delete less selective, such that it just deletes some fixed number of the oldest rows (without requiring them to be older than a certain age), then we may age out rows that are younger than the time window that we care about.

The database administrator has a couple options when this happens.  He or she can increase the per-partition row limit for the table (and accept the increased memory cost that this implies).  In version 5.0, this can be done on a running database by executing the `ALTER TABLE ... ADD CONSTRAINT` command.  Alternatively, the adminstrator could expand the cluster by adding new nodes, as more partitions means fewer rows per partition.  Elastically expanding a cluster can also be done online, without shutting down the database.

To see how insertion rate affects the performance of this example app, try playing with the `--ratelimit` setting in the client.

Another way to delete older data is via the "nibble" pattern, wherein a procedure is created that deletes small amounts of rows.  This procedure can then be run periodically to purge unneeded rows.  One advantage of this approach is that new tuples do not need to be inserted to purge older ones.  A discussion of this pattern appears in the README for the "windowing" example.


What do the Queries Do?
--------------

There are two primary kinds of read operations being run by this app: tracking the maximum value in the table, and computing the average value of windows over time.  For an explanation on how these functions work, see the "windowing" example located in `examples/windowing` in this VoltDB installation.
