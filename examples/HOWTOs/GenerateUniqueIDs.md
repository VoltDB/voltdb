Generating Unique IDs in VoltDB
===============================

Most non-distributed SQL databases provide an easy way to generate a unique ID for use as a key, such as a primary key.  For example, MySQL has [AUTO_INCREMENT](http://dev.mysql.com/doc/refman/5.0/en/example-auto-increment.html), SQL Server has [IDENTITY](http://msdn.microsoft.com/en-us/library/ms186775.aspx), PostgreSQL has a [SERIAL](http://www.postgresql.org/docs/8.4/static/datatype-numeric.html#DATATYPE-SERIAL) datatype, and Oracle provides [SEQUENCE](http://docs.oracle.com/cd/B28359_01/server.111/b28286/statements_6015.htm#SQLRF01314) objects.

For distributed systems, generating unique incremental IDs is a much more difficult problem.  There's a good overview of the challenges in [this Stack Overflow discussion](http://stackoverflow.com/questions/2671858/distributed-sequence-number-generation).  There are also some popular open-source solutions:

- Twitter's [Snowflake](https://github.com/twitter/snowflake)
- Boundary's [Flake](http://boundary.com/blog/2012/01/12/flake-a-decentralized-k-ordered-unique-id-generator-in-erlang/)

However, you may not want to add one of these technologies to your stack, and if you are using VoltDB you can use it to generate Unique IDs with one of the following methods.

Use the built-in getUniqueID() method within a procedure
--------------------------------------------------------

VoltDB provides an implementation of a similar algorithm within the [VoltProcedure](http://voltdb.com/docs/javadoc/server-api/org/voltdb/VoltProcedure.html) API, so that within a stored procedure the [getUniqueID()](http://voltdb.com/docs/javadoc/server-api/org/voltdb/VoltProcedure.html#getUniqueId()) method can be called to provide one unique ID within each transaction.  The id value is a 64-bit java long integer that is generated when transaction is enqueued. The 64-bits are made up of the transaction timestamp, the partition, and a sequential counter.  This provides a deterministic value, so that the result will be the same on multiple servers in a k-safe cluster, or on a replica cluster, or when playing back the transaction when recovering from disk.

Here is an example schema (ddl.sql):

    CREATE TABLE example {
      id BIGINT NOT NULL,
      key1 VARCHAR(20),
      key2 VARCHAR(20),
      value VARCHAR(200),
      CONSTRAINT pk_example PRIMARY KEY (key1,id)
    };
    PARTITION TABLE example ON COLUMN key1;

    CREATE PROCEDURE FROM CLASS example.InsertExample;
    PARTITION PROCEDURE InsertExample ON TABLE example COLUMN key1 PARAMETER 0;

And the stored procedure (InsertExample.java):

    package example;
    import org.voltdb.*;

    public class InsertExample extends VoltProcedure {
        public final SQLStmt insert = new SQLStmt(
            "INSERT INTO example VALUES (?,?,?,?);");

        public VoltTable[] run(String key1,
                               String key2,
                               String value
                               ) throws VoltAbortException {
            voltQueueSQL(insert,
                         getUniqueId(),
                         key1,
                         key2,
                         value);
            return voltExecuteSQL(true);
        }
    }

It does provide only one value per transaction.  Repeated calls to the method with a transction will return the same value.

Use a table
-----------

If you want something closer to a sequence, where you can get sequential IDs like 1,2,3,... you can use a table to keep track of the latest value, and an ACID transaction to make sure you get a value that is unique.

To make this faster, you will want to use a partitioned table and a partitioned procedure. If you need multiple IDs, it will reduce the overhead even further to fetch a range of IDs at once, rather than making one procedure call for each ID.

Example schema:

    CREATE TABLE sequence {
      name VARCHAR(50) NOT NULL,
      last_id BIGINT NOT NULL,
      PRIMARY KEY (name)
    };
    PARTITION TABLE sequence ON COLUMN name;

    CREATE PROCEDURE PARTITION ON TABLE sequence COLUMN name FROM CLASS example.GetSequenceIDs;

GetSequenceIDs.java:

    package example;
    import org.voltdb.*;

    public class GetSequenceIDs extends VoltProcedure {

        public final SQLStmt select(
            "SELECT last_id + 1 AS initial_value,"+
            " last_id + ? AS last_value"+
            " FROM sequence WHERE name = ?;");

        public final SQLStmt update = new SQLStmt(
            "UPDATE sequence SET last_id = last_id + ? WHERE name = ?;");

        public VoltTable[] run(String name, long size) throws VoltAbortException {
            voltQueueSQL(select, size, name);
            voltQueueSQL(update, size, name);
            return voltExecuteSQL(true);
        }
    }

A simple usage (from a java client) would be:

    VoltTable t = client.callProcedure(GetSequenceIDs,1000,"example").getResults()[0];
    t.advanceRow();
    long initialValue = t.getLong(0);
    long lastValue = t.getLong(1);

    for (int i = initialValue; i<= lastValue; i++) {
        client.callProcedure(callback,"EXAMPLE.insert",i,...);
    }

If you needed to get the IDs more randomly than this, you could retrieve a set of values and put then into a LinkedList and remove them as needed. You could write a wrapper to automatically retrieve and add more IDs to the list when it is empty or when it gets low, so that you can then retrieve Unique IDs with a single method call within your application. A distributed application using this scheme would still be guaranteed that all the IDs are unique because of the transaction that generates them.
