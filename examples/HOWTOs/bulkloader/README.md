VoltDB Bulkloader API Example
=============================

This example shows how to use the BulkLoader API from within a Java client application to bulk-load batches of rows into a table for higher throughput. The BulkLoader automatically determines the partition each record will be stored in, and builds batches of records that can be inserted together within the same partition. It automatically sends these batches to the database. If a batch fails, it automatically inserts the records from the batch individually, and invokes a failureCallback() method in the callback for the rows that still fail to insert.

Due to the automatic batching of this interface, it is generally designed to be used for bulk-loading static data or streaming data where some additional delay for batching the records can be tolerated.

Using the Bulkloader API
------------------------

To use the Bulkloader API in a Java client, you first need to create a Client instance and connect it to the cluster, for example:

    org.voltdb.client.Client client;
    client = ClientFactory.createClient();
    client.createConnection("localhost");


Then what you need is a callback class to handle errors, but more on that later. You create a Bulkloader object using the getNewBulkLoader() method on the Client. A BulkLoader object is specific to a table. You can set a batch size and provide a callback. [Client.getNewBulkLoader() Javadoc](https://docs.voltdb.com/javadoc/java-client-api/org/voltdb/client/Client.html#getNewBulkLoader-java.lang.String-int-org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack-)


    String table = "app_session";
    int batchSize = 1000;
    BulkLoaderFailureCallBack callback = new SessionBulkloaderFailureCallback(); // more on this later
    VoltBulkLoader bulkLoader = client.getNewBulkLoader(table, batchSize, callback);

Once you have a BulkLoader, you can insert a row by calling insertRow(Object rowHandle, Object[] row). The rowHandle can be any object that you want to use in the callback later to identify the row in question. In this case we're using an Integer based on a number, which could be a counter. But it can be any object you want. The Object[] row is the data you want to insert into the table. It must be in the same order as the columns in the table are defined.

    // load a row
    Integer rowId = new Integer(0);
    Object[] row = {1, 1, new TimestampType()};
    bulkLoader.insertRow(rowId, row);

This can be called repeatedly, and the bulkLoader will automatically handle batching the rows by the partition they will be stored in, and sending the batches to the database to be inserted.

When you are through inserting rows, it is important to call the drain() method so that the bulkLoader can send off any partial batches that it was in the process of accumulating. You may also want to call client.drain() to make the client wait for responses to all pending requests, to ensure everything was processed before shutting down.

    bulkLoader.drain();
    client.drain();

The callback class is to handle errors if a row cannot be inserted. Here is a simple example that just prints the status message from the response, but you could define different callbacks for different tables, and examine or log the bad record in some way, using the rowHandle and fieldList parameters, which are the same ones passed in to insertRow() earlier.

    public static class SessionBulkloaderFailureCallback implements BulkLoaderFailureCallBack {
        @Override
        public void failureCallback(Object rowHandle, Object[] fieldList, ClientResponse cr) {
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(cr.getStatusString());
            }
        }
    }

You can also create a BulkLoaderSuccessCallback if you want to count the number of rows that were successfully loaded. Look in the ExampleApp.java code to see this variation.


## Example App

This directory contains a working example consisting of the following files:

    ddl.sql
    ExampleApp.java
    run_client.sh

### Usage:

First, load the schema into your running database. This will create a table called APP_SESSION.

    sqlcmd < ddl.sql

Then use the run_client.sh bash script to compile and run ExampleApp.java. It will connect to localhost and load 1M records into the table, first using the default insert procedure, then using the bulk loader.

    ./run_client.sh

Example output:

    Benchmarking APP_SESSION.insert procedure calls...
    Loaded 1000000 records in 5.648790627 seconds (177029 rows/sec)
      commits: 1000000
      errors: 0
    Benchmarking with VoltBulkLoader...
    Loaded 1000000 records in 1.370620937 seconds (729596 rows/sec)
      commits: 1000000
      errors: 0
