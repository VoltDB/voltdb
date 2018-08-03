# Simple Example

This is a simple example app that is intended to be easy to read to help users learn the basics of VoltDB. It can also be easily modified to help new users build their own benchmarks.

The DDL includes one table, one index, one view, one DDL-declared procedure, and one Java stored procedure.  The use case is quite generic, consisting of devices that are recording sessions running different apps. The view and procedures provide some insight into the aggregate usage of the various apps, but really they are just examples of basic VoltDB techniques.

Pre-requisites
--------------

Before running these scripts you need to have VoltDB 6.6 or later installed.  Download and untar the VoltDB .tar.gz installation file to a directory such as your $HOME directory, then add the bin subdirectory to your PATH environment variable.  For example:

    export PATH="$PATH:$HOME/voltdb-ent-8.2/bin"

You may choose to add this to your .bashrc or .profile file.

To verify that you've installed VoltDB correctly and added it to your path, run the following command:

    voltdb --version

Getting Started
---------------

Start the database. In the directory of your choice, run the following commands:

    voltdb init
    voltdb start

Load the schema:

    sqlcmd < ddl.sql


Running the Benchmark
---------------------

Run the client:

    ./run_client.sh

Optional parameters for running the client

    ./run_client.sh {hostname} {number of procedure calls}

For example

    ./run_client.sh localhost 1000000

Stop the database:

    voltadmin shutdown


Modifying this benchmark
------------------------

The simplest way to make this your own benchmark is to simply:

1. Add a table to the ddl.sql file
2. Modify the Benchmark.benchmarkItem method to call the default (generated) TABLENAME.insert procedure.

For example, you might add the following to the ddl.sql file:

    CREATE TABLE my_table (
      id BIGINT NOT NULL,
      val BIGINT
    );
    PARTITION TABLE my_table ON COLUMN id;
    CREATE INDEX my_table_idx1 ON my_table (id);

Then in the client/src/Benchmark.java file, modify the benchmarkItem() method like this:

    public void benchmarkItem() throws Exception {

        // To make an asynchronous procedure call, you need a callback object
        // BenchmarkCallback is a generic callback that keeps track of the transaction results
        // for any given procedure name, which should match the procedure called below.
        ProcedureCallback callback = new BenchmarkCallback("MY_TABLE.insert");

        // generate some random parameter values
        int id = rand.nextInt(1000);
        int val = rand.nextInt(1000000);

        // call the procedure asynchronously, passing in the callback and the procedure name,
        // followed by the input parameters
        client.callProcedure(callback,
                             "MY_TABLE.insert",
                             id,
                             val
                             );
    }


If you want to modify or copy the example java stored procedure to make your own, you will need to compile it into a plain jar file. A script is provided to do this:

    cd procedures
    ./compile_procs.sh

This will generate a new procedures.jar file in the procedures subfolder. This is loaded by the ddl.sql script. If you have made changes and wish to update the procedure code running in the database, you can reload the procedures using the same command:

    sqlcmd
    1> load classes procedures/procedures.jar;

If you modify or add objects to the ddl.sql, keep in mind you would need to drop all of the existing objects first before re-running the script, or you can shutdown and repeate the steps in Getting Started above.

When you run the un_client.sh script, it will first re-compile Benchmark.java and all of the other Java code in the client/src folder and its subdirectories.
