# insertdelete test app #
-------------------------

This app tests a customer-inspired use case where the customer wanted something similar to a user-defined "temp table".  The tables beginning with the prefix "tmp_" are for this purpose.  The procedures insert records into 10 of these tables, then delete those same records by their IDs.  The end result is no change to the state of the tables, but along the way, the EE needed to do the inserts and deletes.


While VoltDB can handle inserts and deletes very well, this is a special edge case where each insert is the first and only row in the table, and each delete is removing the last row from the table.  These operations used to incur performance penalties which cause this workload to be slower and perhaps less scalable than similar OLTP operations where the tables are not empty.  The client performs up to 2*K inserts, where K is a command line parameter.  The default value for K is 1.0e6.  The stored procedure makes K calls to insert into and delete from a table with inline storage only.  It can also make another K calls to a procedure that does the same for a table that includes a VARCHAR(150) as that may perform differently than a purely inline table.  The processing of inline or out-of-line data may be separately suppressed, to test inline-only performance or out-of-line only performance.

Directions:
    ./run.sh server        ## To start the server in one terminal window.
    ./run.sh init          ## To initialize the database schema.
    ./run.sh seed          ## This is optional, to seed the server's tables.
    ./run.sh client [ ... args ... ] ## To run the test
       The args here are:
          --inline            Only test insertion/deletion of inline data
          --outline           Only test insertion/deletion of out-of-line data
          --numiters K        Insert and delete K records.

For comparison, use the "seed" command to insert a set of rows into each table that will not be touched by the client.  Then run the client again, to see the difference when the tables are not empty.

    ./run.sh init
    ./run.sh seed
    ./run.sh client [ ... args as above ... ]

Tests should be run on how well the workload scales.  In the customer case that inspired this, a similar workload would hit maximum throughput with a small number of sitesperhost, and not scale up.
