Test application allowing testing priority levels in VoltDB

Run a test case and check the initiator stats for the average execution times of the different procedures invoked by the test.

Each SP or MP procedure is invoked by a separate thread in the test client; each procedure can be invoked at a specific rate defined in the run options. An undefined rate disables invoking the specific procedure.

When the server is started with priorities enabled and the client uses those priorities each procedure is invoked at a specific priority, e.g.:

- TestSpInsert01 invoked at priority 1 (highest)
- TestSpInsert08 invoked at priority 8 (lowest)

The SP procedures update a different table whereas the MP procedures update a common table.

See src/client/PriorityClient.java for the available options and the run.sh for some example test cases.

Note on latencies: the latencies reported by the test may be greater than normal.
This test attempts to generate enough invocations to load the site transaction queues, so that the effects on the initiator stats are greater, but latencies are also greater.
This can be achieved by a combination of 2 variables:

- the number of sites per host, e.g. reducing SPH creatly increases the contention on the sites
- the insertion rate

General test procedure:

- Step 1: start the server with the desired priority configuration:

  - run server with priorities disabled
    ./run.sh server_no_priority

  - run server with priorities enabled with defaults
    ./run.sh server_no_priority

  - run server with priorities enabled and maxwait=0, i.e. pure priority
    ./run.sh server_pure_priority

- Step 2: initialize schema

  ./run.sh init

- Step 3: run test case, e.g. to test SPs at all levels

  ./run.sh spall

- Step 4: verify initiator stats and compare average execution times

- Step 5: shutdown or simply kill voltdb

- Step 6: cleanup

  ./run.sh clean

  
