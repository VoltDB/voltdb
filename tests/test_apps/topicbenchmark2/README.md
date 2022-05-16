TopicBenchmark2 allows exercising the VoltDB topics in system tests, performance tests, and scale tests.

This document is a succinct description of how to run the basic tests. Those tests have been executed on an 8-core MacBook pro 16 with 32Gb RAM. You may have to adjust the tool's arguments to your conditions.

# System requirements

Even if you are not planning to test avro and the schema registry, TopicBenchmark2 requires that a Confluent platform distribution be accessible, and the CONFLUENT_HOME variable be set in the environment.

The Confluent platform can be downloaded from the [Confluent website](https://www.confluent.io/download/#confluent-platform).

# Basic test sequence (CSV)

All the tests must be executed from the TopicBenchmark2 directory:

    cd ~/ws-voltdb/voltdb/tests/test_apps/topicbenchmark2


The tests are executed via the **run.sh** script:

    ls -l ./run.sh

Start the VoltDB server (this automatically compiles the TopicBenchmark2 java client) with the 'server' option; Once initialized, the server can be sent to run in the background (CTRL-Z + bg):

    ./run.sh server
    server
    (...)
    Server Operational State is: NORMAL
    Server completed initialization.
    ^Z
    [1]  + 32810 suspended  ./run.sh server
    bg
    [1]  + 32810 continued  ./run.sh server

Run the 'init' option which loads the DDL defined in the file **topicTable.sql** and creates the topics used in the test, and wait for the last 'Command succeeded':

    ./run.sh init
    init
    (...)
    Command succeeded.

Run the 'run_benchmark' option which executes a basic test concurrently running 2 producers and 20 subscribers in 2 groups; the  benchmark produces a total of 1 million records using a 512-byte string. In each group, 3 members are 'transient' i.e. they leave and rejoin their groups on a random interval to trigger subscriber group rebalancing.

Every subscriber group verifies that it is able to poll all the records inserted by the producers.

    ./run.sh run_benchmark
    run_benchmark
    INFO: Test using brokers: [localhost:9095]
    INFO: Test producers using ASCII String (size in bytes = 512):
    2dKjir05KbTWafvDnUDfkCPX1H5nADt2zBZbYFguMZTH2atUsyAL535ICMUrz1RSeUpYetR8dIlO8oIEBcueOahCxA3FDWWD7CPz18HnB2HH2NrwnXEUY9jwQrT07lgVnbxLtBDrTE1BPmJVcoQLD4M0twads5ZUq9dfCoI01rMQA8URXsX2k7NwwEw9N6wq7Ybo9s0jalzboutCjiSKXIc6r2JKfrx7KpaDGQXoqzgf7njav47AiHlSTQ2NQoRBYWEjoIX3i54dARIZHIlK5v37t7nrgh3anKuIQerCfS95F9L0z3JpCB8XyMtvXKCTuZ2FpeJOXnNAhHriImUYV1oWx6aRV4kkjCzdKZj67b1Ze9gPV0EbFEiOMLJAPZ60nKtqmo7dT5TrX9M8VQE547lfDfhpR38EUaAzVo8YAVH8hy3Ip6GQmxE6o37RouB8vhs4rwzoGQQdn9vFDGwVgO20oxinh1GZMwJdURWmdBW2omMCjkNXmhf8RBQnWRAj
    INFO: Test initialization
    INFO: Connecting to VoltDB Server...localhost
    INFO: Connected to VoltDB node at: localhost
    INFO: Creating 20 subscriber threads in 2 groups to topic TEST_TOPIC, all groups verifying
    INFO: Subscriber group: 0 using transient members: {4=5000, 7=9000, 8=10000}
    INFO: Subscriber group: 1 using transient members: {1=5000, 4=9000, 8=9000}
    (...)
    INFO: Creating 2 producer threads to topic TEST_TOPIC
    (...)
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 41, verified = 10500, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 35, verified = 10464, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 43, verified = 11787, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 44, verified = 14000, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 39, verified = 16000, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-0 thread 28, verified = 10500, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-0 thread 34, verified = 11000, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 38, verified = 17000, dups = 0
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-0 thread 29, verified = 11500, dups = 0
    (...)
    INFO: Producer thread 45 completed 500000 records
    INFO: Producer thread 46 completed 500000 records
    INFO: Finished benchmark insertion in 11 seconds, last rowId = 999999
    (...)
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-1 thread 39 verified, 1000000 records polled, 0 duplicates
    (...)
    INFO: Group d4432452-ab08-4a9b-a1b7-677e0886f1af-0 thread 34 verified, 1000000 records polled, 0 duplicates
    INFO: LAST_OFFSET settled in 1 seconds for 1000000 records
    INFO: Benchmark complete: 1000000 successful, 0 failed records in 22 seconds

Once the benchmark test is complete, you may choose to run subscriber-only tests as explained in the next section, or decide to finish your testing and clean up.

In order to clean up after testing, you must run the 'shutdown' option to exit VOLTDB:

    ./run.sh shutdown
    shutdown
    INFO: Cluster shutdown in progress.
    (...)
    **************************************************************************
    * The VoltDB server will shut down due to a control-C or other JVM exit. *
    **************************************************************************
    [1]  + 33437 done       ./run.sh server

Afterwards you must run the 'clean' option to remove all the test artifacts:

    ./run.sh clean
    clean

# Alternate test scenarios (CSV)

Once you have executed the 'server' and 'init' options of the **run.sh** script, you may elect to run a test scenario allowing you to produce records to the topic once, using a producer-only test. Then you may run a subscriber-only test multiple times, polling the topic with various parameters.

Like in the basic test scenario, you must execute the 'shutdown' and 'clean' options to cleanup the test by removing the test artifacts.

Run the 'run_producers' option to execute a producers-only test inserting a total of 10 million rows from 2 producers:

    ./run.sh run_producers
    run_producers
    INFO: Test using brokers: [localhost:9095]
    INFO: Test producers using ASCII String (size in bytes = 512):
    3fgQqVPUd0eRr5peiV1bkIWr8ERWwSzCXxr34zARDt4even7ZGWZdR3rL6vLtTM8jP4veRtFrSbsSSmn7w9gqGWOgJwN6m5T7j6KhDx3VogG4jfZesWL3FbgA17GlvPd1hcJlRr7KYLxW08fqIptXoVUFXN6NvlYnmHgdmxjLcc8BnHvz0NfRa2OMOb3Zb1nE4ffdfZgGEbygBAneAH6K8xAYiRzyTazUJNqChXOQ8qFmYonX5OSvKlXkcU6Y7WTxYFHoqgK9Uza7Z7YNfSfC3z2kmnK0fjlrigfAkQdiAAVeHoycCtMqGZkLcOzOoDDX6Cbk0lSWSKLJBpVhrwoCv2af7XXGZfut1awVmmruJSVsWWchjAbxy93WzbnQ0MB5HdHhrqWsXtwmxyG5PMDZxP9gwmX5P6sMwmNE7Y54TaLfTALmJXdyQvkxYmv4riTTCGGPFrHqJvayDEsu360ENv2ziRz5vD6HhD0QEWKEuCUrERxkEGVjfCqBPRxJLtN
    INFO: Test initialization
    INFO: Connecting to VoltDB Server...localhost
    INFO: Connected to VoltDB node at: localhost
    INFO: Creating 2 producer threads to topic TEST_TOPIC
    WARN: Strict java memory checking is enabled, don't do release builds or performance runs with this enabled. Invoke "ant clean" and "ant -Djmemcheck=NO_MEMCHECK" to disable.
    INFO: Producer thread 25 completed 5000000 records
    INFO: Producer thread 26 completed 5000000 records
    INFO: Finished benchmark insertion in 45 seconds, last rowId = 9999999
    INFO: LAST_OFFSET settled in 1 seconds for 10000000 records
    INFO: Benchmark complete: 10000000 successful, 0 failed records in 56 seconds
    INFO: Test client finished successfully

You may now run the 'run_subscribers' option several times. Each test run polls the 10 million rows in the topic from 60 subscribers in 6 groups. Each group has 3 transient members leaving and rejoining their groups at random intervals, triggering group rebalancing. Each subscriber group verifies it successfully polled the 10 million rows:

    ./run.sh run_subscribers
    run_subscribers
    INFO: Test using brokers: [localhost:9095]
    INFO: Test initialization
    INFO: Connecting to VoltDB Server...localhost
    INFO: Connected to VoltDB node at: localhost
    INFO: Creating 60 subscriber threads in 6 groups to topic TEST_TOPIC, all groups verifying
    INFO: Subscriber group: 0 using transient members: {5=3000, 7=3000, 9=9000}
    (...)
    INFO: Group ade8107b-8597-4c54-9e57-f10d93754b1a-3 thread 57 verified, 10000000 records polled, 0 duplicates
    (...)
    INFO: Benchmark complete: 0 successful, 0 failed records in 230 seconds
    INFO: Test client finished successfully

The subscriber-only scenario may also be run after the basic 'run_benchmark' test, but the expected count must be adjusted in the **run.sh** script from 10 million down to 1 million records.

You may edit the **run.sh** script to change the test parameters: the command-line options to TopicBenchmark2 can be reviewed in the **TopicBenchmark2.java** implementation file, and should be straightforward. The main options to change are:
* **count**: the number of records to insert (per producer), or to poll (per subscriber group).
  * Make sure to read the comments in **run.sh** to learn how to adjust the count
  * In benchmark or producer-only tests the count defines the number of records inserted by each producer:
    * so 2 producers and a count of 5 million produces a total of 10 million records
  * In subscriber-only tests the count defined the total number of records that can be polled from the topic
    * so in order to run a subscriber-only test on the topic produced above, the count must be set to 10 million
* **producers**: number of producers
* **groups**: number of subscriber groups
* **groupmembers**: number of subscribers per group
* **transientmembers**: number of members that are transient, i.e. leaving and rejoining the subscriber group during test execution.

# Testing AVRO with the Confluent schema registry
This chapter describes how to test AVRO with the Confluent schema registry. It assumes the Confluent platform is accessible from the test system and the CONFLUENT_HOME variable set in the environment.

This version of TopicBenchmark2 was tested with confluent-6.0.0, confluent-7.0.0, and confluent-7.1.0. Some adjustments may be needed if another version of the Confluent platform is used.

## Confluent operation
This section describes how to start, stop, and cleanup the Confluent applications required for the TopicBenchmark2 tests.

Start the Confluent applications as follows:

    confluent local start

The TopicBenchmark2 test will create different versions of the schema for the topic, so the schema registry **MUST** be configured to disable the compatibility checks; this is done with a PUT operation to the REST API:

    curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data '{"compatibility": "NONE"}' \
    http://localhost:8081/config

This command should return the value of the new mode:

    {"compatibility":"NONE"}%

At this point the schema registry is ready to support the different TopicBenchmark2 test scenarios described in the next section.

Once the tests are complete, or if a brand new test sequence is desired, the Confluent plaftorm applications must be stopped after the 'shutdown' and 'clean' options have been executed.

The Confluent applications are stopped as follows:

    confluent local stop

If the confluent tools are restarted at this point, they will re-use the state stored by a previous test execution. This may be useful if we want to use one of the subscriber-only options of the test. However, if a new test sequence is desired, state created by the Confluent applications must be wioed out as follows:

    confluent local destroy

At this point the Confluent applications might be restarted to support another round of testing. Note that this 'destroy' option can also be invoked when the confluent platform is running: it will stop the components and clean up the state.

## Avro test scenarios
This section describes how to run the avro test scenarios.

Prior to running the test scenarios, the Confluent applications must be started as explained in the previous section.

The VoltDB server must be started with the 'server_avro' option (VoltDB will use port 9095 to avoid conflicting with kafka):

    ./run.sh server_avro
    server
    (...)
    Server Operational State is: NORMAL
    Server completed initialization.
    ^Z
    [1]  + 32810 suspended  ./run.sh server_avro
    bg
    [1]  + 32810 continued  ./run.sh server_avro

Instead of using the 'init' option, the 'init_avro' option must be used to load an alternate DDL defined in **topicAvroTable.sql**. This DDL creates test topics using avro and inline encoding:

    ./run.sh init_avro
    init_avro
    (...)
    Command succeeded.

At this point, the avro test cases may be executed. They are similar to the basic CSV test cases exept that their function names in **run.sh** have 'avro'.

The 'run_avro_benchmark' option runs a test with producers and subscribers running concurrently:

    ./run.sh run_avro_benchmark

Alternatively, the 'run_avro_producers' option can run a producers-only test, and the 'run_avro_subscribers' can run a subscribers-only test.

Similarly to the basic CSV tests, the test setup must be cleaned with the 'shutdown' and 'clean' options:

    ./run.sh shutdown
    ./run.sh clean

After that, the Confluent applications must be stopped and cleaned before another test round can be started.

# Running TopicBenchmark2 tests against Kafka instead of VoltDB

TopicBenchmark2 tests can be run against Kafka in order to compare behavior or performance. Any test scenario (CSV or Avro) can be executed against Kafka.

The generic test sequence is as follows:

* Start the Confluent applications as described above
* No need to start and configure the VoltDB server
* Modify the selected tests as follows:
  * set the topicPort to the default 9092 for Kafka (VoltDB is configured for 9095)
    * --topicPort=9092 \
  * set the option to use kafka
    * --usekafka=true \
* Run the test case (e.g. 'run.sh run_avro_producers', 'runsh run_avro_subscribers')
* After the tests are completed, stop and cleanup the Confluent applications
