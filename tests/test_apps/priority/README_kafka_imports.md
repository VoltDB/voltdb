# Test cases for ENG-21666: example application slowed down by kafka imports

## Goals:
- reproduce case where a client application is slowed down by kafka imports
- verify that assigning a lower priority to import improves application performance

## Methodology:
- use TopicBenchmark2 (TB2) to run a VoltDB instance impersonating kafka, with TopicBenchmark2 client injecting into topics
- use Priority to run another VoltDB instance with topics configured in deployment to pull from the TB2 instance, and priority client invoking SPs simulating client traffic

NOTE: the SP test case must be executed WITHOUT the 'delay' parameter, otherwise the calls to Thread.sleep() in the procedures result in execution times much longer than the execution times of the import procedures and negate the effect of the priorities.
This confims the same observation on DR with conflicts.

## Test sequence:
- the test requires 2 terminal windows
  - one positioned on TB2:        tests/test_apps/topicbenchmark2
  - one positioned on priority:   tests/test_apps/priority

- prior to starting the test, modify tests/test_apps/priority/deployment_kafka_imports.xml to select the priority
  of the importers (or leave them to the default)

- in each window, start the server and initialize the schema: start TB2 first as priority will expect Kafka up and running
    ./run.sh server_kafka_imports
    ./run.sh init_kafka_imports

- start the TB2 traffic in the TB2 window
    ./run.sh run_kafka_imports

- start the priority traffic in the priority window
./run.sh run_kafka_imports

- wait for the priority test to finish and show the stats, analyze the Initiator Statistics

- kill all the java processes because TB2 client is still running: this kills all remaining clients and both VoltDB instances
    pkill java

- in each window, start the server and initialize the schema: start TB2 first as priority will expect Kafka up and running
    ./run.sh clean

## Test results:

### Test #1: run priority client with default importer configurations
    <configuration type="kafka" enabled="true" >

    Initiator Statistics:

    TIMESTAMP      HOST_ID  HOSTNAME                       SITE_ID  CONNECTION_ID  CONNECTION_HOSTNAME                                  PROCEDURE_NAME  INVOCATIONS  AVG_EXECUTION_TIME  MIN_EXECUTION_TIME  MAX_EXECUTION_TIME  ABORTS  FAILURES
    -------------- -------- ------------------------------ -------- -------------- ---------------------------------------------------- --------------- ------------ ------------------- ------------------- ------------------- ------- ---------
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic             23033                 218                  40                 752       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic01           22980                 218                  40                 752       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic05           22626                 218                  40                 752       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic04           22715                 218                  48                 752       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic03           23363                 219                  40                 752       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic02           22834                 217                  39                 859       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic             23215                 217                  35                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic01           23189                 217                   9                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic05           22735                 217                   9                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic04           23011                 216                  35                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic03           23018                 219                   4                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic02           22799                 218                   9                 716       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert08         39907                 201                  50                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert07         39761                 202                  50                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert06         40912                 201                  52                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert05         40885                 201                  52                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert04         38364                 202                  52                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert03         40022                 202                  52                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert02         40678                 202                  50                 712       0         0
    1636573682215        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert01         38602                 203                  52                 712       0         0

### Test #2: run priority client with lower priority assigned to importers
    <configuration type="kafka" enabled="true" priority="5" >

    Note how the lower priority assigned to importers improves the execution times of the application

    Initiator Statistics:

    TIMESTAMP      HOST_ID  HOSTNAME                       SITE_ID  CONNECTION_ID  CONNECTION_HOSTNAME                                  PROCEDURE_NAME  INVOCATIONS  AVG_EXECUTION_TIME  MIN_EXECUTION_TIME  MAX_EXECUTION_TIME  ABORTS  FAILURES
    -------------- -------- ------------------------------ -------- -------------- ---------------------------------------------------- --------------- ------------ ------------------- ------------------- ------------------- ------- ---------  
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic              5790                 831                   0                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic01            5668                 851                  35                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic05            5935                 841                  28                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic04            5820                 843                  28                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic03            5555                 877                  28                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              3 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic02            6079                 841                  14                1110       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic              6110                 812                   9                1211       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic01            6261                 818                  47                1211       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic05            6019                 824                  48                1210       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic04            6068                 809                  46                1211       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic03            5436                 858                   1                1210       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              4 org.voltdb.importclient.kafka10.KafkaStreamImporter  test_topic02            6311                 788                   9                1211       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert08         52297                 148                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert07         52354                 148                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert06         52415                 148                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert05         52191                 148                   2                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert04         51369                 148                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert03         52609                 148                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert02         51391                 147                   1                 467       0         0
    1636573903022        0 richards-MBP.fios-router.home         0              5 localhost                                            TestSpInsert01         50307                 148                   1                 467       0         0
