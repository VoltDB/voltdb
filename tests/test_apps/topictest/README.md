# Goals

This test applications aims at testing:
- That producer applications can send <key, value> records to the correct VoltDB partition using VoltDBKafkaPartitioner.

- If the topic is configured with property producer.parameters.includeKey=true:

  - that the key can be omitted from the 'value' part of the <key, value> record,
  - that the keys are inserted at the proper place in the procedure parameter list.


- That topics declared with more than one procedure can 'fan-out' invoking all the procedures in the list, even if the partitioning of the procedures is different.

The client application can send data in CSV or AVRO formats (the latter requires a Confluent AVRO schema registry running on port 8081).  

The test client can also be used as a programming model for client applications that wish sending data to a VoltDB topic, i.e. on how to configure the Producer instance, and how to produce records.

# Backgound

## VoltDB topic partitioning

In order for Kafka producer applications to send data to a VoltDB topic at the correct VoltDB partition, the following conditions must be met:

- The producer application must send <key, value> records with a key that is non-null.

- The type of the key must match the type of the procedures's partitioning parameter.

- The Kafka Producer must use the Kafka partitioner provided with the VoltDB distribution, implemented in the VoltDBKafkaPartitioner Java class.

If these conditions aren't met, VoltDB logs the following kind of messsage every minute:

    WARN: Topics producer producer-1 (/192.168.1.206:54634) sent records to topic TEST_TOPIC, procedure procedure01 to the incorrect partition 4. If this condition is not corrected, it will impact performance. It is recommended to configure the producer client property 'partitioner.class' as 'org.voltdb.client.topics.VoltDBKafkaPartitioner'.

## Structure of the <key, value> records sent to VoltDB

VoltDB decodes each incoming <key, value> record and builds the procedure's parameter list as described in the examples below. One important property, "producer.parameters.includeKey", has an impact on this mechanism.

### Producing to a topic with producer.parameters.includeKey=false

This is the default value of this property for VoltDB topics.

Assuming a procedure declared as follows:

    --
    -- Stream with partition column in non-default position 2
    --
    DROP STREAM SOURCE01 if exists;
    CREATE STREAM SOURCE01 PARTITION ON COLUMN c EXPORT TO TOPIC source01 (
        a   BIGINT        NOT NULL
      , b   VARCHAR(256)  NOT NULL
      , c   BIGINT        NOT NULL
      , d   VARCHAR(256)  NOT NULL
    );
    DROP PROCEDURE procedure01 if exists;
    CREATE PROCEDURE procedure01 PARTITION ON TABLE SOURCE01 COLUMN c PARAMETER 2 AS INSERT INTO SOURCE01 (a, b, c, d) VALUES (?, ?, ?, ?);

This defines 'procedure01' as a procedure with the following signature:

    VoltTable[] procedure01(long a, String b, long c, String d)

Note that the procedure is partitioned on the parameter 'c' at position 2. If the procedure was declared without the PARAMETER clause the partitioning parameter would be at position 0.

Assuming a topic declared like this:

    <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
      <property name="producer.parameters.includeKey">false</property>
    </topic>

The client application must produce <key, value> records **where the key is repeated in the value at the proper position**. For instance:

- key (a long value) = **10**
- CSV value (a CSV string that includes the key) = **110,"column b",10,"column d"**

### Producing to a topic with producer.parameters.includeKey=true

Assuming a procedure declared as in the previous section, and a topic declared as follows:

    <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
      <property name="producer.parameters.includeKey">true</property>
    </topic>

The client application can now produce <key, value> records **with the key omitted from the value**. For instance:

- key (a long value) = **10**
- CSV value (a CSV string that omits the key) = **110,"column b","column d"**

VoltDB decodes the the <key, value> record and places the key at the proper place in the parameter list for the procedure invocation.

# Running the test

## Common test steps

The **run.sh** script contains a number of test cases. See the comments above each test case, for how to configure the TEST_TOPIC topic prior to starting the server.

We describe below the test case **client_withkey_procedure01**, which illustrates the handling of the keys when **producer.parameters.includeKey=true**. All the test cases can be executed in a similar fashion.

1. Edit the **deloyment.xml** file to configure the topic **TEST_TOPIC** as required by the test case:

        <topic name="TEST_TOPIC" format="csv" procedure="procedure01">
          <property name="producer.parameters.includeKey">true</property>
        </topic>

2. Start VoltDB on localhost (starts in background):

        ./run.sh server
        (...)
        Server Operational State is: NORMAL
        Server completed initialization.

3. Initialize the DDL:

        ./run.sh init
        (...)
        Command succeeded.

4. Run the test case:

        ./run.sh client_withkey_procedure01
        (...)
        Finished test: 10 inserted, 0 failed

5. Verify the egress topic has had the keys properly inserted at the right position (exit with CTRL-C). Note that the egress topic depends on the procedure assigned to TEST_TOPIC. **Here, we expect the keys to have been inserted at position 2**:

        kafka-console-consumer --bootstrap-server localhost:9095 --topic source01 --from-beginning
        102,102-bbb,2,102-ddd
        104,104-bbb,4,104-ddd
        106,106-bbb,6,106-ddd
        101,101-bbb,1,101-ddd
        103,103-bbb,3,103-ddd
        107,107-bbb,7,107-ddd
        108,108-bbb,8,108-ddd
        105,105-bbb,5,105-ddd
        109,109-bbb,9,109-ddd
        110,110-bbb,10,110-ddd
        ^CProcessed a total of 10 messages


 6. Kill or shutdown VoltDB

 7. Cleanup test

        ./run.sh clean
        (...)
        clean

## AVRO test cases

The AVRO test cases have been tested with the Confluent platform confluent-7.0.0.

1. The confluent platform must be started with the command **confluent local start**. Note that on my Mac I need a file placed on my path to work around a Confluent bug on Mac. On other platforms that should not be necessary:

        cat ~/bin/sw_vers
        echo "10.13"

        export PATH=~/bin:$PATH; confluent local start
        (... confluent plaform starts ...)
        control-center is [UP]

2. You MUST disable the backward checks in the schema registry by issuing the following REST command:

        curl -X PUT -H "Content-Type: application/vnd.schemaregistry.v1+json" \
            --data '{"compatibility": "NONE"}' \
            http://localhost:8081/config


3. Configure and execute an AVRO test case after adjusting the deployment as explained in the comments in **run.sh**. Note that the test run reports the AVRO schema being used to produce the values, e.g in the schema below for test case **avro_client_withkey_procedure02** the column A is omitted:

        Using AVRO schema: {"type":"record","name":"TOPIC_TEST_PRODUCER","fields":[{"name":"B","type":"string"},{"name":"C","type":"long"},{"name":"D","type":"string"}]}

4. Test results are still checked with the same **kafka-console-consumer** tool as the egress topics remain in CSV.

5. After terminating and cleaning the test, destroy the confluent platform:

        confluent local destroy
        (...)
        Deleting: /var/folders/mf/f75lh19n157dzgc_42xghvmc0000gp/T/confluent.IMGViyud
