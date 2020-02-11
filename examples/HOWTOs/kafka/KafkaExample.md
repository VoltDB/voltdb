Kafka Export / Import Guide
===========================

This is a tutorial about how to connect a VoltDB cluster to a Kafka server both as a consumer to import data into VoltDB, and as a producer to stream or export data from VoltDB into Kafka.

You should review the section on [Understanding Export](https://docs.voltdb.com/UsingVoltDB/ChapExport.php#ExportIntro) in Using VoltDB to learn more about the Export feature and how it works. Also, it will be useful to reference the section on [The Kafka Export Connector](https://docs.voltdb.com/UsingVoltDB/ExportToKafka.php) for more details on configuration options and optional properties.


There is also a section on [Understanding Import](https://docs.voltdb.com/UsingVoltDB/exportimport.php) which describes how the import feature works, and a section on [The Kafka Importer](https://docs.voltdb.com/UsingVoltDB/exportimportkafka.php) with more configuration details.

This tutorial provides an example of VoltDB Export and Import using a local standalone Kafka instance such as you would install following the Kafka [Quickstart](https://kafka.apache.org/quickstart). It includes all the additional steps you can follow to create a stream in DDL and export records inserted into the stream to an automatically-generated topic in Kafka. It then uses that same topic to show how to configure the Kafka Importer to consume each record from a Kafka topic and pass it to a procedure in VoltDB, where it can be inserted into a table or additional transactional processing could be done. It uses VoltDB version 8.4, but it should work with all supported VoltDB versions. It uses Kafka version 1.1.0. This tutorial assumes a working knowledge of how to start a VoltDB node.


Start up Kafka
--------------


1) Download Kafka from here: http://kafka.apache.org/downloads

2) Un-tar and cd into the Kafka directory:

    tar -xzf kafka_2.11-1.1.0.tgz
    cd kafka_2.11-1.1.0

3) Start the zookeeper server:

    bin/zookeeper-server-start.sh config/zookeeper.properties


4) Start the Kafka server:

    bin/kafka-server-start.sh config/server.properties


Exporting from VoltDB to Kafka
------------------------------

1) Start a voltdb cluster using the attached deployment_k.xml configuration file. Notice the relevant export part:

        <export>
            <configuration enabled="true" target="test" type="kafka">
            <property name="bootstrap.servers">localhost:9092</property>
            <property name="topic.key">Customer_final.test</property>
            <property name="skipinternals">true</property>
            </configuration>
        </export>

The bootstrap.servers property name is required and in this example connects to the default localhost port.

The topic.key property name takes the stream name and links it to the topic name "test". If this property name was not used, it would instead use the default prefix voltdbexport followed by the stream name like so: voltdbexportCUSTOMER_FINAL.

The skipinternals property name stops the export of VoltDB meta data, such as transaction IDs and timestamps. It is purely optional, but it is needed for this particular example app to function.


2) Load the attached ddl.sql file.

    CREATE STREAM Customer_final EXPORT TO TARGET test (
      CustomerID INTEGER NOT NULL,
      FirstName VARCHAR(15) NOT NULL,
      LastName VARCHAR(15) NOT NULL
    );

Notice the relevant stream part and how the "Customer_final" and "test" terms line up with the topic.key property name written above:

    <property name="topic.key">Customer_final.test</property>

And notice the related procedure that will be used to export data:

    CREATE PROCEDURE acs AS INSERT INTO Customer_final VALUES ?,?,?;

The procedure that inputs data into an export stream is exactly the same as one that inputs data into a normal VoltDB table.


3) Once the VoltDB cluster is up with the appropriate configuration file and ddl loaded, use the sqlcmd command to execute the above procedure with some example data:

    sqlcmd
    1> exec acs 1 a b;


4) In a separate console window, verify that the test topic has been created with the following Kafka command:

    bin/kafka-topics.sh --zookeeper=localhost:2181 --list

5) Verify that the data entered in step 3 is in the Kafka topic with the following command:

    bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning

You should see this as the output:

    "1","a","b"


Importing data from Kafka to VoltDB
-----------------------------------

This example app also imports data from Kafka. Notice the relevant import section of the deployment_k.xml file:

    <import>
        <configuration enabled="true" type="kafka">
            <property name="brokers">localhost:9092</property>
            <property name="topics">test</property>
            <property name="procedure">add_customer</property>
        </configuration>
    </import>

This configuration automatically imports data from the test topic in the same broker that the data is exported to. It then uses the add_customer procedure from the DDL to insert the data into the CUSTOMER table.

1) Use the sqlcmd command to export more data using the acs procedure:

    sqlcmd
    1> exec acs 2 a b;

2) Then use the following SQL command to view the contents of the CUSTOMER table:

    SELECT * FROM CUSTOMER;

You should see the inserted data, which first went to the Kafka export server and then was automatically imported into the CUSTOMER table.

One thing to note is that the CUSTOMER table has a unique CustomerID index. What this means is that if you export two rows of data with the same CustomerID using the acs procedure, it will only be imported into the CUSTOMER table once, so that it does not violate the unique constraint seen in the DDL.
