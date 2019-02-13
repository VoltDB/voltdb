Configuring Kafka Exporter and Kafka Importer with Kerberos authentication
==========================================================================

Kafka supports Kerberos authentication in version 0.9 and later. This tutorial shows the steps to configure VoltDB with Kafka Exporter and Kafka Importer to connect to a Kafka cluster that uses Kerberos authentication. This is supported in VoltDB v9.0 and later.

In this example deployment, servers run on these hosts:

    Kerberos server:    volt24.voltdb.lan
    ZooKeeper server:   volt24.voltdb.lan
    KafkaBroker:        volt3d.voltdb.lan
    VoltDB server:      volt3d.voltdb.lan


Configure Kafka to use Kerberos
-------------------------------

This assumes you have a Kerberos server already installed and you know how to add principals and generate keytab files, and that you have Kafka 1.0 or later installed. Consult Kerberos and Kafka documentation for more details on this section.

Create the following service principal names(SPN) on Kerberos server:

    kafka/volt3d.voltdb.lan@VOLTDB.LAN
    zookeeper/volt24.voltdb.lan@VOLTDB.LAN

Note: For your particular servers, different SPNs should be used, not volt24, volt3d.

Generate a keytab file and store it somewhere convenient, such as /home/opt/kerberos/voltdb.keytab

### Start Zookeeper on volt24:

Add the following properties to zookeeper.properties:

    authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider
    requireClientAuthScheme=sasl

Set KAFKA_OPTS variable:

    export KAFKA_OPTS="-Djava.security.auth.login.config=/home/opt/kerberos/voltdb.lan.jaas.kafka_zookeeper.conf -Djava.security.krb5.conf=/home/opt/kerberos/voltdb.lan.kafka.krb5.conf  -Dsun.security.krb5.rcache=none"

Optional: For debugging, add -Dsun.security.krb5.debug=true to KAFKA_OPTS

Start ZooKeeper server:

    ./bin/zookeeper-server-start.sh config/zookeeper.properties


### Start Kafka broker on volt3d:
   add the following properties to server.properties:

    listeners=SASL_PLAINTEXT://volt3d.voltdb.lan:9092
    advertised.listeners=SASL_PLAINTEXT://volt3d.voltdb.lan:9092
    sasl.kerberos.service.name=kafka
    security.inter.broker.protocol=SASL_PLAINTEXT
    sasl.enabled.mechanisms=GSSAPI
    sasl.mechanism.inter.broker.protocol=GSSAPI
    authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer
    zookeeper.connect=volt24.voltdb.lan:2181
    allow.everyone.if.no.acl.found=true

Start kafka broker:

     export KAFKA_OPTS="-Djava.security.auth.login.config=/home/opt/kerberos/voltdb.lan.jaas.kafka_server.conf -Djava.security.krb5.conf=/home/opt/kerberos/voltdb.lan.kafka.krb5.conf -Dsun.security.krb5.rcache=none"

    ./bin/kafka-server-start.sh config/server.properties

Both ZooKeeper and Kafka should be properly authenticated and bootup


### Create Kafka topic 'T8_CUSTOMER'

Open a new console on volt3d and type the following command

    export KAFKA_OPTS="-Djava.security.auth.login.config=/home/opt/kerberos/voltdb.lan.jaas.kafka_server.conf -Djava.security.krb5.conf=/home/opt/kerberos/voltdb.lan.kafka.krb5.conf -Dsun.security.krb5.rcache=none"

    ./bin/kafka-topics.sh --create --zookeeper volt24.voltdb.lan:2181 --replication-factor 1 --partitions 10 --topic T8_CUSTOMER

Grant resource access to user 'kafka' :

    ./bin/kafka-acls.sh --authorizer-properties zookeeper.connect=volt24.voltdb.lan:2181 --add --allow-principal User:kafka --operation All --topic '*' --cluster  --group '*'

To apply the access, restart both ZooKeeper server and kafka broker

### Initialize and start VoltDB on volt3d, then load data to test

    export VOLTDB_OPTS="-Djava.security.auth.login.config=/home/opt/kerberos/voltdb.lan.jaas.kafka_client.conf -Djava.security.krb5.conf=/home/opt/kerberos/voltdb.lan.kafka.krb5.conf  -Dsun.security.krb5.rcache=none"
    voltdb init --config=/home/zhe/workspace/deployment/depl.xml --force
    voltdb start  --count=1 -H volt3d

Use sqlcmd to run the kerberos.sql script to create the schema

    sqlcmd
    SQL1> file kerberos.sql;

Run the customers.sql script to insert some customers into the stream for export to the Kafka topic

    SQL2> file customers.sql;

Check export stats and make sure there are no pending tuples

    SQL3> exec @Statistics EXPORT 0;

Check the CustomerBack table to verify that the data inserted follows back from Kafka

    SQL4> SELECT * FROM CustomerBack;


At this point, you have successfully tested VoltDB exporting to and importing from a Kafka cluster that uses Kerberos authentication.

This directory provides the following mentioned scripts and example files:

* [deployment.xml](deployment.xml)
* [customer.sql](customer.sql)
* [kerberos.sql](kerberos.sql)
* [voltdb.lan.jaas.kafka_client.conf](voltdb.lan.jaas.kafka_client.conf)
* [voltdb.lan.jaas.kafka_server.conf](voltdb.lan.jaas.kafka_server.conf)
* [voltdb.lan.jaas.kafka_zookeeper.conf](voltdb.lan.jaas.kafka_zookeeper.conf)
* [voltdb.lan.kafka.krb5.conf](voltdb.lan.kafka.krb5.conf)
