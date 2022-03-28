# User-hits example: VoltDB topics and compound procedures

User-hits is an application simulating VoltDB handling user hits on web pages, demonstrating the following:

- How a Kafka producer application can send data to a VoltDB topic.
- How a VoltDB topic can invoke a VoltDB 'compound' stored procedure, i.e. a procedure that extends VoltCompoundProcedure instead of VoltProcedure.
- How a VoltDB compound procedure can invoke multiple VoltDB stored procedures in parallel, using a simplified asynchronous invocation framework.
- How a VoltDB compound procedure can invoke multiple VoltDB stored procedures partitioned on different partition keys.
- How a VoltDB stored procedure can output information to VoltDB topics that can be consumed by Kafka consumer applications.

By implementing stored procedures extending VoltCompoundProcedure, it is possible to implement some business logic that hitherto would have to be implemented in a client application using the VoltDB Client API. The API exposed by VoltCompoundProcedure simplifies the invocation of other VoltDB stored procedures, and simplifies the invocation of multiple procedures in parallel.

The client application has an option allowing running the same business logic as a client application using the asynchronous methods of the VoltDB Client API. This enables comparing the differences between the VoltCompoundProcedure API and the Volt Client API.


# Application synopsis

The UserHitClient application generates 'user hits' to the VoltDB topic. Each 'user hit' is made of the following information:

- cookieId: an integer that represents a browser cookie associated with a user.
- url: the URL of the web page that the user accessed.

The kafka producer client sends the user hits to a VoltDB topic 'record_hit': the topic invokes the stored procedure 'HandleHit' for each record sent by the producer.

The HandleHit procedure is a VoltCompoundProcedure that is structured in 3 'stages':

- The first stage invokes 2 stored procedures performing lookups using different partition keys. The procedures are invoked in parallel and could potentially be running on different sites in the VoltDB cluster.

- The second stage invokes 2 stored procedures performing updates using the retrieved information, also invoked in parallel, using different partition keys, and possibly running on different sites.

- The last stage handles the update results.

The database schema reflects the updates to various VoltDB topics that can be consumed by Kafka consumer applications:

- The topic 'next_actions' show a simulated 'action' that is calculated for the user on each hit.
- The topic 'user_hits' reflect each user hit with added information from the database.
- The topic 'account_hits' reflect the activity on each account represented by the URL domain.
- Finally, the topic 'cookie_errors' reflect any logical errors that might occur, such as 'unknown user' (the standard test cases should not generate any errors there).

# Test scenarios

## Kafka producer to VoltDB topic

In this test scenario, the UserHitClient application uses the Kafka producer API to send user hits to the VoltDB 'record_hit' topic, which will invoke the compound procedure 'HandleHit':

- Start the server (this also compiles the Java classes):  
  ./run.sh server

- Initialize the database schema:  
  ./run.sh init

- Run the test case:  
  ./run.sh run-producer

The test will run for about 2 minutes. While the test is running, or after the test has completed, the outgoing topics can be checked with a Kafka consumer, e.g.:

    kafka-console-consumer --bootstrap-server localhost:9092 --topic user_hits --from-beginning --property print.key=true
    kafka-console-consumer --bootstrap-server localhost:9092 --topic next_actions --from-beginning --property print.key=true
    kafka-console-consumer --bootstrap-server localhost:9092 --topic account_hits --from-beginning --property print.key=true

After the test completed, it should be cleaned up as follows:

- Shutdown VoltDB  
  voltadmin shutdown

- Clean up the test (this is necessary if the test is to be repeated)
  ./run.sh clean

## VoltDB client API

In this test scenario, the UserHitClient application does not sent data to the VoltDB topic, but uses the VolDB Client API to mimic the behavior of the stored procedure 'HandleHit': the same VoltDB procedures are invoked with the same degree of parallelism in the 'HitHandler' class, but instead of running the business logic within the VoltDB server, the business logic is running in the client application.

By comparing the implementations in 'HandleHit.java' and 'HitHandler.java', it is possible to assess how the exact same business logic can be implemented in each API.

- Start the server (this also compiles the Java classes):  
  ./run.sh server

- Initialize the database schema:  
  ./run.sh init

- Run the test case:  
  ./run.sh run-client

The test will run for about 2 minutes. While the test is running, or after the test has completed, the outgoing topics can be checked as previously explained.

After the test completed, it should be cleaned up as follows:

- Shutdown VoltDB  
  voltadmin shutdown

- Clean up the test (this is necessary if the test is to be repeated)
  ./run.sh clean
