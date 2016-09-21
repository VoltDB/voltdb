Kafka Importer Tools
====================
script to use: testkafkaimporter

To execute first modify kafkatester.properties to change properties to point to your kafka and
topic see propertis files for more details on properties. 

* `testkafkaimporter` will use VoltDB code to connect to kafka and topic and start fetching messages.
  The offsets wont be committed and no VoltDB transaction are performed.
