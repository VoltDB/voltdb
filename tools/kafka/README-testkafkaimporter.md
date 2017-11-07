Kafka Importer Tools
====================
script to use: testkafkaimporter

To execute, first modify kafkatester.properties to point to your Kafka and
topic. See properties files in this directory for more details and examples.

* `testkafkaimporter` will use VoltDB code to connect to Kafka and start fetching messages from the topic.
  The offsets won't be committed and no VoltDB transactions are performed.
