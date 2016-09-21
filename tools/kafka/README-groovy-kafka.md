Groovy Kafka Tools
==================

A set of scripts to read and manipulate VoltDB importer commit
offsets.

To execute them you need to install groovy first. This can be
done easly by using [sdkman](http://sdkman.io/) for Linux,
or [homebrew](http://brew.sh/) for the Mac.

The first time you run one of the scripts it will download, cache, and
use all the required dependency artifacts. For usage information
invoke the scripts without any arguments

* `kafka-offset-info.groovy` prints out topic partition offset
  consumption statistics
* `kafka-message-prober.groovy` prints out topic partition offset
  consumption statistics, and reads from each topic partition about a
  thousand messages
* `kafka-reset-consumer-offset.groovy` it can be used to reset the
  consumer offset used by VoltDB importers.
