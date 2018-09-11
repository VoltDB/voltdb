How to Export Stream data to a VoltDB Procedure
===============================================

VoltDB includes an experimental export connector known informally as the VoltDB "Loopback" Export Connector. It processes rows from a STREAM by making a call a procedure within the same VoltDB cluster. The columns from the stream have to match the input parameters of the procedure.

One reason this may be useful is chain together procedures that are partitioned on different keys.

For example, suppose you have event data that has a device ID, but you need to look up the customer ID in order to update a record for the customer. You could write a client application that receives the event data, looks up the customer_id first, and then updates a record. But if the data is coming from a source system like Kafka, you may be using the Kafka importer so there isn't a custom applicaiton that can call a lookup query. But the importer can call a procedure to do the lookup, and the procedure can insert an enriched record into a stream. Using the loopback export connector, this enriched record can then be passed on to another procedure that is partitioned on the customer id to complete the processing.

Note that this is not the same as calling a procedure from a procedure (which VoltDB does not currently support). Each procedure is a separate transaction. The first procedure is committed as soon as it finishes inserting into the stream. The second procedure may not run immediately, and may commit or roll back separately, depending on if it successfully completes.

Setting it up is straightforward. In your deployment.xml file, configure an export configuration for each procedure you want to call. The configuration is similar to how custom exporters are configured, except in this case the class (org.voltdb.exportclient.loopback.LoopbackExportClient) is already included in VoltDB's classpath.

Example Configuration:

       <configuration target="callMyProcedure" type="custom" exportconnectorclass="org.voltdb.exportclient.loopback.LoopbackExportClient">
            <property name="procedure">MyProcedure</property>
            <property name="failurelogfile">/var/log/voltdb/MyProcedureFailures.log</property>
        </configuration>

Properties:

 - "procedure" (required) : set to the short procedure name (e.g. "Vote" not "org.voltdb.procedure.Vote").  If you just want to insert into a table, you can specify the table's built-in insert procedure (e.g. TABLENAME.insert)

 - "failurelogfile" (optional) : set this to the path of the filename for a log file into which failed records will be appended.  If you specify a relative path, it will be relative to the voltdbroot directory.  If you use an absolute path, it will be used exactly.
