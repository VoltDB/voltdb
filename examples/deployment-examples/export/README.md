Instructions for exporting to CSV
---------------------------------
1. Edit the deployment.xml file to add the following.  For an example, see the provided deployment-export-csv.xml file.

```xml
<export enabled="true" target="file">
 <configuration>
  <property name="type">csv</property>
  <property name="nonce">MyExport</property>
 </configuration>
</export>
```

2. Then follow the instructions for running on a single server or cluster.


Instructions for exporting to Hadoop
------------------------------------
1. Edit the deployment.xml file to add the following.  For an example, see the provided deployment-export-hadoop.xml file.

```xml
<export enabled="true" target="http">
 <configuration>
  <property name="endpoint">http://myhadoopsvr/webhdfs/v1.0/%t/data%p.%t.csv</property>
  <property name="batch.mode">true</property>
  <property name="period">120</property>
 </configuration>
</export>
```
or

```xml
  <export enabled="true" target="http">
     <configuration>
       <property name="endpoint">http://quickstart.cloudera:50070/webhdfs/v1/user/cloudera/%t/data%p-%g.%t.csv?user.name=cloudera</property>
       <property name="type">csv</property>
     </configuration>
   </export>
```

Instructions for running with HTTP export
-----------------------------------------

Here we run the VoltDB database configured to export rows to an HTTP destination, based on conditions set in the stored procedure, CardSwipe. See CardSwipe.java for more details.

1. Start the app-metro dashboard, and browse to it on http://localhost:8081, or some other URL depending on your configuration:
    ./run.sh start_web <port number>

2. Start the export web server:
    ./run.sh start_export_web

   Exported rows can be viewed in the command line output from the web service.

3. Start the VoltDB server:
    ./run.sh export-server

4. Start the client script:
    ./run.sh client

Browse to http://localhost:8081 to see the app-metro dashboard.

Browse to http://localhost:8083/htmlRows to view a continuously refreshing view of the last 10 exported rows.
