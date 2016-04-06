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