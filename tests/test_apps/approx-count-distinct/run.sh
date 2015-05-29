#!/bin/bash

if [ $# -lt 2 ]; then
    echo "Specify total rows and number of distinct values"
    exit 1
fi

pid=`jps | grep VoltDB | sed s/VoltDB//`
if [ "$pid" != "" ]; then
    echo Killing $pid...
    kill $pid
    sleep 1
fi

rm *.jar
rm procedures/approxcountdistinct/*.class
rm client/approxcountdistinct/*.class

javac procedures/approxcountdistinct/*.java
ls procedures/approxcountdistinct/*.class
jar cf procs.jar -C procedures/ approxcountdistinct

javac client/approxcountdistinct/*.java
ls client/approxcountdistinct/*.class
jar cf client.jar -C client/ approxcountdistinct

ls -l *.jar

voltdb create &
sleep 5

sqlcmd < ddl.sql

export CLASSPATH=${CLASSPATH}:${PWD}/client.jar

java -Dlog4j.configuration=file://${LOG4J} approxcountdistinct.Benchmark \
    --numTotalVals $1 --numUniqueVals $2
