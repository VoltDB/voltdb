#!/usr/bin/env bash

APPNAME="genqa"

# find voltdb binaries in either installation or distribution directory.
if [ -n "$(which voltdb 2> /dev/null)" ]; then
    VOLTDB_BIN=$(dirname "$(which voltdb)")
else
    VOLTDB_BIN="$(pwd)/../../../bin"
fi
# installation layout has all libraries in $VOLTDB_ROOT/lib/voltdb
if [ -d "$VOLTDB_BIN/../lib/voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib/voltdb"
    VOLTDB_VOLTDB="$VOLTDB_LIB"
# distribution layout has libraries in separate lib and voltdb directories
elif [ -d "$VOLTDB_BIN/../voltdb" ]; then
    VOLTDB_BASE=$(dirname "$VOLTDB_BIN")
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
else
    VOLTDB_LIB="`pwd`/../../../lib"
    VOLTDB_VOLTDB="`pwd`/../../../voltdb"
fi

CLASSPATH=$({ \
    \ls -1 "$VOLTDB_VOLTDB"/voltdb-*.jar; \
    \ls -1 "$VOLTDB_LIB"/*.jar; \
    \ls -1 "$VOLTDB_LIB"/extension/*.jar;
} 2> /dev/null | paste -sd ':' - )

CLASSPATH=:/home/test/jdbc/vertica-jdbc.jar:/home/test/jdbc/postgresql-9.4.1207.jar:$CLASSPATH
echo CLASSPATH $CLASSPATH
echo VOLTDB_BASE $VOLTDB_BASE
echo VOLTDB_LIB "$VOLTDB_BASE/lib"
echo VOLTDB_VOLTDB $VOLTDB_VOLTDB

# ZK Jars needed to compile kafka verifier. Apprunner uses a nfs shared path.
MYSQLLIB=${MYSQLLIB:-"/home/opt/mysql.jar"}
VERTICALIB=${VERTICALIB:-"/home/opt/vertica-jdbc.jar"}
POSTGRESLIB=${POSTGRESLIB:="/home/opt/postgresql.jar"}
CLASSPATH="$CLASSPATH:vertica-jdbc.jar:$MYSQLLIB"
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"
EXPORTDATA="exportdata"
EXPORTDATAREMOTE="localhost:${PWD}/${EXPORTDATA}"
CLIENTLOG="clientlog"

# remove build artifacts
function clean() {
    rm -rf obj debugoutput sp.jar voltdbroot exportdata clientlog log build
    rm -f ddl1.sql ddl-migrate-ttl.sql ddl-migrate-nottl.sql
    rm -f $VOLTDB_LIB/extension/customexport.jar
}

# compile the source code for procedures and the client
function srccompile() {
    # this will create a sp.jar file
    ant -f build.xml

    # this is needed for the customexporter
    mkdir -p obj

    javac -classpath $CLASSPATH -d obj \
        src/$APPNAME/*.java \
        src/$APPNAME/procedures/*.java
    javac -classpath $CLASSPATH -d obj \
        src/customexport/*.java

    # stop if compilation fails
    if [ $? != 0 ]; then exit; fi
}

# generate sql files from templates
function generateddl() {
    set -x
    ../ddlgen-tool.py ddl1.tmplt > ddl1.sql
    ../ddlgen-tool.py ddl-export-cdc-topic.tmplt > ddl-export-cdc-topic.sql
    ../ddlgen-tool.py ddl-migrate-nottl.tmplt > ddl-migrate-nottl.sql
    ../ddlgen-tool.py ddl-migrate-ttl.tmplt > ddl-migrate-ttl.sql
    ../ddlgen-tool.py ddl-migrate-ttl-topic.tmplt > ddl-migrate-ttl-topic.sql
    set +x
}

# build an application catalog
function jars() {
    srccompile
    generateddl

    # stop if compilation fails
    rm -rf $EXPORTDATA
    mkdir $EXPORTDATA
    rm -fR $CLIENTLOG
    mkdir $CLIENTLOG
    if [ $? != 0 ]; then exit -1; fi
}

function wait-for-create() {
    let status=1
    while [[ $status != 0  ]]; do
        echo "show tables" | $VOLTDB_BIN/sqlcmd
        status=$?
        sleep 5
    done;
}

# run the voltdb server locally
function startvolt() {
    $VOLTDB init -C $1 -s $2 -j $3 --force
    $VOLTDB start  -l $LICENSE &
    wait-for-create
}

function server() {
    startvolt deployment.xml ddl1.sql sp.jar

}

# run the voltdb server locally with kafka connector
function server-kafka() {
    startvolt deployment-kafka.xml ddl1.sql sp.jar
}

# run the voltdb server locally with mysql connector
function server-mysql() {
    cp $MYSQLLIB $VOLTDB_LIB/extension/
    startvolt deployment_mysql.xml ddl1.sql sp.jar
}


# run the voltdb server locally with vertica connector
function server-vertica() {
    cp $VERTICALIB $VOLTDB_LIB/extension/
    startvolt deployment_vertica.xml ddl1.sql sp.jar
}

# run the voltdb server locally with postgresql connector
# to run the postgres jdbc export test manually:
# voltadmin shutdown
# ./run.sh clean
# ./run.sh start-postgres ; # start postgres with correct database name and user credentials
# ./run.sh server-pg # start volt with a export stream to postgress
# ./run.sh async-export; #populate the database tables and export streams
# ./run.sh export-jdbc-postgres-verify ; # verify the tables are conistent between volt and postgres
# ./run.sh stop-postgres;

function server-pg() {
    cp $POSTGRESLIB $VOLTDB_LIB/extension/
    startvolt deployment_pg_nocat.xml ddl1.sql sp.jar
}

# to run the postgres jdbc geo export test manually:
# voltadmin shutdown
# ./run.sh clean
# ./run.sh start-postgres ; # start postgres with correct database name and user credentials
# ./run.sh server-pg-geo # start volt with a export stream to postgress
# ./run.sh async-export-geo; #populate the database tables and export streams
# ./run.sh export-jdbc-postgres-geo-verify ; # verify the tables are conistent between volt and postgres
# ./run.sh stop-postgres;

function server-pg-geo() {
    cp $POSTGRESLIB $VOLTDB_LIB/extension/
    startvolt deployment_pg_nocat.xml ddl1.sql sp.jar

}

function server-custom() {
    if [ ! -f $APPNAME.jar ]; then jars; fi
    # Get custom class in jar
    cd obj
    jar cvf ../customexport.jar customexport/*
    cd ..
    cp customexport.jar $VOLTDB_LIB/extension/customexport.jar
    # run the server
    #$VOLTDB create -d deployment_custom.xml -l $LICENSE -H $HOST $APPNAME.jar
    startvolt deployment_custom.xml ddl1.sql sp.jar
}

# run the client that drives the example
function client() {
    async-benchmark
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.AsyncBenchmark --help
}

function async-benchmark() {
    # srccompile
    java -classpath obj:$CLASSPATH:obj genqa.AsyncBenchmark \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0 \
        --ratelimit=100000
}

function clean-vertica() {
    echo "drop stream export_partitioned_stream" | ssh volt15d /opt/vertica/bin/vsql -U dbadmin test1
}

function async-export-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.AsyncExportClient --help
}

function async-export() {
    srccompile
    rm -rf $CLIENTLOG/*
    mkdir $CLIENTLOG
    echo file:/${PWD}/../../log4j-allconsole.xml
    java -classpath obj:$CLASSPATH:obj genqa.AsyncExportClient \
        --displayinterval=5 \
        --duration=10 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleExportSinglePartition \
        --poolsize=100000 \
        --ratelimit=500 \
        --usetableexport=true \
        --timeout=300
}

function async-export-geo() {
    srccompile
    rm -rf $CLIENTLOG/*
    mkdir $CLIENTLOG
    echo file:/${PWD}/../../log4j-allconsole.xml
    java -classpath obj:$CLASSPATH:obj genqa.AsyncExportClient \
        --displayinterval=5 \
        --duration=10 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleExportGeoSinglePartition \
        --poolsize=100000 \
        --ratelimit=500 \
        --timeout=300
}

# Multi-threaded synchronous benchmark sample
# Use this target for argument help
function sync-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.SyncBenchmark --help
}

function sync-benchmark() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.SyncBenchmark \
        --threads=40 \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0
}

# JDBC benchmark sample
# Use this target for argument help
function jdbc-benchmark-help() {
    srccompile
    java -classpath obj:$CLASSPATH:obj genqa.JDBCBenchmark --help
}

function jdbc-benchmark() {
    # srccompile
    java -classpath obj:$CLASSPATH:obj genqa.JDBCBenchmark \
        --threads=40 \
        --displayinterval=5 \
        --duration=120 \
        --servers=localhost \
        --port=21212 \
        --procedure=JiggleSinglePartition \
        --poolsize=100000 \
        --wait=0
}

# vertica host_port is volt15d:5433
# posgres should be installed locally and is on post 5432
function export-jdbc-postgres-verify() {
    set_pgpaths
    echo $CLASSPATH
    # postgres connection string should be like : jdbc:postgresql://host:port/database -> jdbc:postgresql://localhost:5432:/vexport?user=vexport
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.JDBCVoltVerifier \
        --vdbServers=localhost \
        --driver=org.postgresql.Driver \
        --host_port=localhost:5432 \
        --jdbcUser=vexport \
        --jdbcDatabase=vexport \
        --jdbcDBMS=postgresql
}

function export-jdbc-postgres-geo-verify() {
    set_pgpaths
    echo $CLASSPATH
    # postgres connection string should be like : jdbc:postgresql://host:port/database -> jdbc:postgresql://localhost:5432:/vexport?user=vexport
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.JDBCVoltVerifier \
        --vdbServers=localhost \
        --driver=org.postgresql.Driver \
        --host_port=localhost:5432 \
        --jdbcUser=vexport \
        --jdbcDatabase=vexport \
        --jdbcDBMS=postgresql \
        --usegeo=true
}


# vertica host_port is volt15d:5433
function export-jdbc-vertica-verify() {
    CLASSPATH=$CLASSPATH:/home/test/jdbc/vertica-jdbc-7.2.1-0.jar
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.JDBCVoltVerifier \
        --vdbServers=localhost \
        --driver=com.vertica.jdbc.Driver \
        --host_port=volt15d:5433 \
        --jdbcUser=dbadmin \
        --jdbcDatabase=Test1 \
        --jdbcDBMS=vertica
}

# drop vertica tables so that the JDBC test will create them fresh
function jdbc-drop-vertica-tables() {
    # echo $CLASSPATH
    echo java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj genqa.JDBCVoltVerifier \
        --jdbcDrop=true \
        --vdbServers=localhost \
        --driver=com.vertica.jdbc.Driver \
        --host_port=volt15d:5433 \
        --jdbcUser=dbadmin \
        --jdbcDatabase=Test1 \
        --jdbcDBMS=vertica
}

function export-kafka-server-verify() {
    java -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp -Xmx512m -classpath obj:$CLASSPATH:obj:/home/opt/kafka/libs/zkclient-0.3.jar:/home/opt/kafka/libs/zookeeper-3.3.4.jar \
        genqa.ExportKafkaOnServerVerifier kafka2:2181 voltdbexport
}

function set_pgpaths() {
    export PGTMPDIR=/tmp/`whoami`/genqa/postgress
    export PG_PATH=$(locate pg_restore | grep /bin | grep -v /usr/bin | xargs dirname)
    export CLASSPATH=$CLASSPATH:/home/test/jdbc/postgresql-9.4.1207.jar
}

function start-postgres() {
    set_pgpaths
    mkdir -p $PGTMPDIR
    echo "PGTMPDIR:" $PGTMPDIR
    sudo chown postgres $PGTMPDIR
    echo "PG_PATH:" $PG_PATH

    alias python=python2.6
    # Use the same version of the JDBC jar, on all platforms
    echo "CLASSPATH:" $CLASSPATH

    # Start the PostgreSQL server, in the new temp directory
    # Note: '-o --lc-collate=C' causes varchar sorting to match VoltDB's
    sudo su - postgres -c "$PG_PATH/initdb -A trust -D $PGTMPDIR/data --lc-collate=C"
    sudo su - postgres -c "$PG_PATH/pg_ctl start  -D $PGTMPDIR/data -l $PGTMPDIR/postgres.log"

    # create a user "vexport" that we will use for connecting to postgres through volt via jdbc
    sudo su - postgres -c "$PG_PATH/psql -c \"create role vexport with superuser createdb login\""
    sudo su - postgres -c "$PG_PATH/psql -c \"create database vexport\""
}

function stop-postgres() {
    set_pgpaths
    if [ -d "$PGTMPDIR" ]; then
        sudo su - postgres -c "$PG_PATH/pg_ctl stop -m fast -D $PGTMPDIR/data"
        #sudo su - postgres -c "ls -l $PGTMPDIR/postgres.log"
        #sudo su - postgres -c "cat   $PGTMPDIR/postgres.log"
        sudo rm -r $PGTMPDIR
    fi
}

function help() {
    echo "Usage: ./run.sh {clean|jars|server|async-benchmark|async-benchmark-help|...}"
    echo "       {...|sync-benchmark|sync-benchmark-help|jdbc-benchmark|jdbc-benchmark-help|...}"
    echo "       {...|async-export-help|async-export|async-export-geo}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
if [ $# -gt 1 ]; then help; exit; fi
if [ $# = 1 ]; then $1; else server; fi
