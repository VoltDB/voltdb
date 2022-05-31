#!/usr/bin/env bash

APPNAME="txnid"
set -e
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
    VOLTDB_BASE="`pwd`/../../.."
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
fi

CLASSPATH=$(ls -x "$VOLTDB_VOLTDB"/voltdb-*.jar | tr '[:space:]' ':')$(ls -x "$VOLTDB_LIB"/*.jar | egrep -v 'voltdb[a-z0-9.-]+\.jar' | tr '[:space:]' ':')
VOLTDB="$VOLTDB_BIN/voltdb"
LOG4J="$VOLTDB_VOLTDB/log4j.xml"
if [ -f "$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml" ]; then
    CLIENTLOG4J="$VOLTDB_VOLTDB/../tests/log4j-allconsole.xml"
elif [ -f  $PWD/../../log4j-allconsole.xml ]; then
    CLIENTLOG4J="$PWD/../../log4j-allconsole.xml"
fi
LICENSE="$VOLTDB_VOLTDB/license.xml"
HOST="localhost"

# remove build artifacts
function clean() {
    rm -rf obj log build debugoutput $APPNAME.jar $APPNAME-alt.jar $APPNAME-big-*.jar dummy.jar $APPNAME-noexport.jar voltdbroot
}

# remove everything from "clean" as well as the jarfiles
function cleanall() {
    ant clean
}

# compile the source code for procedures and the client into jarfiles
function jars() {
    ant
    alt-jars
    if [[ -n "$BIGJARS" ]]; then
        bigjars
    fi
}

# compile the procedure and client jarfiles if they don't exist
function jars-ifneeded() {
    if [ ! -e txnid.jar ]; then
        jars
    fi
}

# create an alternate jar that is functionally equivalent
# but has a different checksum
function alt-jars() {
    if [ -e txnid.jar ]; then
        mv txnid.jar txnid-orig.jar
    fi
    # src/txnIdSelfCheck/procedures/
    cp src/txnIdSelfCheck/procedures/ReadSP.java src/txnIdSelfCheck/procedures/ReadSP.java.orig
    cp src/txnIdSelfCheck/procedures/UpdateBaseProc.java src/txnIdSelfCheck/procedures/UpdateBaseProc.java.orig
    # on a Mac, sed needs an extra empty-string ('') arg
    QT=
    if [[ "$OSTYPE" == darwin* ]]; then
        QT="''"
    fi
    sed -i $QT 's/SELECT \* FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc/SELECT \* FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc limit 1000000/' src/txnIdSelfCheck/procedures/ReadSP.java
    sed -i $QT 's/SELECT count(\*) FROM dimension where cid = ?/SELECT count(\*) FROM dimension where cid = ? limit 1000000/' src/txnIdSelfCheck/procedures/UpdateBaseProc.java
    sed -i $QT 's/SELECT \* FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc/SELECT \* FROM partitioned p INNER JOIN dimension d ON p.cid=d.cid WHERE p.cid = ? ORDER BY p.cid, p.rid desc limit 1000000/' src/txnIdSelfCheck/procedures/UpdateBaseProc.java
    sed -i $QT 's/SELECT \* FROM partview WHERE cid=? ORDER BY cid DESC/SELECT \* FROM partview WHERE cid=? ORDER BY cid DESC limit 1000000/' src/txnIdSelfCheck/procedures/UpdateBaseProc.java
    sed -i $QT 's/SELECT \* FROM ex_partview WHERE cid=? ORDER BY cid DESC/SELECT \* FROM ex_partview WHERE cid=? ORDER BY cid DESC limit 1000000/' src/txnIdSelfCheck/procedures/UpdateBaseProc.java
    sed -i $QT 's/SELECT \* FROM ex_partview_shadow WHERE cid=? ORDER BY cid DESC/SELECT \* FROM ex_partview_shadow WHERE cid=? ORDER BY cid DESC limit 1000000/' src/txnIdSelfCheck/procedures/UpdateBaseProc.java

    # keep a copy for debugging
    cp src/txnIdSelfCheck/procedures/ReadSP.java src/txnIdSelfCheck/procedures/ReadSP.java.alt
    cp src/txnIdSelfCheck/procedures/UpdateBaseProc.java src/txnIdSelfCheck/procedures/UpdateBaseProc.java.alt
    ant clean
    ant

    cp txnid.jar txnid-alt.jar
    mv txnid-orig.jar txnid.jar
    mv src/txnIdSelfCheck/procedures/ReadSP.java.orig src/txnIdSelfCheck/procedures/ReadSP.java
    mv src/txnIdSelfCheck/procedures/UpdateBaseProc.java.orig src/txnIdSelfCheck/procedures/UpdateBaseProc.java
}

# create alternate jars that are functionally equivalent but
# each includes some very large files (40Mb < size < 50Mb)
function bigjars() {
    echo -e `date`": Running function bigjars..."

    # call jars-ifneeded, to make sure that txnid.jar has already been
    # generated; but make sure not to run this function twice as a result
    if [[ $DONT_RUN_BIG_JARS_TWICE -gt 0 ]]; then
        return
    fi
    DONT_RUN_BIG_JARS_TWICE=1
    jars-ifneeded

    # make copies of the standard txnid.jar, to be added to below
    cp -fv txnid.jar txnid-big-text1.jar
    cp -fv txnid.jar txnid-big-text2.jar
    cp -fv txnid.jar txnid-big-text3.jar
    cp -fv txnid.jar txnid-big-text4.jar

    # find the voltdb-X.X.jar file
    JAR_NAME=`ls $VOLTDB_VOLTDB | grep .jar | grep -v client`
    VOLTDB_JAR=$VOLTDB_VOLTDB/$JAR_NAME

    # compile the program used to create large (text or java) files
    if [[ ! -d "obj" ]]; then
        mkdir obj
    fi
    set +e
    javac -cp $VOLTDB_JAR -d obj src/bigjar/CreateLargeFiles.java
    # stop if compilation fails, and print some debug info
    if [[ $? != 0 ]]; then
        echo -e "Compilation failed, with:\nVOLTDB_VOLTDB: $VOLTDB_VOLTDB"
        echo -e "JAR_NAME     : $JAR_NAME\nVOLTDB_JAR   : $VOLTDB_JAR"
        exit 10
    fi
    echo -e `date`": Completed compilation of src/bigjar/CreateLargeFiles.java"

    # create large (random) text files
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -o obj/large-random-text1.txt
    if [ $? != 0 ]; then echo "Failed to create large-random-text1.txt"; exit 11; fi
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -o obj/large-random-text2.txt
    if [ $? != 0 ]; then echo "Failed to create large-random-text2.txt"; exit 12; fi
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -o obj/large-random-text3.txt
    if [ $? != 0 ]; then echo "Failed to create large-random-text3.txt"; exit 13; fi
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -o obj/large-random-text4.txt
    if [ $? != 0 ]; then echo "Failed to create large-random-text4.txt"; exit 14; fi
    echo -e `date`": Completed generation of large text files"

    # add different large text files to various copies of the standard txnid.jar
    jar uf txnid-big-text1.jar obj/large-random-text1.txt
    if [ $? != 0 ]; then echo "Failed to update txnid-big-text1.jar"; exit 15; fi
    jar uf txnid-big-text2.jar obj/large-random-text2.txt
    if [ $? != 0 ]; then echo "Failed to update txnid-big-text2.jar"; exit 16; fi
    jar uf txnid-big-text3.jar obj/large-random-text3.txt
    if [ $? != 0 ]; then echo "Failed to update txnid-big-text3.jar"; exit 17; fi
    jar uf txnid-big-text4.jar obj/large-random-text4.txt
    if [ $? != 0 ]; then echo "Failed to update txnid-big-text4.jar"; exit 18; fi
    echo -e `date`": Completed generation of jar files containing large text files."


    if [[ -e txnid-big-java1.jar && -e txnid-big-java2.jar ]]; then
        set -e
        echo -e `date`": No need to generate jar files containing lots of Java files:"
        echo -e `date`"      txnid-big-java1.jar, txnid-big-java2.jar already exist"
        echo -e `date`": Completed generation of ALL big-jar files."
        return
    fi

    # if VOLTCORE is set, use it to find the relevant test directory
    if [[ -d ${VOLTCORE}/tests/test_apps/txnid-selfcheck2 ]]; then
        VOLTDB_TEST="${VOLTCORE}/tests/test_apps/txnid-selfcheck2"
    # voltdb (community) build has relevant tests 3 levels below the "base" (voltdb) directory
    elif [[ -d ${VOLTDB_BASE}/tests/test_apps/txnid-selfcheck2 ]]; then
        VOLTDB_TEST="${VOLTDB_BASE}/tests/test_apps/txnid-selfcheck2"
    # pro build has to find the tests below the adjacent 'voltdb' (community) or 'internal' directory
    elif [[ -d ${VOLTDB_BASE}/../../../../voltdb/tests/test_apps/txnid-selfcheck2 ]]; then
        VOLTDB_TEST="${VOLTDB_BASE}/../../../../voltdb/tests/test_apps/txnid-selfcheck2"
    elif [[ -d ${VOLTDB_BASE}/../../../../internal/tests/test_apps/txnid-selfcheck2 ]]; then
        VOLTDB_TEST="${VOLTDB_BASE}/../../../../internal/tests/test_apps/txnid-selfcheck2"
    fi
    if [[ -e ${VOLTDB_TEST}/txnid-big-java1.jar && -e ${VOLTDB_TEST}/txnid-big-java2.jar ]]; then
        set -e
        cp -fv ${VOLTDB_TEST}/txnid-big-java1.jar ./txnid-big-java1.jar
        cp -fv ${VOLTDB_TEST}/txnid-big-java2.jar ./txnid-big-java2.jar
        echo -e `date`": No need to generate jar files containing lots of Java files:"
        echo -e `date`"      txnid-big-java1.jar, txnid-big-java2.jar copied from/to:"
        echo -e `date`"      ${VOLTDB_TEST}"
        echo -e `date`"      "`pwd`
        echo -e `date`": Completed generation of ALL big-jar files."
        return
    fi
    echo -e `date`": Beginning generation of jar files containing lots of Java classes..."


    # make copies of the standard txnid.jar, to be added to below
    cp -fv txnid.jar txnid-big-java1.jar
    cp -fv txnid.jar txnid-big-java2.jar

    # create many small, almost identical, Java source (.java) files
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -f java -m 100000 -M 123000
    if [ $? != 0 ]; then echo "Failed to create lots of Java files (1)"; exit 21; fi
    java -cp obj:$VOLTDB_JAR bigjar.CreateLargeFiles -f java -m 200000 -M 223000
    if [ $? != 0 ]; then echo "Failed to create lots of Java files (2)"; exit 22; fi
    echo -e `date`": Completed generation of lots of Java files"

    # compile the various Java source (.java files)
    javac -cp $VOLTDB_JAR -d obj `pwd`/src/bigjar/procedures/Insert*
    if [ $? != 0 ]; then echo "Failed to compile the Java Insert class"; exit 23; fi
    # the list of SelectLE* classes is too long for javac, so break it up
    for i in {1..2}; do
        for j in {0..2}; do
            for k in {0..9}; do
                if [[ -e `pwd`/src/bigjar/procedures/SelectLE${i}${j}${k}000.java ]]; then
                    javac -cp $VOLTDB_JAR -d obj `pwd`/src/bigjar/procedures/SelectLE${i}${j}${k}*
                    if [ $? != 0 ]; then echo "Failed to compile the Java SelectLE${i}${j}${k}* classes"; exit 24; fi
                fi
            done
            echo -e `date`": Completed compilation of SelectLE${i}${j}*.java"
        done
    done
    echo -e `date`": Completed compilation of all Java files"

    # add various Java source (.java) and compiled (.class) files to copies
    # of the standard txnid.jar;
    jar uf txnid-big-java1.jar obj/bigjar/procedures/Insert* src/bigjar/procedures/Insert*
    if [ $? != 0 ]; then echo "Failed to add the Java Insert class to txnid-big-java1.jar"; exit 25; fi
    jar uf txnid-big-java2.jar obj/bigjar/procedures/Insert* src/bigjar/procedures/Insert*
    if [ $? != 0 ]; then echo "Failed to add the Java Insert class to txnid-big-java2.jar"; exit 26; fi
    # the list of SelectLE* classes is too long for jar, so break it up
    for i in {1..2}; do
        for j in {0..2}; do
            for k in {0..9}; do
                if [[ -e `pwd`/obj/bigjar/procedures/SelectLE${i}${j}${k}000.class ]]; then
                    jar uf txnid-big-java${i}.jar obj/bigjar/procedures/SelectLE${i}${j}${k}*.class
                    if [ $? != 0 ]; then echo "Failed to add SelectLE1${i}${j}*.class to txnid-big-java${i}.jar"; exit 27; fi
                    jar uf txnid-big-java${i}.jar src/bigjar/procedures/SelectLE${i}${j}${k}*.java
                    if [ $? != 0 ]; then echo "Failed to add SelectLE1${i}${j}*.java  to txnid-big-java${i}.jar"; exit 28; fi
                    rm -f obj/bigjar/procedures/SelectLE${i}${j}${k}*.class
                    rm -f src/bigjar/procedures/SelectLE${i}${j}${k}*.java
                fi
            done
            echo -e `date`": Completed addition of SelectLE${i}${j}*.class, SelectLE${i}${j}*.java to txnid-big-java${i}.jar"
        done
        echo -e `date`": Completed additions to txnid-big-java${i}.jar"
    done

    set -e
    echo -e `date`": Completed generation of ALL big-jar files."
}

# run the voltdb server locally
function server() {
    jars-ifneeded
    # run the server
    $VOLTDB init -C deployment.xml --force
    $VOLTDB start -l $LICENSE -H $HOST
}

# run the voltdb server locally with priority queues enabled
function server-priority() {
    jars-ifneeded
    # run the server
    $VOLTDB init -C deployment2.xml --force
    $VOLTDB start -l $LICENSE -H $HOST
}

# run the client that drives the example
function client() {
    async-benchmark
}

# run the client that drives the example, using V2 client
function client2() {
    async-benchmark2
}

# run the client that drives the example, using V2 client, with random priorities
function client2p() {
    async-benchmark2p
}

# Asynchronous benchmark sample
# Use this target for argument help
function async-benchmark-help() {
    jars-ifneeded
    java -classpath $CLASSPATH:txnid.jar txnIdSelfCheck.Benchmark --help
}

function async-benchmark() {
    jars-ifneeded
    java -ea -classpath txnid.jar:$CLASSPATH: -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.Benchmark $ARGS \
        --displayinterval=1 \
        --duration=30 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=20 \
        --usecompression=false \
        --allowinprocadhoc=false
        # --enabledthreads=partttlMigratelt,replttlMigratelt
        # --disabledthreads=ddlt,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,partTrunclt,replTrunclt
#ddlt,clients,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,readThread,partTrunclt,replTrunclt
        # --sslfile=./keystore.props
}

function async-benchmark2() {
    jars-ifneeded
    java -ea -classpath txnid.jar:$CLASSPATH: -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.Benchmark $ARGS \
        --displayinterval=1 \
        --duration=100 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --useclientv2=true \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=20 \
        --usecompression=false \
        --allowinprocadhoc=false
        # --enabledthreads=partttlMigratelt,replttlMigratelt
        # --disabledthreads=ddlt,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,partTrunclt,replTrunclt
#ddlt,clients,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,readThread,partTrunclt,replTrunclt
        # --sslfile=./keystore.props
}

function async-benchmark2p() {
    jars-ifneeded
    java -ea -classpath txnid.jar:$CLASSPATH: -Dlog4j.configuration=file://$CLIENTLOG4J \
        txnIdSelfCheck.Benchmark $ARGS \
        --displayinterval=1 \
        --duration=100 \
        --servers=localhost \
        --threads=20 \
        --threadoffset=0 \
        --useclientv2=true \
        --usepriorities=true \
        --minvaluesize=1024 \
        --maxvaluesize=1024 \
        --entropy=127 \
        --fillerrowsize=10240 \
        --replfillerrowmb=32 \
        --partfillerrowmb=128 \
        --progresstimeout=20 \
        --usecompression=false \
        --allowinprocadhoc=false
        # --enabledthreads=partttlMigratelt,replttlMigratelt
        # --disabledthreads=ddlt,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,partTrunclt,replTrunclt
#ddlt,clients,partBiglt,replBiglt,partCappedlt,replCappedlt,replLoadlt,partLoadlt,adHocMayhemThread,idpt,readThread,partTrunclt,replTrunclt
        # --sslfile=./keystore.props
}

function init() {
    jars-ifneeded
    sqlcmd < src/txnIdSelfCheck/ddl.sql
}

function help() {
    echo "Usage: ./run.sh {clean|jars|bigjars|server|init|async-benchmark|async-benchmark-help}"
}

# Run the target passed as the first arg on the command line
# If no first arg, run server
# If more than one arg, pass the rest to the client (async-benchmark)
ARGS=""
if [ $# -gt 1 ]; then
    if [ $1 = "client" ] || [ $1 = "async-benchmark" ]; then
        ARGS="${@:2}";
    else
        help; exit;
    fi;
fi
if [ $# -ge 1 ]; then $1; else server; fi
