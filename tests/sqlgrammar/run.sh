#!/bin/bash

# Remember the directory where we started, and find the <voltdb> and
# <voltdb>/tests/sqlgrammar/ directories, and the <voltdb>/bin directory
# containing the VoltDB binaries, plus the UDF test directory; and set
# variables accordingly
function find-directories() {
    HOME_DIR=$(pwd)
    if [[ -e $HOME_DIR/tests/sqlgrammar/run.sh ]]; then
        # It looks like we're running from the <voltdb> directory
        VOLTDB_DIR=$HOME_DIR
        VOLTDB_BIN=$VOLTDB_DIR/bin
        SQLGRAMMAR_DIR=$VOLTDB_DIR/tests/sqlgrammar
        UDF_TEST_DIR=$VOLTDB_DIR/tests/testfuncs/org/voltdb_testfuncs
    elif [[ $HOME_DIR == */tests/sqlgrammar ]] && [[ -e $HOME_DIR/run.sh ]]; then
        # It looks like we're running from the <voltdb>/tests/sqlgrammar/ directory
        SQLGRAMMAR_DIR=$HOME_DIR
        VOLTDB_DIR=$(cd $SQLGRAMMAR_DIR/../..; pwd)
        VOLTDB_BIN=$VOLTDB_DIR/bin
        UDF_TEST_DIR=$VOLTDB_DIR/tests/testfuncs/org/voltdb_testfuncs
    elif [[ -n "$(which voltdb 2> /dev/null)" ]]; then
        # It looks like we're using VoltDB from the PATH
        VOLTDB_BIN=$(dirname "$(which voltdb)")
        VOLTDB_DIR=$(cd $VOLTDB_BIN/..; pwd)
        SQLGRAMMAR_DIR=$VOLTDB_DIR/tests/sqlgrammar
        UDF_TEST_DIR=$VOLTDB_DIR/tests/testfuncs/org/voltdb_testfuncs
    else
        echo "Unable to find VoltDB installation."
        echo "Please add VoltDB's bin directory to your PATH."
        exit -1
    fi    
}

# Find the directories and set variables, only if not done already
function find-directories-if-needed() {
    if [[ -z $HOME_DIR || -z $VOLTDB_DIR || -z $VOLTDB_BIN || -z $SQLGRAMMAR_DIR || -z $UDF_TEST_DIR ]]; then
        find-directories
    fi
}

# Set CLASSPATH, PATH, DEFAULT_ARGS, MINUTES, and python, as needed
function init() {
    find-directories-if-needed
    echo -e "\n$0 performing: init"

    # Set CLASSPATH to include the VoltDB Jar file
    VOLTDB_JAR=$(ls $VOLTDB_DIR/voltdb/voltdb-*.jar)
    if [[ -z $CLASSPATH ]]; then
        CLASSPATH=$VOLTDB_JAR
    else
        CLASSPATH=$VOLTDB_JAR:$CLASSPATH
    fi

    # Set PATH to include the voltdb/bin directory, containing the voltdb and sqlcmd executables
    if [[ -z $(which voltdb) || -z $(which sqlcmd) ]]; then
        PATH=$VOLTDB_BIN:$PATH
    fi

    # Set the default value of the args to pass to SQL-grammar-gen
    DEFAULT_ARGS="--initial_number=100 --log=voltdbroot/log/volt.log,volt_console.out"

    # Set MINUTES to a default value, if it is unset
    if [[ -z $MINUTES ]]; then
        MINUTES=1
    fi

    # Set python to use version 2.7
    alias python=python2.7

    INIT_WAS_RUN="true"
}

# Set CLASSPATH, PATH, DEFAULT_ARGS, MINUTES, and python, only if not set already
function init-if-needed() {
    if [[ -z $INIT_WAS_RUN ]]; then
        init
    fi
}

function debug() {
    init-if-needed
    echo -e "\n$0 performing: debug"

    echo "HOME_DIR      :" $HOME_DIR
    echo "VOLTDB_DIR    :" $VOLTDB_DIR
    echo "VOLTDB_BIN    :" $VOLTDB_BIN
    echo "VOLTDB_JAR    :" $VOLTDB_JAR
    echo "SQLGRAMMAR_DIR:" $SQLGRAMMAR_DIR
    echo "UDF_TEST_DIR  :" $UDF_TEST_DIR
    echo "DEFAULT_ARGS  :" $DEFAULT_ARGS
    echo "SEED     :" $SEED
    echo "MINUTES  :" $MINUTES
    echo "PATH     :" $PATH
    echo "CLASSPATH:" $CLASSPATH
    echo "which python    :" `which python`
    echo "python --version:"
    python --version
    echo "which sqlcmd    :" `which sqlcmd`
    echo "which voltdb    :" `which voltdb`
    echo "voltdb --version:" `$VOLTDB_BIN/voltdb --version`
}

# Build VoltDB
function build() {
    find-directories-if-needed
    echo -e "\n$0 performing: build"

    cd $VOLTDB_DIR
    ant killstragglers clean dist
    cd -
}

# Compile the Java stored procedures (& user-defined functions), and create the Jar files
function jars() {
    init-if-needed
    echo -e "\n$0 performing: jars"

    # Compile the classes and build the main jar file for the SQL-grammar-gen tests
    mkdir -p obj
    javac -d ./obj $SQLGRAMMAR_DIR/procedures/sqlgrammartest/*.java
    # TODO: We don't want the Stored Procedures' base class??
#    rm obj/sqlgrammartest/InsertRow.class
    jar cvf testgrammar.jar -C obj sqlgrammartest

    # Compile the classes and build the jar files for the UDF tests
    cd ../testfuncs
    ./build_udf_jar.sh
    mv testfuncs*.jar $HOME_DIR
    cd -
}

# Create the Jar files, only if not created already
function jars-if-needed() {
    if [[ ! -e testgrammar.jar || ! -e testfuncs.jar ]]; then
        jars
    fi
}

# Start the VoltDB server
function server() {
    find-directories-if-needed
    echo -e "\n$0 performing: server"

    $VOLTDB_BIN/voltdb init --force
    $VOLTDB_BIN/voltdb start > volt_console.out 2>&1 &
    # TODO: improve this waiting method (??):
    sleep 60

    # Prevent exit before stopping the VoltDB server, if the SQL grammar generator tests fail
    set +e
    echo "VoltDB Server is running..."
}

# Start the VoltDB server, only if not already running
function server-if-needed() {
    if [[ -z $(ps -ef | grep -i voltdb | grep -v "grep -i voltdb") ]]; then
        server
    else
        echo -e "\nNot starting a VoltDB server, because ps -ef includes:\n   " $(ps -ef | grep -i voltdb | grep -v "grep -i voltdb")
    fi
}

# Load the main SQL-grammar-gen schema and procedures, plus the UDF functions
function ddl() {
    find-directories-if-needed
    jars-if-needed
    server-if-needed
    echo -e "\n$0 performing: ddl"

    $VOLTDB_BIN/sqlcmd < $SQLGRAMMAR_DIR/DDL.sql
    code1a=$?
    $VOLTDB_BIN/sqlcmd < $UDF_TEST_DIR/UserDefinedTestFunctions-drop.sql
    code1b=$?
    $VOLTDB_BIN/sqlcmd < $UDF_TEST_DIR/UserDefinedTestFunctions-load.sql
    code1c=$?
    $VOLTDB_BIN/sqlcmd < $UDF_TEST_DIR/UserDefinedTestFunctions-DDL.sql
    code1d=$?

    code1=$(($code1a|$code1b|$code1c|$code1d))
}

# Load the schema and procedures (in sqlcmd), only if not loaded already
function ddl-if-needed() {
    if [[ -z $code1 ]]; then
        ddl
    fi
}

# Run the SQL-grammr-generator tests, only
function tests-only() {
    init-if-needed
    echo -e "\n$0 performing: tests$ARGS"

    echo -e "running:\n    python sql_grammar_generator.py $DEFAULT_ARGS --minutes=$MINUTES --seed=$SEED $ARGS"
    python $SQLGRAMMAR_DIR/sql_grammar_generator.py $DEFAULT_ARGS --minutes=$MINUTES --seed=$SEED $ARGS
    code2=$?
}

# Run the SQL-grammr-generator tests, with the usual prerequisites
function tests() {
    server-if-needed
    ddl-if-needed
    tests-only
}

# Stop the VoltDB server, and other clean-up
function shutdown() {
    find-directories-if-needed
    echo -e "\n$0 performing: shutdown"

    # Stop the VoltDB server (& any stragglers)
    $VOLTDB_BIN/voltadmin shutdown
    code3=$?
    cd $VOLTDB_DIR
    ant killstragglers
    cd -    

    # Compress the VoltDB server console output & log files
    gzip -f volt_console.out
    gzip -f voltdbroot/log/volt.log

    # Delete any class files added to the /obj directory (and the directory, if empty)
    rm obj/sqlgrammartest/*.class
    rmdir obj/sqlgrammartest
    rmdir obj
}

function all() {
    echo -e "\n$0 performing: all$ARGS"
    init
    debug
    build
    jars
    server
    ddl
    tests
    shutdown
}

function tests-help() {
    find-directories-if-needed
    python $SQLGRAMMAR_DIR/sql_grammar_generator.py --help
}

function help() {
    echo -e "\nUsage: ./run.sh {init|debug|build|jars|server|ddl|tests-only|tests|shutdown|all|tests-help|help}"
    echo -e "Multiple options may be specified; options (except 'tests-only') generally call other options that are prerequisites."
    echo -e "The 'tests-only', 'tests', and 'all' options accept arguments: see 'tests-help' for details.\n"
}

# Check the exit code(s), and exit
function exit-with-code() {
    find-directories-if-needed
    cd $HOME_DIR

    code=0
    if [[ -n $code1 || -n $code2 || -n $code3 ]]; then
        if [[ -z $code1 ]]; then
            code1=0
        fi
        if [[ -z $code2 ]]; then
            code2=0
        fi
        if [[ -z $code3 ]]; then
            code3=0
        fi
        code=$(($code1|$code2|$code3))
        if [[ "$code1" -ne "0" ]]; then
            echo -e "\ncode1a code1b code1c code1d: $code1a $code1b $code1c $code1d (grammar-ddl, UDF-drop, UDF-load, UDF-ddl)"
        fi
        if [[ "$code" -ne "0" ]]; then
            echo -e "\ncode1 code2 code3: $code1 $code2 $code3 (ddl, tests, shutdown)"
            echo "code:" $code
        fi
    fi
    exit $code
}

# If no options specified, run help
if [[ $# -eq 0 ]]; then
    help
fi

# Run the options passed on the command line
while [[ -n "$1" ]]; do
    CMD="$1"
    ARGS=
    if [[ "$1" == "tests" || "$1" == "tests-only" || "$1" == "all" ]]; then
        while [[ "$2" == -* ]]; do
            if [[ "$2" == --* ]]; then
                ARGS="$ARGS $2"
            else
                ARGS="$ARGS $2 $3"
                shift
            fi
            shift
        done
    fi
    $CMD
    shift
done
exit-with-code
