#!/bin/bash
# Script used to run the SQL-grammar-generator tests, including all the steps
# needed for the tests to work, such as: building VoltDB, creating Jar files,
# starting a VoltDB server, running the associated DDL files, passing arguments
# to the tests themselves, and shutting down the VoltDB server at the end.
# These steps may be run separately, or all together.

# Run the <voltdb>/tests/test-tools.sh script, which contains useful functions
function run-test-tools() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: run-test-tools"
    fi
    HOME_DIR=$(pwd)
    if [[ -e $HOME_DIR/tests/test-tools.sh ]]; then
        # It looks like we're running from a <voltdb> directory
        VOLTDB_TESTS=$HOME_DIR/tests
    elif [[ $HOME_DIR == */tests ]] && [[ -e $HOME_DIR/test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/ directory
        VOLTDB_TESTS=$HOME_DIR
    elif [[ $HOME_DIR == */tests/* ]] && [[ -e $HOME_DIR/../test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/sqlgrammar/ directory
        # (or a similar directory, just below /tests/)
        VOLTDB_TESTS=$(cd $HOME_DIR/..; pwd)
    elif [[ -n "$(which voltdb 2> /dev/null)" ]]; then
        # It looks like we're using VoltDB from the PATH
        # TODO: this won't work with a 'pro' build of VoltDB
        VOLTDB_BIN_DIR=$(dirname "$(which voltdb)")
        VOLTDB_TESTS=$(cd $VOLTDB_BIN_DIR/../tests; pwd)
    else
        echo "Unable to find VoltDB installation."
        echo "Please add VoltDB's bin directory to your PATH."
        exit -1
    fi
    source $VOLTDB_TESTS/test-tools.sh
}

# Remember the directory where we started, and find the <voltdb> and
# <voltdb>/tests/sqlgrammar/ directories, and the <voltdb>/bin directory
# containing the VoltDB binaries, plus the UDF test directories; and set
# variables accordingly
function find-directories() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: find-directories"
    fi
    test-tools-find-directories-if-needed
    SQLGRAMMAR_DIR=$VOLTDB_TESTS/sqlgrammar
    UDF_TEST_DIR=$VOLTDB_TESTS/testfuncs
    UDF_TEST_DDL=$UDF_TEST_DIR/org/voltdb_testfuncs
}

# Find the directories and set variables, only if not set already
function find-directories-if-needed() {
    if [[ -z "$SQLGRAMMAR_DIR" || -z "$UDF_TEST_DIR" || -z "$UDF_TEST_DDL" ]]; then
        find-directories
    fi
}

# Build VoltDB: 'community', open-source version
function build() {
    echo -e "\n$0 performing: build$BUILD_ARGS"
    test-tools-build
    code[0]=$code_tt_build
}

# Build VoltDB: 'pro' version
function build-pro() {
    echo -e "\n$0 performing: build-pro$BUILD_ARGS"
    test-tools-build-pro
    code[0]=$code_tt_build
}

# Build VoltDB, only if not built already
function build-if-needed() {
    test-tools-build-if-needed
    code[0]=$code_tt_build
}

# Build VoltDB 'pro' version, only if not built already
function build-pro-if-needed() {
    test-tools-build-pro-if-needed
    code[0]=$code_tt_build
}

# Set CLASSPATH, PATH, python, DEFAULT_ARGS, and MINUTES, as needed
function init() {
    find-directories-if-needed
    build-if-needed
    echo -e "\n$0 performing: init"
    test-tools-init

    # Set the default value of the args to pass to SQL-grammar-gen
    DEFAULT_ARGS="--path=$SQLGRAMMAR_DIR --initial_number=100 --log=voltdbroot/log/volt.log,volt_console.out"

    # Set MINUTES to a default value, if it is unset
    if [[ -z "$MINUTES" ]]; then
        MINUTES=1
    fi

    code[1]=${code_tt_init}
}

# Set CLASSPATH, PATH, DEFAULT_ARGS, MINUTES, and python, only if not set already
function init-if-needed() {
    if [[ -z "${code[1]}" ]]; then
        init
    fi
}

# Print the values of various variables, including those set in the
# find-directories() and init() functions
function debug() {
    init-if-needed
    echo -e "\n$0 performing: debug"
    test-tools-debug

    echo "HOME_DIR       :" $HOME_DIR
    echo "SQLGRAMMAR_DIR :" $SQLGRAMMAR_DIR
    echo "UDF_TEST_DIR   :" $UDF_TEST_DIR
    echo "UDF_TEST_DDL   :" $UDF_TEST_DDL
    echo "BUILD_ARGS     :" $BUILD_ARGS
    echo "BUILD_UDF_ARGS :" $BUILD_UDF_ARGS
    echo "DEFAULT_ARGS   :" $DEFAULT_ARGS
    echo "ARGS           :" $ARGS
    echo "SEED           :" $SEED
    echo "MINUTES        :" $MINUTES
    echo "SUFFIX         :" $SUFFIX
}

# Compile the Java stored procedures (& user-defined functions), and create the Jar files
function jars() {
    init-if-needed
    echo -e "\n$0 performing: jars"

    # Compile the classes and build the main jar file for the SQL-grammar-gen tests
    mkdir -p obj
    javac -cp $CLASSPATH -d ./obj $SQLGRAMMAR_DIR/procedures/sqlgrammartest/*.java
    code2a=$?
    jar cvf testgrammar.jar -C obj sqlgrammartest
    code2b=$?

    # Compile the classes and build the jar files for the UDF tests
    BUILD_UDF_ARGS=
    if [[ "$BUILD_ARGS" == *-Dbuild=debug*  ]]; then
        BUILD_UDF_ARGS="--build=debug"
    fi

    cd $UDF_TEST_DIR
    ./build_udf_jar.sh $BUILD_UDF_ARGS
    code2c=$?
    mv testfuncs*.jar $HOME_DIR
    code2d=$?
    cd -

    code[2]=$(($code2a|$code2b|$code2c|$code2d))
}

# Create the Jar files, only if not created already
function jars-if-needed() {
    if [[ ! -e testgrammar.jar || ! -e testfuncs.jar ]]; then
        jars
    fi
}

# Start the VoltDB server: 'community', open-source version
function server() {
    echo -e "\n$0 performing: server"
    test-tools-server
    code[3]=${code_tt_server}
}

# Start the VoltDB server: 'pro' version
function server-pro() {
    echo -e "\n$0 performing: server-pro"
    test-tools-server-pro
    code[3]=${code_tt_server}
}

# Start the VoltDB 'community' server, only if not already running
function server-if-needed() {
    test-tools-server-if-needed
    code[3]=${code_tt_server}
}

# Start the VoltDB 'pro' server, only if not already running
function server-pro-if-needed() {
    test-tools-server-pro-if-needed
    code[3]=${code_tt_server}
}

# Load the main SQL-grammar-gen schema and procedures, plus the UDF functions
function ddl() {
    find-directories-if-needed
    jars-if-needed
    server-if-needed

    echo -e "\n$0 performing: ddl; running (in sqlcmd): $SQLGRAMMAR_DIR/DDL.sql"
    $VOLTDB_BIN_DIR/sqlcmd < $SQLGRAMMAR_DIR/DDL.sql
    code4a=$?
    echo -e "\n$0 performing: ddl; running (in sqlcmd): $UDF_TEST_DDL/UserDefinedTestFunctions-drop.sql"
    $VOLTDB_BIN_DIR/sqlcmd < $UDF_TEST_DDL/UserDefinedTestFunctions-drop.sql
    code4b=$?
    echo -e "\n$0 performing: ddl; running (in sqlcmd): $UDF_TEST_DDL/UserDefinedTestFunctions-load.sql"
    $VOLTDB_BIN_DIR/sqlcmd < $UDF_TEST_DDL/UserDefinedTestFunctions-load.sql
    code4c=$?

    cd $UDF_TEST_DDL
    echo -e "\n$0 performing: ddl; running (in sqlcmd): $UDF_TEST_DDL/UserDefinedTestFunctions-batch.sql"
    $VOLTDB_BIN_DIR/sqlcmd < UserDefinedTestFunctions-batch.sql
    code4d=$?
    cd -

    code[4]=$(($code4a|$code4b|$code4c|$code4d))
}

# Load the schema and procedures (in sqlcmd), only if not loaded already
function ddl-if-needed() {
    # TODO: find a more reliable test of whether 'ddl' has been loaded
    if [[ -z "${code[4]}" ]]; then
        ddl
    fi
}

# Run everything you need to before running the SQL-grammar-generator tests,
# using a 'community', open-source build, without actually running them;
# provides a "fresh start", i.e., re-builds the latest versions of VoltDB
# ('community'), the Jar files, etc.
function prepare() {
    echo -e "\n$0 performing: prepare"
    build
    init
    debug
    jars
    server
    ddl
}

# Run everything you need to before running the SQL-grammar-generator tests,
# using a 'pro' build, without actually running them; provides a "fresh start",
# i.e., re-builds the latest versions of VoltDB ('pro'), the Jar files, etc.
function prepare-pro() {
    echo -e "\n$0 performing: prepare-pro"
    build-pro
    init
    debug
    jars
    server-pro
    ddl
}

# Run the SQL-grammar-generator tests, only, on the assumption that 'prepare'
# (or the equivalent) has already been run
function tests-only() {
    init-if-needed
    echo -e "\n$0 performing: tests[-only]$ARGS"

    TEST_COMMAND="python $SQLGRAMMAR_DIR/sql_grammar_generator.py $DEFAULT_ARGS --minutes=$MINUTES --seed=$SEED $ARGS"
    echo -e "running:\n$TEST_COMMAND"
    $TEST_COMMAND
    code[5]=$?
}

# Run the SQL-grammar-generator tests, with the usual prerequisites
function tests() {
    server-if-needed
    ddl-if-needed
    tests-only
}

# Stop the VoltDB server, and other clean-up
function shutdown() {
    find-directories-if-needed
    SUFFIX_INFO=
    if [[ -n "$SUFFIX" ]]; then
        SUFFIX_INFO=" --suffix=$SUFFIX"
    fi
    echo -e "\n$0 performing: shutdown$SUFFIX_INFO"

    # Stop the VoltDB server (& any stragglers)
    test-tools-shutdown
    code[6]=${code_tt_shutdown}

    # Compress the VoltDB server console output & log files; and the files
    # containing their (Java) Exceptions, and other ERROR messages
    mv volt_console.out volt_console$SUFFIX.out
    mv voltdbroot/log/volt.log voltdbroot/log/volt$SUFFIX.log
    gzip -f volt_console$SUFFIX.out
    gzip -f voltdbroot/log/volt$SUFFIX.log
    gzip -f exceptions_in_volt$SUFFIX.log
    gzip -f exceptions_in_volt_console$SUFFIX.out
    gzip -f errors_in_volt$SUFFIX.log
    gzip -f errors_in_volt_console$SUFFIX.out

    # Delete any class files added to the obj/ directory (and the directory, if empty)
    rm obj/sqlgrammartest/*.class
    rmdir obj/sqlgrammartest 2> /dev/null
    rmdir obj 2> /dev/null
}

# Run the SQL-grammar-generator tests, after first running everything you need
# to prepare for them; provides a "fresh start", i.e., re-builds the latest
# versions of VoltDB ('community'), the Jar files, etc.
function all() {
    echo -e "\n$0 performing: all$ARGS"
    prepare
    tests
    shutdown
}

# Run the SQL-grammar-generator tests, after first running everything you need
# to prepare for them; provides a "fresh start", i.e., re-builds the latest
# versions of VoltDB ('pro'), the Jar files, etc.
function all-pro() {
    echo -e "\n$0 performing: all-pro$ARGS"
    prepare-pro
    tests
    shutdown
}

# Run the SQL-grammar-generator tests' "help" option, which describes its own arguments
function tests-help() {
    find-directories-if-needed
    python $SQLGRAMMAR_DIR/sql_grammar_generator.py --help
    PRINT_ERROR_CODE=0
}

# Print a simple help message, describing the options for this script
function help() {
    echo -e "\nUsage: ./run.sh {build[-pro]|init|debug|jars|server[-pro]|ddl|prepare[-pro]|"
    echo -e "  tests-only|tests|shutdown|all[-pro]|tests-help|test-tools-help|help}\n"
    echo -e "This script is used to run the SQL-grammar-generator tests, and the various things that"
    echo -e "  go with them, e.g., building and running a VoltDB server, or shutting it down afterward."
    echo -e "Options:"
    echo -e "    build           : builds VoltDB ('community', open-source version)"
    echo -e "    build-pro       : builds VoltDB ('pro' version)"
    echo -e "    init            : sets useful variables such as CLASSPATH and DEFAULT_ARGS"
    echo -e "    debug           : prints the values of variables such as VOLTDB_COM_DIR and PATH"
    echo -e "    jars            : creates the (Java) .jar files needed by the tests"
    echo -e "    server          : starts a VoltDB server ('community', open-source version)"
    echo -e "    server-pro      : starts a VoltDB server ('pro' version)"
    echo -e "    ddl             : runs (in sqlcmd) the DDL (.sql) files needed by the tests"
    echo -e "    prepare         : runs (almost) all of the above, except the '-pro' options"
    echo -e "    prepare-pro     : runs (almost) all of the above, using the '-pro' options"
    echo -e "    tests-only      : runs only the tests, on the assumption that 'prepare' has been run"
    echo -e "    tests           : runs the tests, preceded by whatever other options are needed"
    echo -e "    shutdown        : stops a VoltDB server that is currently running"
    echo -e "    all             : runs 'prepare', 'tests', 'shutdown', effectively calling everything (non-pro)"
    echo -e "    all-pro         : runs 'prepare-pro', 'tests', 'shutdown', effectively calling everything (-pro)"
    echo -e "    tests-help      : prints a help message for the SQL-grammar-generator Python program"
    echo -e "    test-tools-help : prints a help message for the test-tools.sh script, which is used by this one"
    echo -e "    help            : prints this message"
    echo -e "The 'tests-only', 'tests', and 'all' options accept arguments: see the 'tests-help' option for details."
    echo -e "The 'build[-pro]', options accept VoltDB build arguments, e.g. '-Dbuild=debug'."
    echo -e "Some options (build[-pro], init, jars, server[-pro], ddl) may have '-if-needed' appended,"
    echo -e "  e.g., 'server-if-needed' will start a VoltDB server only if one is not already running."
    echo -e "Multiple options may be specified; but options usually call other options that are prerequisites.\n"
    PRINT_ERROR_CODE=0
}

# Check the exit code(s), and exit
function exit-with-code() {
    find-directories-if-needed
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: exit-with-code"
    fi
    cd $HOME_DIR

    errcode=0
    for i in {0..6}; do
        if [[ -z "${code[$i]}" ]]; then
            code[$i]=0
        fi
        errcode=$(($errcode|${code[$i]}))
    done
    if [[ "$errcode" -ne "0" ]]; then
        if [[ "${code[1]}" -ne "0" ]]; then
            echo -e "\ncode1a code1b: $code_voltdb_jar $code_python (classpath, python)"
        fi
        if [[ "${code[2]}" -ne "0" ]]; then
            echo -e "\ncode2a code2b code2c code2d: $code2a $code2b $code2c $code2d (javac, jar, UDF, mv)"
        fi
        if [[ "${code[3]}" -ne "0" ]]; then
            echo -e "\ncode3a code3b: $code_voltdb_init $code_voltdb_start (server-init, server-start)"
        fi
        if [[ "${code[4]}" -ne "0" ]]; then
            echo -e "\ncode4a code4b code4c code4d: $code4a $code4b $code4c $code4d (grammar-ddl, UDF-drop, UDF-load, UDF-ddl)"
        fi
        echo -e "\ncodes 0-6: ${code[*]} (build, init, jars, server, ddl, tests, shutdown)"
    fi
    if [[ $PRINT_ERROR_CODE -ne 0 ]]; then
        echo "error code:" $errcode
    fi
    exit $errcode
}

# If no options specified, run help
if [[ $# -eq 0 ]]; then
    help
fi

# Run the options passed on the command line
run-test-tools
SUFFIX=
BUILD_ARGS=
while [[ -n "$1" ]]; do
    CMD="$1"
    ARGS=
    if [[ "$1" == build* || "$1" == "tests" || "$1" == "tests-only" ||
          "$1" == "shutdown" || "$1" == all* ]]; then
        while [[ "$2" == -* ]]; do
            if [[ "$2" == --suffix=* ]]; then
                SUFFIX="${2/--suffix=/}"
            elif [[ "$2" == "-X" ]]; then
                SUFFIX="$3"
            fi
            if [[ "$2" == --* || "$2" == -D* ]]; then
                ARGS="$ARGS $2"
            else
                ARGS="$ARGS $2 $3"
                shift
            fi
            shift
        done
        if [[ "$CMD" == build* ]]; then
            BUILD_ARGS=$ARGS
        fi
    fi
    PRINT_ERROR_CODE=1
    $CMD
    COMMAND_CODE=$?
    if [[ $COMMAND_CODE -eq 127 ]]; then
        echo -e "Option '$CMD' returned exit code: $COMMAND_CODE. This is probably an"
        echo -e "    unknown option, or it might have been used incorrectly."
        echo -e "For more info about options, try: '$0 help'"
    fi
    shift
done
exit-with-code
