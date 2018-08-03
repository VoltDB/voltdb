#!/bin/bash
# Script that contains functions that may be useful for a variety of different
# test frameworks in the directories below. Many of these were originally
# developed in connection with the SQL-grammar-generator tests, but they may
# be equally useful for other test frameworks, such as SqlCoverage, the (GEB)
# VMC tests, etc. Some examples: building VoltDB, starting a VoltDB server,
# and shutting down the VoltDB server at the end.

# Remember the directory where we started, and find the <voltdb>, <voltdb>/bin/,
# and <voltdb>/tests/ directories; and set variables accordingly
function test-tools-find-directories() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: test-tools-find-directories"
    fi
    TT_HOME_DIR=$(pwd)
    if [[ -e $TT_HOME_DIR/tests/test-tools.sh ]]; then
        # It looks like we're running from a <voltdb> directory
        VOLTDB_COM_DIR=$TT_HOME_DIR
    elif [[ $TT_HOME_DIR == */tests ]] && [[ -e $TT_HOME_DIR/test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/ directory
        VOLTDB_COM_DIR=$(cd $TT_HOME_DIR/..; pwd)
    elif [[ $TT_HOME_DIR == */tests/* ]] && [[ -e $TT_HOME_DIR/../test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/FOO/ directory,
        # e.g., from <voltdb>/tests/sqlgrammar/ or <voltdb>/tests/sqlcoverage/
        VOLTDB_COM_DIR=$(cd $TT_HOME_DIR/../..; pwd)
    elif [[ $TT_HOME_DIR == */tests/*/* ]] && [[ -e $TT_HOME_DIR/../../test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/FOO/BAR/ directory,
        # e.g., from <voltdb>/tests/geb/vmc/
        VOLTDB_COM_DIR=$(cd $TT_HOME_DIR/../../..; pwd)
    elif [[ $TT_HOME_DIR == */tests/*/*/* ]] && [[ -e $TT_HOME_DIR/../../../test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/FOO/BAR/BAZ/ directory,
        # e.g., from <voltdb>/tests/geb/vmc/src/
        VOLTDB_COM_DIR=$(cd $TT_HOME_DIR/../../../..; pwd)
    elif [[ -n "$(which voltdb 2> /dev/null)" ]]; then
        # It looks like we're using VoltDB from the PATH
        VOLTDB_BIN_DIR=$(dirname "$(which voltdb)")
        if [[ $VOLTDB_BIN_DIR == */pro/obj/pro/voltdb-ent-*/bin ]]; then
            # It looks like we found a VoltDB 'pro' directory
            VOLTDB_PRO_DIR=$(cd $VOLTDB_BIN_DIR/../../../..; pwd)
            VOLTDB_COM_DIR=$(cd $VOLTDB_PRO_DIR/../voltdb; pwd)
        else
            # It looks like we found a VoltDB 'community' directory
            VOLTDB_COM_DIR=$(cd $VOLTDB_BIN_DIR/..; pwd)
        fi
    else
        echo "Unable to find VoltDB installation."
        echo "Please add VoltDB's bin directory to your PATH."
        exit -1
    fi
    VOLTDB_COM_BIN=$VOLTDB_COM_DIR/bin
    VOLTDB_TESTS=$VOLTDB_COM_DIR/tests
    # These directories may or may not exist, so ignore any errors
    VOLTDB_PRO_DIR=$(cd $VOLTDB_COM_DIR/../pro 2> /dev/null; pwd)
    VOLTDB_PRO_BIN=$(cd $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*/bin 2> /dev/null; pwd)
    # Use 'community', open-source VoltDB by default (not 'pro')
    if [[ -z "$VOLTDB_BIN_DIR" ]]; then
        VOLTDB_BIN_DIR=$VOLTDB_COM_BIN
    fi
}

# Find the directories and set variables, only if not set already
function test-tools-find-directories-if-needed() {
    if [[ -z "$TT_HOME_DIR" || -z "$VOLTDB_COM_DIR" || -z "$VOLTDB_COM_BIN" || -z "$VOLTDB_TESTS" ]]; then
        test-tools-find-directories
    fi
}

# Build VoltDB: 'community', open-source version
# Optionally, you may specify BUILD_ARGS
function test-tools-build() {
    test-tools-find-directories-if-needed
    echo -e "\n$0 performing: [test-tools-]build $BUILD_ARGS"

    cd $VOLTDB_COM_DIR
    ant clean dist $BUILD_ARGS
    code_tt_build=$?
    cd -

    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "\ncode_tt_build: $code_tt_build"
    fi
}

# Build VoltDB: 'pro' version
# Optionally, you may specify BUILD_ARGS
function test-tools-build-pro() {
    test-tools-find-directories-if-needed
    echo -e "\n$0 performing: [test-tools-]build-pro $BUILD_ARGS"

    cd $VOLTDB_PRO_DIR
    ant -f mmt.xml dist.pro $BUILD_ARGS
    code_tt_build=$?
    cd -
    VOLTDB_PRO_BIN=$(cd $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*/bin; pwd)
    cp $VOLTDB_PRO_BIN/../voltdb/license.xml $VOLTDB_COM_DIR/voltdb/

    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "\ncode_tt_build(pro): $code_tt_build"
    fi
}

# Build VoltDB ('community'), only if not built already
# Optionally, you may specify BUILD_ARGS
function test-tools-build-if-needed() {
    test-tools-find-directories-if-needed
    VOLTDB_COM_JAR=$(ls $VOLTDB_COM_DIR/voltdb/voltdb-*.jar)
    if [[ ! -e $VOLTDB_COM_JAR ]]; then
        test-tools-build
    fi
}

# Build VoltDB 'pro' version, only if not built already
# Optionally, you may specify BUILD_ARGS
function test-tools-build-pro-if-needed() {
    test-tools-find-directories-if-needed
    VOLTDB_PRO_TAR=$(ls $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*.tar.gz)
    if [[ ! -e $VOLTDB_PRO_TAR ]]; then
        test-tools-build-pro
    fi
}

# Set CLASSPATH, PATH, and python, as needed
function test-tools-init() {
    test-tools-find-directories-if-needed
    test-tools-build-if-needed
    if [[ "$TT_DEBUG" -gt "0" ]]; then
        echo -e "\n$0 performing: test-tools-init"
    fi

    # Set CLASSPATH to include the VoltDB Jar file
    VOLTDB_COM_JAR=$(ls $VOLTDB_COM_DIR/voltdb/voltdb-*.jar)
    code_voltdb_jar=$?
    if [[ -z "$CLASSPATH" ]]; then
        CLASSPATH=$VOLTDB_COM_JAR
    else
        CLASSPATH=$VOLTDB_COM_JAR:$CLASSPATH
    fi

    # Set PATH to include the voltdb/bin directory, containing the voltdb and sqlcmd executables
    if [[ -z "$(which voltdb)" || -z "$(which sqlcmd)" ]]; then
        PATH=$VOLTDB_BIN_DIR:$PATH
    fi

    # Set python to use version 2.7
    alias python=python2.7
    code_python=$?

    code_tt_init=$(($code_voltdb_jar|$code_python))
    if [[ "$code_tt_init" -ne "0" ]]; then
        echo -e "\ncode_voltdb_jar: $code_voltdb_jar"
        echo -e "code_python    : $code_python"
        echo -e "code_tt_init   : $code_tt_init"
    fi
}

# Set CLASSPATH, PATH, and python, only if not set already
function test-tools-init-if-needed() {
    if [[ -z "${code_tt_init}" ]]; then
        test-tools-init
    fi
}

# Print the values of various variables, mainly those set in the
# test-tools-find-directories() and test-tools-init() functions
function test-tools-debug() {
    test-tools-init-if-needed
    if [[ "$TT_DEBUG" -gt "0" ]]; then
        echo -e "\n$0 performing: test-tools-debug"
        echo "TT_DEBUG       :" $TT_DEBUG
    fi

    echo "TT_HOME_DIR    :" $TT_HOME_DIR
    echo "VOLTDB_COM_DIR :" $VOLTDB_COM_DIR
    echo "VOLTDB_COM_BIN :" $VOLTDB_COM_BIN
    echo "VOLTDB_COM_JAR :" $VOLTDB_COM_JAR
    echo "VOLTDB_PRO_DIR :" $VOLTDB_PRO_DIR
    echo "VOLTDB_PRO_BIN :" $VOLTDB_PRO_BIN
    echo "VOLTDB_PRO_TAR :" $VOLTDB_PRO_TAR
    echo "VOLTDB_BIN_DIR :" $VOLTDB_BIN_DIR
    echo "DEPLOYMENT_FILE:" $DEPLOYMENT_FILE
    echo "DEPLOYMENT_ARG :" $DEPLOYMENT_ARG
    echo "CLASSPATH      :" $CLASSPATH
    echo "PATH           :" $PATH
    echo "which sqlcmd   :" `which sqlcmd`
    echo "which voltdb   :" `which voltdb`
    echo "voltdb version :" `$VOLTDB_BIN_DIR/voltdb --version`
    echo "which python   :" `which python`
    echo "python version :"
    python --version
}

# Wait for a VoltDB server to finish initializing; should not be called directly
function test-tools-wait-for-server-to-start() {
    test-tools-find-directories-if-needed
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: test-tools-wait-for-server-to-start"
    fi

    SQLCMD_COMMAND="$VOLTDB_COM_BIN/sqlcmd --query='select C1 from NONEXISTENT_TABLE' 2>&1"
    if [[ "$TT_DEBUG" -ge "3" ]]; then
        echo -e "DEBUG: sqlcmd command:\n$SQLCMD_COMMAND"
    fi

    MAX_SECONDS=15
    for (( i=1; i<=${MAX_SECONDS}; i++ )); do
        SQLCMD_RESPONSE=$(eval $SQLCMD_COMMAND)
        if [[ "$TT_DEBUG" -ge "4" ]]; then
            echo -e "\nDEBUG: sqlcmd response $i:\n$SQLCMD_RESPONSE"
        fi

        # If the VoltDB server is now running, we're done
        if [[ "$SQLCMD_RESPONSE" == *"object not found: NONEXISTENT_TABLE"* ]]; then
            echo "VoltDB server is running..."
            break

        # If the VoltDB server has not yet completed initialization, keep waiting
        elif [[ "$SQLCMD_RESPONSE" == *"Unable to connect"* || "$SQLCMD_RESPONSE" == *"Connection refused"* ]]; then
            sleep 1

        # Otherwise, print an error message and exit
        else
            echo -e "\nVoltDB server unable to start: sqlcmd response had error(s):\n$SQLCMD_RESPONSE\n"
            exit -2
        fi
    done

    if [[ "$i" -gt "$MAX_SECONDS" ]]; then
        echo -e "\n\nERROR: VoltDB server unable to start after waiting $MAX_SECONDS seconds"
        echo -e "Here is the end of the VoltDB console output (./volt_console.out):"
        tail -10 volt_console.out
        echo -e "\nHere is the end of the VoltDB log (./voltdbroot/log/volt.log):"
        tail -10 voltdbroot/log/volt.log
        exit -3
    fi
}

# Start a VoltDB server: 'community' or 'pro', depending on the value of the
# VOLTDB_BIN_DIR variable; optionally, you may set the DEPLOYMENT_FILE or
# DEPLOYMENT_ARG variable (the latter should start with '-C ' or '--config=')
# before calling this function; should not be called directly
function test-tools-server-community-or-pro() {
    test-tools-find-directories-if-needed
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: test-tools-server-community-or-pro"
    fi

    if [[ -z "$DEPLOYMENT_ARG" && -n "$DEPLOYMENT_FILE" ]]; then
        DEPLOYMENT_ARG="--config=$DEPLOYMENT_FILE"
    fi
    INIT_COMMAND="${VOLTDB_BIN_DIR}/voltdb init --force ${DEPLOYMENT_ARG}"
    echo -e "Running:\n${INIT_COMMAND}"
    ${INIT_COMMAND}
    code_voltdb_init=$?

    START_COMMAND="${VOLTDB_BIN_DIR}/voltdb start"
    echo -e "Running:\n${START_COMMAND} > volt_console.out 2>&1 &"
    ${START_COMMAND} > volt_console.out 2>&1 &
    code_voltdb_start=$?
    test-tools-wait-for-server-to-start

    # Prevent exit before stopping the VoltDB server, if your tests fail
    set +e

    code_tt_server=$(($code_voltdb_init|$code_voltdb_start))
    if [[ "$code_tt_server" -ne "0" ]]; then
        echo -e "\ncode_voltdb_init : $code_voltdb_init"
        echo -e "code_voltdb_start: $code_voltdb_start"
        echo -e "code_tt_server   : $code_tt_server"
    fi
}

# Start the VoltDB server: 'community', open-source version
function test-tools-server() {
    test-tools-find-directories-if-needed
    test-tools-build-if-needed
    echo -e "\n$0 performing: [test-tools-]server"

    VOLTDB_BIN_DIR=${VOLTDB_COM_BIN}
    test-tools-server-community-or-pro
}

# Start the VoltDB server: 'pro' version
function test-tools-server-pro() {
    test-tools-find-directories-if-needed
    test-tools-build-pro-if-needed
    echo -e "\n$0 performing: [test-tools-]server-pro"

    VOLTDB_BIN_DIR=${VOLTDB_PRO_BIN}
    test-tools-server-community-or-pro
}

# Start the VoltDB server ('community'), only if not already running
function test-tools-server-if-needed() {
    if [[ -z "$(ps -ef | grep -i voltdb | grep -v SQLCommand | grep -v 'grep -i voltdb')" ]]; then
        test-tools-server
    else
        echo -e "\nNot (re-)starting a VoltDB server, because 'ps -ef' now includes a 'voltdb' process."
        #echo -e "    DEBUG:" $(ps -ef | grep -i voltdb | grep -v SQLCommand | grep -v "grep -i voltdb")
    fi
}

# Start the VoltDB 'pro' server, only if not already running
function test-tools-server-pro-if-needed() {
    if [[ -z "$(ps -ef | grep -i voltdb | grep -v SQLCommand | grep -v 'grep -i voltdb')" ]]; then
        test-tools-server-pro
    else
        echo -e "\nNot (re-)starting a VoltDB pro server, because 'ps -ef' now includes a 'voltdb' process."
        #echo -e "    DEBUG:" $(ps -ef | grep -i voltdb | grep -v SQLCommand | grep -v "grep -i voltdb")
    fi
}

# Stop the VoltDB server, and kill any straggler processes
function test-tools-shutdown() {
    test-tools-find-directories-if-needed
    if [[ "$TT_DEBUG" -gt "0" ]]; then
        echo -e "\n$0 performing: test-tools-shutdown"
    fi

    # Stop the VoltDB server (& kill any stragglers)
    $VOLTDB_BIN_DIR/voltadmin shutdown
    code_tt_shutdown=$?
    cd $VOLTDB_COM_DIR
    ant killstragglers
    cd -

    if [[ "$code_tt_shutdown" -ne "0" ]]; then
        echo -e "\ncode_tt_shutdown: $code_tt_shutdown"
    fi
}

# Builds the latest (local) version of VoltDB (the 'community', open-source
# version), and starts a ('community') VoltDB server, after setting and echoing
# various environment variables
function test-tools-all() {
    echo -e "\n$0 performing: test-tools-all"

    test-tools-build
    test-tools-init
    test-tools-debug
    test-tools-server
}

# Builds the latest (local) version of VoltDB (the 'pro' version), and starts a
# ('pro') VoltDB server, after setting and echoing various environment variables
function test-tools-all-pro() {
    echo -e "\n$0 performing: test-tools-all-pro"

    test-tools-build-pro
    test-tools-init
    test-tools-debug
    test-tools-server-pro
}

# Print a simple help message, describing the options for this script
function test-tools-help() {
    echo -e "\nUsage: ./test-tools.sh test-tools-{build|build-pro|init|debug|server|all|shutdown|all|help}\n"
    echo -e "This script is mainly intended to provide useful functions that may be called"
    echo -e "  by a variety of other test scripts, e.g., <voltdb>/tests/sqlgrammar/run.sh,"
    echo -e "  but it may also be called directly on the command line."
    echo -e "Options:"
    echo -e "    test-tools-build      : builds VoltDB ('community', open-source version)"
    echo -e "    test-tools-build-pro  : builds VoltDB ('pro' version)"
    echo -e "    test-tools-init       : sets useful variables such as CLASSPATH and PATH"
    echo -e "    test-tools-debug      : prints the values of variables such as VOLTDB_COM_DIR and PATH"
    echo -e "    test-tools-server     : starts a VoltDB server ('community', open-source version)"
    echo -e "    test-tools-server-pro : starts a VoltDB server ('pro' version)"
    echo -e "    test-tools-all        : runs (almost) all of the above, except the '-pro' options"
    echo -e "    test-tools-all-pro    : runs (almost) all of the above, using the '-pro' options"
    echo -e "    test-tools-shutdown   : stops a VoltDB server that is currently running"
    echo -e "    test-tools-help       : prints this message"
    echo -e "Some options (test-tools-build[-pro], test-tools-init, test-tools-server[-pro])"
    echo -e "  may have '-if-needed' appended, e.g., 'test-tools-server-if-needed'"
    echo -e "  will start a VoltDB server only if one is not already running."
    echo -e "Multiple options may be specified; but options usually call other options that are prerequisites.\n"
    PRINT_ERROR_CODE=0
}

# If run on the command line with no options specified, run test-tools-help
if [[ $# -eq 0 && $0 == */test-tools.sh ]]; then
    test-tools-help
fi

# Run options passed on the command line, if any
while [[ -n "$1" ]]; do
    CMD="$1"
    $CMD
    shift
done
