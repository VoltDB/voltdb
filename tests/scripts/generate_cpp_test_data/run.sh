#!/usr/bin/env bash

# Global state variables.
OBJ="./obj"
CLIENT_DIR="./cpp-test-files"
BUILD=debug
CLEANUP=YES
VOLTDB_VERSION=6.1
DEBUGGER_PORT=8000

function help() {
    echo "Usage: ./run.sh ... arguments ..."
    echo "Arguments:"
    echo "  --voltdb DIR         The VoltDB binary directory will be in $DIR."
    echo "                       The administrative commands, voltdb, voltadmin"
    echo "                       and so forth, should be in $DIR/bin."
    echo "                       If this is not set, we try \"which voltdb\""
    echo "                       and use the drectory which contains the command."
    echo "                       If we cannot find a voltdb command and this is"
    echo "                       not given it is an error."
    echo "  --license FILE       The license file is in the given file.  If this"
    echo "                       is not provided we look in \"$VOLTDB_BASE/voltdb\"."
    echo "                       If we can't find \"license.xml\" anywhere we"
    echo "                       complain and quit."
    echo "  --build BUILD        Look for generator class files in the"
    echo "                       build directory obj/BUILD.  This should"
    echo "                       be debug or release.  By default is it debug."
    echo "  --client-dir DIR     The C++ files should be installed"
    echo "                       in directory DIR.  If DIR is a file,"
    echo "                       it is an error.  If DIR does not exist"
    echo "                       at all it is created.  By default this"
    echo "                       will be ./obj."
    echo "  --working-dir DIR    Run the server in the given directory."
    echo "                       By default this will be ./obj.  After a"
    echo "                       successful generation run this will be"
    echo "                       deleted."
    echo "  --debug[=port]       Debug the java application which generates"
    echo "                       the data.  Use the given port, or 8000 if"
    echo "                       no port is given."
    echo "  --voltdb-version V   Set the voltdb version to the given one."
    echo "  --no-cleanup         Don't clean up the working directory."
    echo "  --help               Print this message and exit."
}

function calculate_volt_paths() {
    # Find the lib and bin directories.
    if [ -n "$VOLTDB_BASE" ] ; then
        VOLTDB_BIN="$VOLTDB_BASE/bin"
    elif [ -n "$(which voltdb 2> /dev/null)" ]; then
        # find voltdb binaries in either installation or distribution directory.
        VOLTDB_BIN="$(dirname "$(which voltdb)")"
        VOLTDB_BASE="$(dirname "$VOLTDB_BIN")"
    else
        echo "The VoltDB scripts are not in your PATH."
        echo "Please add them before running this script,"
        echo "Or execute the script from inside the source"
        echo "folder \"$VOLT_SRC/tests/scripts/generate_cpp_test_data\"."
        echo
        return 100
    fi
    # move voltdb commands into path for this script if necessary.
    if [ -z "$(which voltdb)" ] ; then
        PATH=$VOLTDB_BIN:$PATH
    fi  
    return 0
}

function make_fulders() {
    make_dir_if_necessary "$CLIENT_DIR"
    make_dir_if_necessary "$OBJ"
}

function initialize_script() {
    # Remember where we are now.
    START_DIR=$(/bin/pwd)

    calculate_volt_paths
    # Set up and validate some other detritus.
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    LOG4J="$VOLTDB_VOLTDB/log4j.xml"
    DEPLOYMENT_FILE="$START_DIR/deployment.xml"
    if [ -z "$LICENSE_FILE" ] ; then
        LICENSE_FILE="$VOLTDB_VOLTDB/license.xml"
    fi

    if [ ! -f "$LICENSE_FILE" ] ; then
        echo "$0: The license file \"$LICENSE\" does not seem to exist.  Please sort this out before we continue."
        return 100
    fi
    return 0
}

# Run the voltdb server locally
function server() {
    local DEPLOYMENT_FILE="$1"
    local LICENSE_FILE="$2"
    # run the server
    echo "Starting a voltdb server"
    echo "  with deployment file \"$DEPLOYMENT_FILE\""
    echo "  with license file \"$LICENSE_FILE\""
    /bin/rm -rf "$OBJ"
    mkdir -p "$OBJ"
    (cd "$OBJ"; voltdb init -C "$DEPLOYMENT_FILE" -l "$LICENSE_FILE"; voltdb start -H localhost &)
}

# Load the schema.  We don't have any stored procedures
# or jars, as they are all in the test directory.
function init_database() {
    local SQL_FILE="$1"
    local NTRIES=0
    local MAXTRIES=5
    local DONE
    while [ -z "$DONE" ] && [ $NTRIES -lt $MAXTRIES ] ; do
        NTRIES=$((NTRIES + 1))
        sqlcmd < "$SQL_FILE" && DONE=YES
        if [ -z "$DONE" ] ; then
            echo "Cannot connect to the server.  We'll try again soon."
            sleep 5
        fi
    done
    if [ -z "$DONE" ] ; then
        echo "Could not connect to the server in half a minute.  Something is wrong."
        return 100
    else
        return 0
    fi
}

function noserver() {
    echo "Shutting down the server."
    (cd "$OBJ"; voltadmin shutdown)
}

# run the client that drives the example
function client() {
    echo "Running the client.  Data files go in \"$CLIENT_DIR\""
    echo "  Classpath:"
    echo "    $VOLTDB_BASE/voltdb/voltdb-$VOLTDB_VERSION.jar"
    echo "    $VOLTDB_BASE/obj/${BUILD}/test"
    if [ -n "$DEBUG" ] ; then
      echo "   Debugging on port ${DEBUGGER_PORT}."
      DEBUG_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=${DEBUGGER_PORT}"
    fi
    (set -x ; java $DEBUG_OPTS -classpath $VOLTDB_BASE/voltdb/voltdb-$VOLTDB_VERSION.jar:$VOLTDB_BASE/obj/${BUILD}/test org.voltdb.GenerateCPPTestFiles --client-dir "$CLIENT_DIR" "$@")
}

function cleanup() {
    if [ "$CLEANUP" == YES ] ; then
        echo "Cleaning up."
        rm -rf "$OBJ"
        rm -rf "${START_DIR}/log"
    else
        echo "The server's data and log files are in \"$OBJ\" and perhaps \"${START_DIR}/log\"."
    fi
}

function make_dir_if_necessary() {
    local FILE="$1"
    if [ ! -f "$FILE" ] ; then
        echo "$0: File \"$FILE\" is a regular file.  It wants to be a directory."
        return 100
    fi
    mkdir -p "$FILE"
}


function run_one_test() {
    local DEPLOYMENT_FILE="$1"
    local SCHEMA_FILE="$2"
    echo "Shutting down any existing servers.  This may cause an error message."
    voltadmin shutdown
    server "$DEPLOYMENT_FILE" "$LICENSE_FILE" \
        && init_database "$SCHEMA_FILE" \
        && client \
        && noserver \
        && cleanup
}

while [ -z "$DONE" ]; do
    case "$1" in
        --#)
            shift
            set -x
            ;;
        --voltdb)
            shift
            VOLTDB_BASE="$1"
            if [ ! -d  "$VOLTDB_BASE" ] || [ ! -x "$VOLTDB_BASE/bin/voltdb" ] ; then
                echo "$0: The voltdb base directory \"$VOLTDB_BASE\" is not helpful."
                echo "    This must exist and contain commands \"$VOLTDB_BASE/bin/voltdb\" and"
                echo "    \"VOLTDB_BASE/bin/voltadmin\"".
                exit 100
            fi
            shift
            ;;
        --voltdb-version)
            shift
            VOLTDB_VERSION="$1"
            shift
            ;;
        --license)
            shift;
            LICENSE_FILE="$1"
            shift
            ;;
        --client-dir)
            shift
            CLIENT_DIR="$1"
            echo "CLIENT_DIR -> $CLIENT_DIR"
            shift
            ;;
        --build)
            shift
            BUILD="$1"
            shift
            case "$BUILD" in
                release|debug)
                    ;;
                *)
                    echo "$0: The --build argument must be \"debug\" or \"release\", not \"$BUILD\"."
                    exit 100
                    ;;
            esac
            ;;
        --working-dir)
            shift
            OBJ="$1"
            shift
            ;;
        --no-cleanup)
            shift
            CLEANUP=NO
            ;;
        --debug=*)
	    DEBUGGER_PORT="$(echo "$1" | sed 's;--debug=;;')"
            DEBUG=YES
	    shift
	    ;;
	--debug)
	    DEBUG=YES
	    shift
	    ;;
        --help)
            help
            exit 100
            ;;
        "")
            DONE=YES
            ;;
        *)
            echo "$0: Unknown command line argument: \"$1\""
            exit 100
            ;;
    esac
done

initialize_script || exit 100

run_one_test "$PWD/deployment.xml" "$PWD/generate.sql"

