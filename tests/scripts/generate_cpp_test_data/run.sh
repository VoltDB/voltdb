#!/usr/bin/env bash

# Global state variables.
OBJ="./obj"
CLIENTDIR="./cpp-test-files"
BUILD=debug
CLEANUP=YES

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
    echo "  --no-cleanup         Don't clean up the working directory."
    echo "  --help               Print this message and exit."
}


function readargs() {
    while [ -z "$DONE" ]; do
        case "$1" in
            --#)
                shift
                set -x
                ;;
            --authenticate)
                shift
                AUTHENTICATION=YES
                ;;
            --interaction)
                shift
                INTERACTION=YES
                ;;
            --all)
                shift
                ALLOPS=YES
                ;;
            --voltdb)
                shift
                VOLTDB_BASE="$1"
                if [ ! -d  "$VOLTDB_BASE" ] || [ ! -x "$VOLTDB_BASE/bin/voltdb" ] ; then
                    echo "$0: The voltdb base directory \"$VOLTDB_BASE\" is not helpful."
                    echo "    This must exist and contain commands \"$VOLTDB_BASE/bin/voltdb\" and"
                    echo "    \"VOLTDB_BASE/bin/voltadmin\"".
                    shift
                    ;;
                    --license)
                        shift;
                        LICENSE="$1"
                        shift
                        ;;
                    --client-dir)
                        shift
                        CLIENT_DIR="$1"
                        shift
                        ;;
                    --build)
                        shift
                        BUILD="$1"
                        case "$BUILD" in
                            release|debug)
                            ;;
                            *)
                                echo "$0: The --build argument must be \"debug\" or \"release\", not \"$BUILD\"."
                                return 100
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
                    --help)
                        help
                        return 100
                        ;;
                    "")
                        DONE=YES
                        ;;
                    *)
                        echo "$0: Unknown command line argument: \"$1\""
                        return 100
                        ;;
        esac
    done
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

function initialize_script() {
    # Remember where we are now.
    START_DIR=$(/bin/pwd)

    # Set up and validate some other detritus.
    VOLTDB_VOLTDB="$VOLTDB_BASE/voltdb"
    VOLTDB_LIB="$VOLTDB_BASE/lib"
    LOG4J="$VOLTDB_VOLTDB/log4j.xml"
    DEPLOYMENT_FILE="$START_DIR/deployment.xml"
    if [ -z "$LICENSE" ] ; then
        LICENSE="$VOLTDB_VOLTDB/license.xml"
    fi

    if [ ! -f "$LICENSE" ] ; then
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
    /bin/rm -rf "$OBJ"
    (cd "$OBJ"; voltdb create -d $DEPLOYMENT_FILE -l $LICENSE_FILE -H localhost &)
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
        sqlcmd < helloworld.sql && DONE=YES
    done
}

function noserver() {
    voltadmin shutdown
}

# run the client that drives the example
function client() {
    java -classpath $VOLTDB_BASE/voltdb/voltdb/voltdb-$VOLTDB_VERSION.jar:$VOLTDB_BASE/voltdb/obj/${BUILD}/test org.voltdb.GenerateCPPTestFiles --clientdir "$CLIENT_DIR" "$@"
}

function cleanup() {
    if [ "$CLEANUP" == YES ] ; then
        rm -rf "$OBJ"
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


function make_fulders() {
    make_dir_if_necessary "$CLIENT_DIR"
    make_dir_if_necessary "$OBJ"
}


function generate_authentication() {
    run_one_test deployment_auth.xml generate_auth.sql
}

function generate_interactions() {
    run_one_test deployment_noauth.xml generate_noauth.sql
}

function run_one_test() {
    local DEPLOYMENT_FILE = "R1"
    local SCHEMA_FILE="$2"
    server "$DEPLOYMENT_FILE" "$LICENSE_FILE" \
        && init_database $SCHEMA_FILE \
        && client --clientdir "$CLIENTDIR" --authentication \
        && noserver
        && cleanup
}

readargs || exit 100
initialize_script || exit 100



if [ -n "$ALLOPS" ] || [ -n "$AUTHENTICATION" ] ; then
    generate_authentication
fi

if [ -n "$ALLOPS" ] || [ -n "$INTERACTION" ] ; then
    generate_interactions
fi
