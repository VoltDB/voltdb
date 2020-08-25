#!/bin/bash
# Script that contains functions that may be useful for a variety of different
# test frameworks in the directories below. Many of these were originally
# developed in connection with the SQL-grammar-generator tests, but they may
# be equally useful for other test frameworks, such as SqlCoverage, the (GEB)
# VMC tests, etc. Some examples: building VoltDB, starting a VoltDB server,
# and shutting down the VoltDB server at the end.

# Remember the directory where we started, and find the <voltdb>, <voltdb>/bin/,
# and <voltdb>/tests/ directories; and set variables accordingly
function tt-find-directories() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    TT_HOME_DIR=$(pwd)
    if [[ -e $TT_HOME_DIR/tests/test-tools.sh ]]; then
        # It looks like we're running from a <voltdb> directory
        VOLTDB_COM_DIR=$TT_HOME_DIR
    elif [[ -e $TT_HOME_DIR/voltdb/tests/test-tools.sh ]]; then
        # It looks like we're running from just 'above' a <voltdb> directory
        VOLTDB_COM_DIR=$TT_HOME_DIR/voltdb
    elif [[ -e $TT_HOME_DIR/internal/tests/test-tools.sh ]]; then
        # It looks like we're running from just 'above' an <internal> directory
        VOLTDB_COM_DIR=$TT_HOME_DIR/internal
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
        tt-exit-script 11 $FUNCNAME "Unable to find VoltDB installation" \
                       "Please add VoltDB's bin directory to your PATH."
        return
    fi
    VOLTDB_COM_BIN=$VOLTDB_COM_DIR/bin
    VOLTDB_TESTS=$VOLTDB_COM_DIR/tests
    # These directories may or may not exist, so ignore any errors
    if [[ -z "$VOLTDB_PRO_DIR" ]]; then
        VOLTDB_PRO_DIR=$(cd $VOLTDB_COM_DIR/../pro 2> /dev/null; pwd)
    fi
    VOLTDB_PRO_BIN=$(cd $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*/bin 2> /dev/null; pwd)
    # Use 'community', open-source VoltDB by default (not 'pro')
    if [[ -z "$VOLTDB_BIN_DIR" ]]; then
        VOLTDB_BIN_DIR=$VOLTDB_COM_BIN
    fi
}

# Find the directories and set variables, only if not set already
function tt-find-directories-if-needed() {
    if [[ "$TT_DEBUG" -ge "4" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    if [[ -z "$TT_HOME_DIR" || -z "$VOLTDB_COM_DIR" || -z "$VOLTDB_COM_BIN" || -z "$VOLTDB_TESTS" ]]; then
        tt-find-directories
    fi
}

# Echo environment variables that are commonly used in Jenkins jobs, either as
# arguments to a Jenkins build, or within a Jenkins build, as arguments to a
# VoltDB build or to test code, such as JUnit tests
function tt-echo-build-args() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME"
    echo "VOLTDB_REV: $VOLTDB_REV"
    echo "PRO_REV   : $PRO_REV"
    echo "GIT_BRANCH: $GIT_BRANCH"
    echo "VERIFY_ARG: $VERIFY_ARG"
    echo "BUILD_OPTS: $BUILD_OPTS"
    echo "BUILD_ARGS: $BUILD_ARGS"
    echo "VOLTBUILD_ARG: $VOLTBUILD_ARG"
    echo "TEST_OPTS : $TEST_OPTS"
    echo "TEST_ARGS : $TEST_ARGS"
    echo "SEED      : $SEED"
    echo "SETSEED   : $SETSEED"
    BUILD_ARGS_WERE_ECHOED="true"
}

# Echo environment variables that are commonly used in Jenkins jobs,
# only if not echoed already
function tt-echo-build-args-if-needed() {
    if [[ "$TT_DEBUG" -ge "4" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    if [[ -z "$BUILD_ARGS_WERE_ECHOED" ]]; then
        tt-echo-build-args
    fi
}

# Set build arguments to be passed to a build of VoltDB (community or pro),
# based on simpler arguments passed to this function, such as 'debug', 'pool',
# 'memcheck', etc.
function tt-set-build-args() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME $@"
    tt-echo-build-args-if-needed

    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without 'source' or '.':"
        echo -e "    will have no external effect!\n"
    fi

    if [[ -z "$1" || "$1" == tt-* || "$1" == test-tools* ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without an argument:"
        echo -e "    will have little or no effect!\n"
    else
        # If at least one arg was passed, reset BUILD_ARGS to start over from
        # scratch, since it is set cumulatively, possibly containing many values
        BUILD_ARGS=
    fi

    # If VOLTBUILD_ARG is unset, give it a default value
    if [[ -z "$VOLTBUILD_ARG" ]]; then
        VOLTBUILD_ARG="release"
    fi

    while [[ -n "$1" ]]; do
        # Stop if we encounter other "test-tools" functions as args
        if [[ "$1" == tt-* || "$1" == test-tools* ]]; then
            break
        fi

        IFS=',' read -r -a build_options <<< "$1"
        for option in "${build_options[@]}"; do
            # Check for standard build options
            if [[ "$option" == "release" || "$option" == "reset" ]]; then
                VOLTBUILD_ARG="release"
                BUILD_ARGS=
            elif [[ "$option" == "debug" ]]; then
                VOLTBUILD_ARG="debug"
                BUILD_ARGS="$BUILD_ARGS -Dbuild=debug"
            elif [[ "$option" == "pool" ]]; then
                VOLTBUILD_ARG="debug"
                BUILD_ARGS="$BUILD_ARGS -Dbuild=debug -DVOLT_POOL_CHECKING=true"
            elif [[ "$option" == "memcheck" ]]; then
                VOLTBUILD_ARG="memcheck"
                BUILD_ARGS="$BUILD_ARGS -Dbuild=memcheck"
            elif [[ "$option" == "memcheckrelease" || "$option" == "memcheck-release" || \
                    "$option" == "memcheck_release" ]]; then
                VOLTBUILD_ARG="memcheck_release"
                BUILD_ARGS="$BUILD_ARGS -Dbuild=memcheck_release"
            elif [[ "$option" == "jmemcheck" ]]; then
                VOLTBUILD_ARG="jmemcheck"
                BUILD_ARGS="$BUILD_ARGS -Djmemcheck=MEMCHECK_FULL"
            elif [[ "$option" == "nojmemcheck" ]]; then
                VOLTBUILD_ARG="nojmemcheck"
                BUILD_ARGS="$BUILD_ARGS -Djmemcheck=NO_MEMCHECK"
            # User is allowed to specify their own -D... option(s)
            elif [[ "$option" == -D* ]]; then
                BUILD_ARGS="$BUILD_ARGS $option"
            else
                tt-exit-script 14 $FUNCNAME "Unrecognized arg / build option" "$1 / $option"
                return
            fi
        done
        shift
        SHIFT_BY=$((SHIFT_BY+1))
    done
    echo -e "VOLTBUILD_ARG: $VOLTBUILD_ARG"
    echo -e "BUILD_ARGS: $BUILD_ARGS"
}

# Set test arguments to be passed to tests of VoltDB (community or pro), e.g.,
# JUnit tests, based on simpler arguments passed to this function, such as
# 'flaky', 'nonflaky', 'flakydebug', etc.
function tt-set-test-args() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME $@"
    tt-echo-build-args-if-needed

    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without 'source' or '.':"
        echo -e "    will have no external effect!\n"
    fi

    if [[ -z "$1" || "$1" == tt-* || "$1" == test-tools* ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without an argument:"
        echo -e "    will have no effect!\n"
    fi

    TEST_ARGS=
    while [[ -n "$1" ]]; do
        # Stop if we encounter other "test-tools" functions as args
        if [[ "$1" == tt-* || "$1" == test-tools* ]]; then
            break
        fi

        IFS=',' read -r -a test_options <<< "$1"
        for option in "${test_options[@]}"; do
            # Check for standard test options
            if [[ "$option" == "reset" ]]; then
                TEST_ARGS=
            elif [[ "$option" == "flakytrue" || "$option" == "flaky" ]]; then
                TEST_ARGS="$TEST_ARGS -Drun.flaky.tests=true"
            elif [[ "$option" == "flakyfalse" || "$option" == "nonflaky" ]]; then
                TEST_ARGS="$TEST_ARGS -Drun.flaky.tests=false"
            elif [[ "$option" == "flakynone" ]]; then
                TEST_ARGS="$TEST_ARGS -Drun.flaky.tests=none"
            elif [[ "$option" == "flakydebug" ]]; then
                TEST_ARGS="$TEST_ARGS -Drun.flaky.tests.debug=true"
            # User is allowed to specify their own -D... option(s)
            elif [[ "$option" == -D* ]]; then
                TEST_ARGS="$TEST_ARGS $option"
            else
                tt-exit-script 15 $FUNCNAME "Unrecognized arg / test option" "$1 / $option"
                return
            fi
        done
        shift
        SHIFT_BY=$((SHIFT_BY+1))
    done
    echo -e "TEST_ARGS: $TEST_ARGS"
}

# Set the VERIFY_ARG environment variable, to be used for one of the arguments
# to be passed to JUnit tests of VoltDB (community or pro); typically, the
# GIT_BRANCH environment variable is passed as the (one and only) argument to
# this function, which will then set VERIFY_ARG to 'false' only if the GIT_BRANCH
# is 'master'; but if you wish to explicitly pass 'false' to set VERIFY_ARG to
# 'false' and avoid verifying DDL regardless of the current branch, you may do
# so. Any argument value other than 'master' or 'false' will leave VERIFY_ARG
# unset, so the default behavior of verifying DDL will occur.
function tt-set-verify-arg() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME $1"
    tt-echo-build-args-if-needed

    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without 'source' or '.':"
        echo -e "    will have no external effect!\n"
    fi

    # On master, don't verify DDL; on all other branches always verify
    # (which is the default), unless explicitly passed 'false'
    if [[ "$1" = "master" || "$1" == "false" ]]; then
        VERIFY_ARG="false";
    fi

    if [[ -z "$1" || "$1" == tt-* || "$1" == test-tools* ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without an argument."
        echo -e "    will have no external effect!\n"
    else
        SHIFT_BY=$((SHIFT_BY+1))
    fi

    echo -e "VERIFY_ARG: $VERIFY_ARG"
}

# Sets the SETSEED variable, typically to be used by SqlCoverage, based on
# an argument passed to this function, or on the current value (if any) of
# the SEED variable
function tt-set-setseed() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME $1"
    tt-echo-build-args-if-needed

    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without 'source' or '.':"
        echo -e "    will have no external effect!\n"
    fi

    SETSEED=
    if [[ -n "$1" && "$1" != tt-* && "$1" != test-tools* ]]; then
        SETSEED="-Dsql_coverage_seed=$1"
        SHIFT_BY=$((SHIFT_BY+1))
    elif [[ -n "$SEED" ]]; then
        SETSEED="-Dsql_coverage_seed=$SEED"
    fi
    echo -e "SETSEED: $SETSEED"
}

# Call the 'killstragglers' script, passing it the standard PostgreSQL port number
function tt-killstragglers() {
    echo -e "\n${BASH_SOURCE[0]} performing: $FUNCNAME"
    tt-find-directories-if-needed

    cd $VOLTDB_COM_DIR
    ant killstragglers -Dport=5432
    cd -
}

# Call the start_postgresql.sh script
function tt-start-postgresql() {
    echo -e "\n${BASH_SOURCE[0]} performing: tt-start-postgres[ql]"
    tt-find-directories-if-needed

    # SqlCoverage will only work well with PostgreSQL, if PostgreSQL is
    # started by user 'test'
    if [[ `whoami` != "test" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called as user '`whoami`', not 'test':"
        echo -e "    SqlCoverage will not work well!!!\n"
    fi

    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME called without 'source' or '.':"
        echo -e "    stop-postgresql will be unable to access the environment variables set here!\n"
    fi

    # Start the PostgreSQL server, in a new temp directory ('source' is used so that
    # the environment variables defined here can be used by stop_postgresql.sh later)
    source $VOLTDB_TESTS/sqlcoverage/start_postgresql.sh

    # TODO: does this have any effect, inside this script, or only in Jenkins??
    # Prevent exit before stopping the PostgreSQL server & deleting the temp directory,
    # if the sqlCoverage tests fail
    set +e
}

# Equivalent, shorter function name
function tt-start-postgres() {
    tt-start-postgresql $@
}

# Call the stop_postgresql.sh script
function tt-stop-postgresql() {
    echo -e "\n${BASH_SOURCE[0]} performing: tt-stop-postgres[ql]"
    tt-find-directories-if-needed

    # Stop the PostgreSQL server & delete the temp directory
    $VOLTDB_TESTS/sqlcoverage/stop_postgresql.sh
}

# Equivalent, shorter function name
function tt-stop-postgres() {
    tt-stop-postgresql $@
}

# TODO: this could be implemented in the future, to run the SqlCoverage test
# program, with various arguments
function tt-sqlcoverage() {
    echo -e "\n${BASH_SOURCE[0]} performing: tt-run-sqlcoverage $@"

    echo -e "\nWARNING: ${BASH_SOURCE[0]} function $FUNCNAME not implemented yet!!!\n"
}

# Sets the TT_BUILD_ARGS environment variable, based on the args passed to it;
# translates certain brief arg abbreviations into their full meanings, leaves
# the rest unchanged
function tt-set-tt-build-args() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    TT_BUILD_ARGS=()
    for arg in $@; do
        if [[ "$arg" == "--debug" ]]; then
            TT_BUILD_ARGS+=("-Dbuild=debug")
        elif [[ "$arg" == "--pool" ]]; then
            TT_BUILD_ARGS+=("-Dbuild=debug" "-DVOLT_POOL_CHECKING=true")
        elif [[ "$arg" == "--release" ]]; then
            TT_BUILD_ARGS+=("-Dbuild=release")
        else
            if [[ "$arg" == --* ]]; then
                echo -e "\nWARNING: in $FUNCNAME, unrecognized build arg $arg will be passed on, but effect is unknown"
            fi
            TT_BUILD_ARGS+=("$arg")
        fi
    done
}

# Build VoltDB: 'community', open-source version
# Optionally, you may specify one or more build arguments ($@)
function tt-build() {
    tt-find-directories-if-needed
    tt-set-tt-build-args "$@"
    echo -e "\n$0 performing: [tt-]build ${TT_BUILD_ARGS[@]}"
    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "ERROR: $FUNCNAME skipped because of a previous failure."
        return $code_tt_build
    fi

    cd $VOLTDB_COM_DIR
    ant clean dist "${TT_BUILD_ARGS[@]}"
    code_tt_build=$?
    cd -
    VOLTDB_BIN_DIR=${VOLTDB_COM_BIN}

    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "\ncode_tt_build: $code_tt_build"
    fi
}

# Build VoltDB: 'pro' version
# Optionally, you may specify one or more build arguments ($@)
function tt-build-pro() {
    tt-find-directories-if-needed
    tt-set-tt-build-args "$@"
    echo -e "\n$0 performing: [tt-]build-pro ${TT_BUILD_ARGS[@]}"
    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "ERROR: $FUNCNAME skipped because of a previous failure."
        return $code_tt_build
    fi

    cd $VOLTDB_PRO_DIR
    ant -f mmt.xml clean dist.pro "${TT_BUILD_ARGS[@]}"
    code_tt_build=$?
    cd -
    VOLTDB_PRO_BIN=$(cd $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*/bin; pwd)
    cp $VOLTDB_PRO_BIN/../voltdb/license.xml $VOLTDB_COM_DIR/voltdb/
    VOLTDB_BIN_DIR=${VOLTDB_PRO_BIN}

    if [[ "$code_tt_build" -ne "0" ]]; then
        echo -e "\ncode_tt_build(pro): $code_tt_build"
    fi
}

# Build VoltDB ('community'), only if not built already
# Optionally, you may specify one or more build arguments ($@)
function tt-build-if-needed() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed
    VOLTDB_COM_JAR=$(ls $VOLTDB_COM_DIR/voltdb/voltdb-*.jar)
    if [[ ! -e $VOLTDB_COM_JAR ]]; then
        tt-build "$@"
    fi
}

# Build VoltDB 'pro' version, only if not built already
# Optionally, you may specify one or more build arguments ($@)
function tt-build-pro-if-needed() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed
    VOLTDB_PRO_TAR=$(ls $VOLTDB_PRO_DIR/obj/pro/voltdb-ent-*.tar.gz)
    if [[ ! -e $VOLTDB_PRO_TAR ]]; then
        tt-build-pro "$@"
    fi
}

# Set CLASSPATH, PATH, and python, as needed
function tt-init() {
    if [[ "$TT_DEBUG" -ge "1" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed
    tt-build-if-needed "$@"

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
function tt-init-if-needed() {
    if [[ "$TT_DEBUG" -ge "4" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    if [[ -z "${code_tt_init}" ]]; then
        tt-init "$@"
    fi
}

# Print the values of various variables, mainly those set in the
# tt-find-directories() and tt-init() functions
function tt-print-vars() {
    if [[ "$TT_DEBUG" -ge "1" ]]; then
        echo -e "\n$0 performing: $FUNCNAME"
        echo "TT_DEBUG       :" $TT_DEBUG
    fi
    tt-init-if-needed "$@"

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
    echo "python version :" `python --version 2>&1`
}

# Wait for a VoltDB server to finish initializing; should not be called directly
function tt-wait-for-server-to-start() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed

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
            tt-exit-script 12 $FUNCNAME "VoltDB server unable to start" \
                           "sqlcmd response had error(s):\n    $SQLCMD_RESPONSE"
            return
        fi
    done

    if [[ "$i" -gt "$MAX_SECONDS" ]]; then
        EXTENDED_ERROR_MSG="Here is the end of the VoltDB console output (./volt_console.out):\n"
        EXTENDED_ERROR_MSG+=`tail -10 volt_console.out`
        EXTENDED_ERROR_MSG+="\n\nHere is the end of the VoltDB log (./voltdbroot/log/volt.log):\n"
        EXTENDED_ERROR_MSG+=`tail -10 voltdbroot/log/volt.log`
        tt-exit-script 13 $FUNCNAME "VoltDB server unable to start after waiting $MAX_SECONDS seconds" "$EXTENDED_ERROR_MSG"
        return
    fi
}

# Start a VoltDB server: 'community' or 'pro', depending on the value of the
# VOLTDB_BIN_DIR variable; optionally, you may set the DEPLOYMENT_FILE or
# DEPLOYMENT_ARG variable (the latter should start with '-C ' or '--config=')
# before calling this function; should not be called directly
function tt-server-community-or-pro() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed
    if [[ "$code_tt_server" -ne "0" ]]; then
        echo -e "ERROR: $FUNCNAME skipped because of a previous failure."
        return $code_tt_server
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
    tt-wait-for-server-to-start

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
function tt-server() {
    echo -e "\n$0 performing: [tt-]server"
    tt-find-directories-if-needed
    tt-build-if-needed

    VOLTDB_BIN_DIR=${VOLTDB_COM_BIN}
    tt-server-community-or-pro
}

# Start the VoltDB server: 'pro' version
function tt-server-pro() {
    echo -e "\n$0 performing: [tt-]server-pro"
    tt-find-directories-if-needed
    tt-build-pro-if-needed

    VOLTDB_BIN_DIR=${VOLTDB_PRO_BIN}
    tt-server-community-or-pro
}

# Counts the number of VoltDB processes, e.g., processes that contain
# 'voldb/voltdb-8.4.4.jar' or 'voldb/voltdb-9.1.jar' or 'voldb/voltdb-10.0.jar'
# (this version should be good through 'voldb/voltdb-99.9.9.9.9.9.jar').
# Note that 'org.voltdb.VoltDB' cannot be used because it does not work on
# Ubuntu-14.04 machines; and 'jps' no longer seems to work either.
function tt-server-count() {
    COUNT_VOLTDB_PROCESSES=`ps -ef | grep -v grep | grep -vi SQLCommand | grep -cE 'voltdb/voltdb-[1-9]?[0-9](.[0-9])+.jar'`
    echo -e "\n$0 performing: $FUNCNAME, result: $COUNT_VOLTDB_PROCESSES"
    return $COUNT_VOLTDB_PROCESSES
}

# Start the VoltDB server ('community'), only if not already running
function tt-server-if-needed() {
    if [[ "$TT_DEBUG" -ge "4" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-server-count
    RETURN_CODE=$?
    if [[ "$RETURN_CODE" -gt "0" ]]; then
        echo -e "Not (re-)starting a VoltDB server, because 'ps -ef' now includes a VoltDB process."
    else
        echo -e "A VoltDB server will be started, because 'ps -ef' does not include a VoltDB process."
        tt-server
    fi
}

# Start the VoltDB 'pro' server, only if not already running
function tt-server-pro-if-needed() {
    if [[ "$TT_DEBUG" -ge "4" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-server-count
    RETURN_CODE=$?
    if [[ "$RETURN_CODE" -gt "0" ]]; then
        echo -e "Not (re-)starting a VoltDB (pro) server, because 'ps -ef' now includes a VoltDB process."
    else
        echo -e "A VoltDB (pro) server will be started, because 'ps -ef' does not include a VoltDB process."
        tt-server-pro
    fi
}

# Stop the VoltDB server, and kill any straggler processes
function tt-shutdown() {
    if [[ "$TT_DEBUG" -ge "1" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi
    tt-find-directories-if-needed

    # Stop the VoltDB server (& kill any stragglers)
    $VOLTDB_BIN_DIR/voltadmin shutdown --force
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
function tt-all() {
    echo -e "\n$0 performing: $FUNCNAME"
    tt-build
    tt-init
    tt-print-vars
    tt-server
}

# Builds the latest (local) version of VoltDB (the 'pro' version), and starts a
# ('pro') VoltDB server, after setting and echoing various environment variables
function tt-all-pro() {
    echo -e "\n$0 performing: $FUNCNAME"
    tt-build-pro
    tt-init
    tt-print-vars
    tt-server-pro
}

# Print a simple help message, describing the options for this script
function tt-help() {
    echo -e "\nUsage: ./test-tools.sh tt-{build[-pro]|init|print-vars|server[-pro]|all[-pro]|shutdown}"
    echo -e "Or   : ./test-tools.sh tt-{echo-build-args|set-build-args|set-setseed|killstragglers|"
    echo -e "                           start-postgres[ql]|stop-postgres[ql]|help}"
    echo -e "This script is largely intended to provide useful functions that may be called"
    echo -e "    by a variety of other test scripts, e.g., <voltdb>/tests/sqlgrammar/run.sh,"
    echo -e "    but it may also be called directly on the command line."
    echo -e "Options:"
    echo -e "    tt-build              : builds VoltDB ('community', open-source version)"
    echo -e "    tt-build-pro          : builds VoltDB ('pro' version)"
    echo -e "    tt-init               : sets useful variables such as CLASSPATH and PATH"
    echo -e "    tt-print-vars         : prints the values of variables such as VOLTDB_COM_DIR and PATH"
    echo -e "    tt-server             : starts a VoltDB server ('community', open-source version)"
    echo -e "    tt-server-pro         : starts a VoltDB server ('pro' version)"
    echo -e "    tt-all                : runs (almost) all of the above, except the '-pro' options"
    echo -e "    tt-all-pro            : runs (almost) all of the above, using the '-pro' options"
    echo -e "    tt-shutdown           : stops a VoltDB server that is currently running"
    echo -e "    tt-echo-build-args    : echoes build (& test) args commonly used in Jenkins"
    echo -e "    tt-set-build-args     : sets the BUILD_ARGS, VOLTBUILD_ARG environment variables"
    echo -e "    tt-set-test-args      : sets the TEST_ARGS environment variable"
    echo -e "    tt-set-verify-arg     : sets the VERIFY_ARG environment variable"
    echo -e "    tt-set-setseed        : sets the SETSEED environment variable"
    echo -e "    tt-killstragglers     : calls the 'killstragglers' script, passing"
    echo -e "                                it the standard PostgreSQL port number"
    echo -e "    tt-start-postgres[ql] : calls the start_postgresql.sh script"
    echo -e "    tt-stop-postgres[ql]  : calls the stop_postgresql.sh script"
    #echo -e "    tt-sqlcoverage        : NOT YET IMPLEMENTED!"
    echo -e "    tt-help               : prints this message"
    echo -e "Some options (tt-build[-pro], tt-init, tt-server[-pro], tt-echo-build-args) may have"
    echo -e "    '-if-needed' appended, e.g.,'tt-server-if-needed' will start a VoltDB server only"
    echo -e "    if one is not already running."
    echo -e "Some options (tt-set-build-args, tt-set-test-args, tt-set-verify-arg, tt-set-setseed) may be"
    echo -e "    passed argument(s) that determine what the relevant environment variable will be set to."
    echo -e "Multiple options may be specified; but options usually call other options that are prerequisites.\n"
    PRINT_ERROR_CODE=0
}

# Old, deprecated name for tt-find-directories
function test-tools-find-directories() {
    tt-find-directories "$@"
}
# Old, deprecated name for tt-find-directories-if-needed
function test-tools-find-directories-if-needed() {
    tt-find-directories-if-needed "$@"
}
# Old, deprecated name for tt-build
function test-tools-build() {
    tt-build "$@"
}
# Old, deprecated name for tt-build-pro
function test-tools-tt-build-pro() {
    tt-build-pro "$@"
}
# Old, deprecated name for tt-build-if-needed
function test-tools-build-if-needed() {
    tt-build-if-needed "$@"
}
# Old, deprecated name for tt-build-pro-if-needed
function test-tools-build-pro-if-needed() {
    tt-build-pro-if-needed "$@"
}
# Old, deprecated name for tt-init
function test-tools-init() {
    tt-init "$@"
}
# Old, deprecated name for tt-print-vars
function test-tools-debug() {
    tt-print-vars "$@"
}
# Old, deprecated name for tt-server
function test-tools-server() {
    tt-server "$@"
}
# Old, deprecated name for tt-server-pro
function test-tools-server-pro() {
    tt-server-pro "$@"
}
# Old, deprecated name for tt-server-if-needed
function test-tools-server-if-needed() {
    tt-server-if-needed "$@"
}
# Old, deprecated name for tt-server-pro-if-needed
function test-tools-server-pro-if-needed() {
    tt-server-pro-if-needed "$@"
}
# Old, deprecated name for tt-shutdown
function test-tools-shutdown() {
    tt-shutdown "$@"
}
# Old, deprecated name for tt-help
function test-tools-help() {
    tt-help
}

# After detecting a fatal error, call this function, which will exit with the
# specified exit code ($1), after printing the specified function name ($2)
# and error messages ($3 and $4); note that all parameters are optional, and
# will assume default values if unspecified
function tt-exit-script() {
    if [[ "$TT_DEBUG" -ge "1" ]]; then echo -e "\n$0 performing: $FUNCNAME"; fi

    # Set variables equal to the first 4 args or, if not specified, default values
    TT_EXIT_CODE="${1:-1}"
    FUNC_NAME="${2:-$FUNCNAME}"
    ERROR_MSG="${3:-Unknown error}"
    EXTRA_MSG="$4"

    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\nTT_EXIT_CODE: $TT_EXIT_CODE"
        echo -e "FUNC_NAME: $FUNC_NAME"
        echo -e "ERROR_MSG: $ERROR_MSG"
        echo -e "EXTRA_MSG: $EXTRA_MSG"
    fi

    if [[ "$TT_EXIT_CODE" -ne "0" ]]; then
        echo -e "\nFATAL: $ERROR_MSG in ${BASH_SOURCE[0]}, function $FUNC_NAME"
        if [[ -n "$EXTRA_MSG" ]]; then
            echo -e "\nAdditional FATAL Error Info:\n$EXTRA_MSG"
        fi
    fi

    # If this script was called "normally", then 'exit' with the appropriate
    # error code; but if it was called using 'source' or '.', then simply
    # 'return' with that code, in order to avoid also exiting the shell
    if [[ "$0" == "${BASH_SOURCE[0]}" ]]; then
        echo -e "\n$FUNCNAME exiting with code: $TT_EXIT_CODE\n"
        exit $TT_EXIT_CODE
    else
        echo -e "\n$FUNCNAME returning with code: $TT_EXIT_CODE\n"
        return $TT_EXIT_CODE
    fi
}

# If run on the command line with no options specified, run tt-help
if [[ $# -eq 0 && $0 == *test-tools.sh ]]; then
    tt-help
fi

# Set (or reset) environment variables used only in this script, not externally
BUILD_ARGS_WERE_ECHOED=
TT_EXIT_CODE=0

# Run options passed on the command line, if any; with arguments, if any
while [[ -n "$1" ]]; do
    SHIFT_BY=1  # Can be increased, by a function that handles multiple args
    $@
    shift $SHIFT_BY

    if [[ "$TT_EXIT_CODE" -ne "0" ]]; then
        return $TT_EXIT_CODE
    fi
done
