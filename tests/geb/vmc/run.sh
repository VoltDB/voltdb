#!/bin/bash
# Script used to run the GEB tests of the VMC (VoltDB Management Center),
# including all the steps needed for the tests to work, such as: building
# VoltDB, starting a VoltDB server, running the associated DDL file, and
# shutting down the VoltDB server at the end. These steps may be run
# separately, or all together.

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
        # It looks like we're running from a <voltdb>/tests/geb/ directory
        # (or a similar directory, just below /tests/)
        VOLTDB_TESTS=$(cd $HOME_DIR/..; pwd)
    elif [[ $HOME_DIR == */tests/*/* ]] && [[ -e $HOME_DIR/../../test-tools.sh ]]; then
        # It looks like we're running from a <voltdb>/tests/geb/vmc/ directory
        # (or a similar directory, two levels below /tests/)
        VOLTDB_TESTS=$(cd $HOME_DIR/../..; pwd)
    elif [[ -n "$(which voltdb 2> /dev/null)" ]]; then
        # It looks like we're using VoltDB from the PATH
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
# <voltdb>/tests/geb/vmc/ directories, and the <voltdb>/bin directory
# containing the VoltDB binaries; and set variables accordingly
function find-directories() {
    if [[ "$TT_DEBUG" -ge "2" ]]; then
        echo -e "\n$0 performing: find-directories"
    fi
    test-tools-find-directories-if-needed
    GEB_VMC_DIR=$VOLTDB_TESTS/geb/vmc
    DDL_FILE=$GEB_VMC_DIR/ddl.sql
    # Default value, assuming 'community', open-source version of VoltDB
    DEPLOYMENT_FILE=$GEB_VMC_DIR/deployment.xml
}

# Find the directories and set variables, only if not set already
function find-directories-if-needed() {
    if [[ -z "$GEB_VMC_DIR" ]] || [[ -z "$DDL_FILE" ]] || [[ -z "$DEPLOYMENT_FILE" ]]; then
        find-directories
    fi
}

# Build VoltDB: 'community', open-source version
function build() {
    echo -e "\n$0 performing: build"
    test-tools-build
    code[0]=$code_tt_build
}

# Build VoltDB: 'pro' version
function build-pro() {
    echo -e "\n$0 performing: build-pro"
    test-tools-build-pro
    DEPLOYMENT_FILE=$GEB_VMC_DIR/deploy_pro.xml
    code[0]=$code_tt_build
}

# Build VoltDB ('community'), only if not built already
function build-if-needed() {
    test-tools-build-if-needed
    code[0]=$code_tt_build
}

# Build VoltDB 'pro' version, only if not built already
function build-pro-if-needed() {
    test-tools-build-pro-if-needed
    code[0]=$code_tt_build
}

# Set CLASSPATH, PATH, and python, as needed
function init() {
    find-directories-if-needed
    build-if-needed
    echo -e "\n$0 performing: init"
    test-tools-init
    code[1]=${code_tt_init}
}

# Set CLASSPATH, PATH, and python, only if not set already
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
    echo "GEB_VMC_DIR    :" $GEB_VMC_DIR
    echo "DDL_FILE       :" $DDL_FILE
    echo "DEPLOYMENT_FILE:" $DEPLOYMENT_FILE
    echo "FIRST_ARGS     :" $FIRST_ARGS
    echo "MIDDLE_ARGS    :" $MIDDLE_ARGS
    echo "LAST_ARGS      :" $LAST_ARGS
    echo "ARGS           :" $ARGS
    echo "TEST_ARGS      :" $TEST_ARGS
}

# Create the Jar file used by the GEB tests of the VMC
function jars() {
    init-if-needed
    echo -e "\n$0 performing: jars"

    # Build the jar file for the GEB tests of the VMC
    JARS_HOME_DIR=$(pwd)
    cd ${VOLTDB_COM_DIR}/obj/release/testprocs/
    ls org/voltdb_testprocs/fullddlfeatures/*.class > classes.list
    jar cvf ${JARS_HOME_DIR}/fullddlfeatures.jar \
        -C ${VOLTDB_COM_DIR}/obj/release/testfuncs org/voltdb_testfuncs/BasicTestUDFSuite.class \
        @classes.list
    code[2]=$?
    rm -f classes.list
    cd -
}

# Create the Jar file, only if not created already
function jars-if-needed() {
    if [[ ! -e fullddlfeatures.jar ]]; then
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
    find-directories-if-needed
    echo -e "\n$0 performing: server-pro"
    DEPLOYMENT_FILE=$GEB_VMC_DIR/deploy_pro.xml
    TEST_ARGS=" -Pdr=true"
    # TODO: uncomment the line below, and delete the one above
    # (& this comment), once ENG-14518 is fixed
    #TEST_ARGS=" -Pdr=true -Pimporter=true"
    test-tools-server-pro
    code[3]=${code_tt_server}
}

# Start the VoltDB server ('community'), only if not already running
function server-if-needed() {
    test-tools-server-if-needed
    code[3]=${code_tt_server}
}

# Start the VoltDB 'pro' server, only if not already running
function server-pro-if-needed() {
    test-tools-server-pro-if-needed
    code[3]=${code_tt_server}
}

# Load the DDL file for the GEB tests of the VMC
function ddl() {
    find-directories-if-needed
    server-if-needed
    echo -e "\n$0 performing: ddl; running (in sqlcmd): $DDL_FILE"
    $VOLTDB_BIN_DIR/sqlcmd < $DDL_FILE
    code[4]=$?
}

# Load the schema and procedures (in sqlcmd), only if not loaded already
function ddl-if-needed() {
    # TODO: find a more reliable test of whether 'ddl' has been loaded
    if [[ -z "${code[4]}" ]]; then
        ddl
    fi
}

# Run everything you need to before running the GEB tests of the VMC, without
# actually running them; provides a "fresh start", i.e., re-builds the latest
# version of VoltDB ('community'), etc.
function prepare() {
    echo -e "\n$0 performing: prepare"
    build
    init
    debug
    jars
    server
    ddl
}

# Run everything you need to before running the GEB tests of the VMC using a
# 'pro' build, without actually running them; provides a "fresh start", i.e.,
# re-builds the latest version of VoltDB 'pro', etc.
function prepare-pro() {
    echo -e "\n$0 performing: prepare-pro"
    build-pro
    init
    debug
    jars
    server-pro
    ddl
}

# Run the GEB tests of the VMC, only, on the assumption that 'prepare' or
# 'prepare-pro' (or the equivalent) has already been run
function tests-only() {
    init-if-needed
    echo -e "\n$0 performing: tests[-only]${TEST_ARGS}${ARGS}"

    cd $GEB_VMC_DIR
    TEST_COMMAND="./gradlew${TEST_ARGS}${ARGS}"
    echo -e "running:\n$TEST_COMMAND"
    $TEST_COMMAND
    code[5]=$?
    cd -
}

# Run the GEB tests of the VMC, with the usual prerequisites
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
    test-tools-shutdown
    code[6]=${code_tt_shutdown}
}

# Run the GEB tests of the VMC, after first running everything you need to
# prepare for them; provides a "fresh start", i.e., re-builds the latest
# versions of VoltDB ('community'), the Jar file, etc.
function all() {
    echo -e "\n$0 performing: all$ARGS"
    prepare
    tests
    shutdown
}

# Run the GEB tests of the VMC, using a 'pro' build, after first running
# everything you need to prepare for them; provides a "fresh start", i.e.,
# re-builds the latest versions of VoltDB ('pro'), the Jar file, etc.
function all-pro() {
    echo -e "\n$0 performing: all-pro$ARGS"
    prepare-pro
    tests
    shutdown
}

# Print a brief help message, describing arguments for the GEB tests of the VMC
function tests-help() {
    echo -e "\nHere is a brief description of some of the arguments available when running the GEB tests of the VMC:"
    echo -e "    --debug       : gets replaced with '-PdebugPrint=true', which provides debug print"
    echo -e "    --basic       : gets replaced with '--tests=*BasicTest*', which causes the 'basic' tests to run"
    echo -e "    phantomjs     : run the tests against PhantomJS / Ghost driver (as in Jenkins; must download)"
    echo -e "    chrome        : run the tests against a Chrome browser (must download chromedriver, in your PATH)"
    echo -e "    firefox       : run the tests against a Firefox browser (used to work well, needs work)"
    echo -e "    ie            : run the tests against a Chrome browser (does not work that well)"
    echo -e "    safari        : run the tests against a Safari browser (does not work well)"
    echo -e "    htmlunit      : run the tests against HtmlUnit (does not work well)"
    echo -e "    --rerun-tasks : standard Gradle arg; unnecessary, gets added automatically"
    echo -e "    --tests=*NavigatePages*       : run only the 'NavigatePagesBasicTest' tests"
    echo -e "    --tests=*FullDdlSqlBasicTest* : run only the 'FullDdlSqlBasicTest' tests"
    echo -e "    --tests=*SqlQueriesBasicTest* : run only the 'SqlQueriesBasicTest' tests"
    echo -e "    --tests=*sqlQueries*          : run only the 'SqlQueriesBasicTest' sqlQueries.txt tests"
    echo -e "    -PtimeoutSeconds=30           : use a timeout of 30 seconds for loading pages"
    echo -e "    -PsqlTests=SetVariables,InsertBigInt : after setting variables, run InsertBigInt test only"
    echo -e "    -PsqlTests=CREATE_ROLE_admin,CREATE_TABLE_T26 : run those two tests only"
    echo -e "    -Purl=URL              : the URL to use for testing (default: http://localhost:8080/)"
    echo -e "    -PgebVersion=VER       : the version of GEB to use (default: 0.12.2)"
    echo -e "    -PspockVersion=VER     : the version of Spock to use (default: 0.7-groovy-2.0)"
    echo -e "    -PseleniumVersion=VER  : the version of Selenium to use (default: 2.47.1)"
    echo -e "    -PphantomjsVersion=VER : the version of PhantomJS Ghost Driver to use (default: 1.2.1)"
    echo -e "For more info, read the (long) README.md file (via the 'readme' option)"
}

# Print the (long) README.md file for the GEB tests of the VMC
function readme() {
    find-directories-if-needed
    more $GEB_VMC_DIR/README.md
}

# Print a simple help message, describing the options for this script
function help() {
    echo -e "\nUsage: ./run.sh {build[-pro]|init|debug|jars|server[-pro]|ddl|prepare[-pro]|"
    echo -e "  tests-only|tests|shutdown|all[-pro]|tests-help|readme|test-tools-help|help}\n"
    echo -e "This script is used to run the GEB tests of the VMC, and the various things that go"
    echo -e "  with them, e.g., building and running a VoltDB server, or shutting it down afterward"
    echo -e "Options:"
    echo -e "    build           : builds VoltDB ('community', open-source version)"
    echo -e "    build-pro       : builds VoltDB ('pro' version)"
    echo -e "    init            : sets useful variables such as CLASSPATH and PATH"
    echo -e "    debug           : prints the values of variables such as VOLTDB_COM_DIR and PATH"
    echo -e "    jars            : creates the (Java) .jar file needed by the tests"
    echo -e "    server          : starts a VoltDB server ('community', open-source version)"
    echo -e "    server-pro      : starts a VoltDB server ('pro' version)"
    echo -e "    ddl             : runs (in sqlcmd) the DDL (.sql) file needed by the tests"
    echo -e "    prepare         : runs (almost) all of the above, except the '-pro' options"
    echo -e "    prepare-pro     : runs (almost) all of the above, using the '-pro' options"
    echo -e "    tests-only      : runs only the tests, on the assumption that 'prepare[-pro]' has been run"
    echo -e "    tests           : runs the tests, preceded by whatever other (non-pro) options are needed"
    echo -e "    shutdown        : stops a VoltDB server that is currently running"
    echo -e "    all             : runs 'prepare', 'tests', 'shutdown', effectively calling everything (non-pro)"
    echo -e "    all-pro         : runs 'prepare-pro', 'tests', 'shutdown', effectively calling everything (-pro)"
    echo -e "    tests-help      : prints a (brief) help message for the GEB tests of the VMC"
    echo -e "    readme          : prints the (long) README file for the GEB tests of the VMC"
    echo -e "    test-tools-help : prints a help message for the test-tools.sh script, which is used by this one"
    echo -e "    help            : prints this message"
    echo -e "The 'tests-only', 'tests', and 'all[-pro]' options accept arguments: see the 'tests-help' option for details.\n"
    echo -e "Some options (build[-pro], init, jars, server[-pro], ddl) may have '-if-needed' appended,"
    echo -e "  e.g., 'server-if-needed' will start a VoltDB server only if one is not already running."
    echo -e "Multiple options may be specified; but options usually call other options that are prerequisites.\n"
    exit
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
        if [[ "${code[3]}" -ne "0" ]]; then
            echo -e "\ncode3a code3b: $code_voltdb_init $code_voltdb_start (server-init, server-start)"
        fi
        echo -e "\ncodes 0-6: ${code[*]} (build, init, jars, server, ddl, tests, shutdown)"
    fi
    echo "error code:" $errcode
    exit $errcode
}

# If no options specified, run help
if [[ $# -eq 0 ]]; then
    help
fi

# Run the options passed on the command line
run-test-tools
FIRST_ARGS=
MIDDLE_ARGS=
LAST_ARGS=

while [[ -n "$1" ]]; do
    CMD="$1"
    FOUND_RERUN_TASKS=false
    if [[ "$1" == "tests" ]] || [[ "$1" == "tests-only" ]] || [[ "$1" == all* ]]; then
        # Divide test arguments into the "-P" args (which go first), the "--"
        # arguments (which go last), and the rest (which go in the middle)
        while [[ -n "$2" ]] && [[ "$2" != "shutdown" ]] && [[ "$2" != "debug" ]]; do
            if [[ "$2" == -P* ]]; then
                FIRST_ARGS="${FIRST_ARGS} $2"
            elif [[ "$2" == "--basic" ]] || [[ "$2" == "-basic" ]]; then
                # "--basic" is short for "--tests=*BasicTest*"
                LAST_ARGS="${LAST_ARGS} --tests=*BasicTest*"
            elif [[ "$2" == "--debug" ]] || [[ "$2" == "-debug" ]]; then
                # "--debug" is short for "-PdebugPrint=true"
                FIRST_ARGS="${FIRST_ARGS} -PdebugPrint=true"
            elif [[ "$2" == "--dr" ]] || [[ "$2" == "-dr" ]]; then
                # "--dr" is short for "-Pdr=true"
                FIRST_ARGS="${FIRST_ARGS} -Pdr=true"
            elif [[ "$2" == "--importer" ]] || [[ "$2" == "-importer" ]]; then
                # "--importer" is short for "-Pimporter=true"
                FIRST_ARGS="${FIRST_ARGS} -Pimporter=true"
            elif [[ "$2" == "--headless" ]] || [[ "$2" == "-headless" ]]; then
                # Changes the meaning of the browser selection
                export HEADLESS="TRUE"
                # HEADLESS=true

            elif [[ "$2" == --* ]]; then
                LAST_ARGS="${LAST_ARGS} $2"
                if [[ "$item" == "--rerun-tasks" ]]; then
                    FOUND_RERUN_TASKS=true
                fi
            else
                # This is usually one (or more) browser to test, e.g.,
                # chrome, firefox, phantomjs, ie, safari, or htmlunit
                MIDDLE_ARGS="${MIDDLE_ARGS} $2"
            fi
            shift
        done
        # Use default browser, chrome, if none was specified
        if [[ -z "${MIDDLE_ARGS}" ]]; then
            MIDDLE_ARGS=" chrome"
        fi

        # Make sure the "--rerun-tasks" arg is included, if not specified explicitly
        if [[ "$FOUND_RERUN_TASKS" == false ]]; then
            LAST_ARGS="${LAST_ARGS} --rerun-tasks"
        fi
        ARGS=${FIRST_ARGS}${MIDDLE_ARGS}${LAST_ARGS}
    fi
    $CMD
    shift
done
exit-with-code
