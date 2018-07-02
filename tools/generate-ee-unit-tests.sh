#!/bin/sh
# A script to generate all the generated EE unit tests.  This allows us to
# avoid the churn which results from checking them into git.  We don't need
# to manage licenses for the generated files, since they are only present at
# build time.
#
# This seems like it should be temporary, and that the the command
# should be in the ant file, or else embedded into buildtools.py.  But
# it's convenient and useful for debugging to put it here, so that the
# tests can easily be generated manually.

BUILD_TYPE=release
VERBOSE=
ECHO=+x

while [ -n "$1" ]; do
    case "$1" in
        --debug)
            ECHO=-x
            shift
            ;;
        --verbose)
            VERBOSE=-v
            shift
            ;;
        --test-class=*)
            TEST_CLASSES="$TEST_CLASSES $(echo $1 | sed 's/--test-class=//')"
            shift
            ;;
        --build-type=*)
            BUILD_TYPE="$(echo $1 | sed 's/--build-type=//')"
            shift
            case "$BUILD_TYPE" in
                debug|release|memcheck)
                    ;;
                *)
                    echo "$0: Unknown argument to --build: \"$BUILD_TYPE\""
                    exit 100
                    ;;
            esac
            ;;
        --voltdbroot=*)
            VOLTDB_ROOT="$(echo $1 | sed 's/--voltdbroot=//')"
            shift
            if [ ! -d "$VOLTDB_ROOT" ] ; then
                echo "$0: Source directory \"$VOLTDB_ROOT\" does not exist."
                exit 100
            fi
            if [ ! -d "$VOLTDB_ROOT/lib" ] \
                || [ ! -d "$VOLTDB_ROOT/third_party/java/jars" ] \
                || [ ! -f "$VOLTDB_ROOT/tests/log4j-allconsole.xml" ] ; then
                  echo "$0: Source directory \"$VOLTDB_ROOT\" is implausible.  Is it right?"
                  exit 100
            fi
            ;;
        --help)
            echo 'Usage: generate-ee-unit-tests [ options ] -- test classes'
            echo 'Options:'
            echo ' --verbose                Run java -v'
            echo ' --voltdbroot DIR         The root of the voltdb tree is DIR.'
            echo ' --test-class class-name  Run the given class name'
            echo '                          as a Java main program.  It'
            echo '                          will know which tests it'
            echo '                          generates.  This can be'
            echo '                          repeated multiple times, but'
            echo '                          there must be at least one of'
            echo '                          these options provided.'
            exit 100
            ;;
        *)
            echo "$0: Unknown command line parameter \"$1\""
            exit 100
            ;;
    esac
done
if [ -z "$TEST_CLASSES" ] ; then
    echo "$0: No test classes specified."
    exit 100
fi

SRC_DIR="$VOLTDB_ROOT/tests/ee"
GENERATED_DIR_NAME='ee_auto_generated_unit_tests'
GENERATED_DIR_PATH="${SRC_DIR}/${GENERATED_DIR_NAME}"
OBJDIR="$VOLTDB_ROOT/obj/${BUILD_TYPE}"
TEST_NAMES_FILE="${GENERATED_DIR_PATH}/generated_tests.txt"
# echo "SRC_DIR=${SRC_DIR}"
# echo "GENERATED_DIR_PATH=${GENERATED_DIR_PATH}"
# echo "TEST_NAMES_FILE=${TEST_NAMES_FILE}"
# echo "OBJDIR=${OBJDIR}"
#
# Empty out the test names file.
#
rm -f "$TEST_NAMES_FILE"
for CLASS in $TEST_CLASSES; do
    (set $ECHO; java $VERBOSE \
                     -cp ${OBJDIR}/prod:${OBJDIR}/test:${VOLTDB_ROOT}/lib/\*:${VOLTDB_ROOT}/third_party/java/jars/\* \
                     -Dlog4j.configuration=file:${VOLTDB_ROOT}/tests/log4j-allconsole.xml $CLASS \
                     --generated-source-dir="$GENERATED_DIR_PATH" \
                     --test-names-file="$TEST_NAMES_FILE"
    )
done
