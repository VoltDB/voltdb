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

BUILD=release
VERBOSE=
ECHO=+x
#
# Look in various places to find the root of the
# build hierarchy.
#
isVoltDBRoot() {
	local DIR="$1"
    if [ ! -f "$DIR/build.xml" ]; then
        return 1
    fi
    if grep '<project' "$DIR/build.xml" | grep -q 'name="VoltDB"'; then
        return 0
    fi
    return 1;
}
if isVoltDBRoot "."; then
	VOLTDB_ROOT="$(/bin/pwd)"
elif isVoltDBRoot "../.."; then
	VOLTDB_ROOT="$(cd ../..; /bin/pwd)"
else
    echo "$0: Can't find the voltdb root"
    exit 100
fi
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
        --names-only)
            NAMES_ONLY=--names-only
            shift
            ;;
        --test-class=*)
            TEST_CLASSES="$TEST_CLASSES $(echo $1 | sed 's/--test-class=//')"
            shift
            ;;
        --build=*)
            BUILD="$(echo $1 | sed 's/--build=//')"
            shift
            case "$BUILD" in
                debug|release|memcheck)
                    ;;
                *)
                    echo "$0: Unknown argument to --build: \"$BUILD\""
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
            echo ' --build buildType        Set the build type.  The'
            echo '                          possibilities are debug,'
            echo '                          release and memcheck.'
            echo ' --names-only             Only echo the test names'
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

GENERATED_DIR="$VOLTDB_ROOT/tests/ee/ee_auto_generated_unit_tests"
OBJDIR="$VOLTDB_ROOT/obj/${BUILD}"
echo "VOLTDB_ROOT=$VOLTDB_ROOT"
echo "OBJDIR=${OBJDIR}"
for CLASS in $TEST_CLASSES; do
    (set $ECHO; java $VERBOSE -cp ${OBJDIR}/prod:${OBJDIR}/test:${VOLTDB_ROOT}/lib/\*:${VOLTDB_ROOT}/third_party/java/jars/\* -Dlog4j.configuration=file:${VOLTDB_ROOT}/tests/log4j-allconsole.xml $CLASS ${NAMES_ONLY} --voltdb-root="${VOLTDB_ROOT}")
done
