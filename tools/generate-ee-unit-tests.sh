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

BUILD=debug
VERBOSE=
GENERATED_DIR="generated"
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
        --names-only)
            NAMES_ONLY=--names-only
            shift
            ;;
        --test-class)
            shift
            TEST_CLASSES="$TEST_CLASSES $1"
            shift
            ;;
        --build)
            shift
            BUILD="$1"
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
        --voltdbroot)
            shift
            VOLTDBROOT="$1"
            if [ ! -d "$VOLTDBROOT" ] ; then
                echo "$0: Source directory \"$VOLTDBROOT\" does not exist."
                exit 100
            fi
            shift
            ;;
        --objdir)
            shift
            OBJDIR="$1"
            shift
            if [ ! -d "$OBJDIR" ] ; then
                echo "$0: Object directory \"$OBJDIR\" does not exist."
                exit 100
            fi
            ;;
        --help)
            echo 'Usage: generate-ee-unit-tests [ options ] -- test classes'
            echo 'Options:'
            echo ' --verbose                Run java -v'
            echo ' --voltdbroot DIR         The root of the voltdb tree is DIR.'
            echo ' --objdir DIR             The object directory, where all the'
            echo '                          builds occur, is DIR.  This will typically be'
            echo '                          /home/user/.../voltdb/obj/debug for a release'
            echo '                          build.  This is required.'
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
            echo ' --generated-dir dirname  Put generated artifacts in'
            echo '                          obj/$BUILD/dirname, where'
            echo '                          $BUILD is the build type.'
            echo '                          Files will go:'
            echo '                            obj/$BUILD/dirname/src  source files'
            echo '                            obj/$BUILD/dirname/obj  .o files'
            echo '                            obj/$BUILD/dirname/bin  executable files'
            exit 100
            ;;
        *)
            echo "$0: Unknown command line parameter $1"
            exit 100
            ;;
    esac
done
if [ -z "$TEST_CLASSES" ] ; then
    echo "$0: No test classes specified."
    exit 100
fi
if [ -z "$OBJDIR" ] ; then
    echo "$0: --objdir is required."
    exit 100
fi
if [ -z "$VOLTDBROOT" ] ; then
    echo "$0: --voltdbroot is required."
    exit 100
fi

for CLASS in $TEST_CLASSES; do
    (set $ECHO; java $VERBOSE -cp ${OBJDIR}/prod:${OBJDIR}/test:${VOLTDBROOT}/lib/\*:${VOLTDBROOT}/third_party/java/jars/\* -Dlog4j.configuration=file:${VOLTDBROOT}/tests/log4j-allconsole.xml $CLASS ${NAMES_ONLY} --generated-dir "$GENERATED_DIR/src")
done
