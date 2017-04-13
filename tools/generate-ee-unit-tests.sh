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
while [ -n "$1" ]; do
    case "$1" in
        --debug)
            set -x
            shift
            ;;
        --verbose)
            VERBOSE=-v
            shift
            ;;
        --generated-dir)
            shift
            GENERATED_DIR="$1"
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
        --help)
            echo 'Usage: generate-ee-unit-tests [ options ]'
            echo 'Options:'
            echo ' --verbose                Run java -v'
            echo ' --build buildType        Set the build type.  The'
            echo '                          possibilities are debug,'
            echo '                          release and memcheck.'
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

    esac
done
java $VERBOSE -cp obj/$BUILD/prod:obj/$BUILD/test:lib/\*:third_party/java/jars/\* org.voltdb.planner.eegentests.EEPlanTestGenerator --generated-dir "obj/$BUILD/$GENERATED_DIR/src"
