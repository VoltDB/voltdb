#!/bin/sh
# This is just temporary.  Delete this before merging to master.
# this command should go into the makefile somehow.

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
            case "$BUILD" in
                debug|release|memcheck)
                    ;;
                *)
                    echo "$0: Unknown argument to --build: \"$BUILD\""
                    exit 100
                    ;;
            esac
            shift
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
