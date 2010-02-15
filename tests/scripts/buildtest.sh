#!/bin/bash
#
# Automatically builds the project and runs the test. Intended to be used with
# a continuous build system.

SVN_CLEAN=../../tools/svn-clean.pl
VALLEAK=MIT_ACR/scripts/valleak.py

#~ set -o errexit

if [ -n "$HUDSON_URL" ]; then
    # If we are running under hudson do a clean build: only keep files in svn
    $SVN_CLEAN -f -q
fi

# HACK: LD_LIBRARY_PATH should not need to be set. rpath linking options should be used to avoid
# this problem.
LD_LIBRARY_PATH=. ant all -Dhudson=true -Djunit.haltonfailure=no
RESULT=$?

# try to build and run as many tests as possible
make -k -j4 --directory=MIT_ACR/EE/src &> /dev/null
make -k -j4 --directory=MIT_ACR/EE/test &> /dev/null
echo "Collecting C++ test results with Valgrind"
find MIT_ACR/EE/test -type f | grep '_test$' | xargs MIT_ACR/scripts/runcppunit.py --output_dir=GPL_VCR/gpl_java/build/testoutput
TESTS_FAILED=$?

if (($RESULT != 0 || $TESTS_FAILED != 0)); then
    echo "BUILD FAILED"
    exit 1
fi
