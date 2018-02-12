#!/usr/bin/env python

import sys
import argparse
from subprocess import Popen, PIPE, STDOUT

parser = argparse.ArgumentParser(description="Run All The Tests.")
parser.add_argument("--valgrind",
                    default=False,
                    action='store_true',
                    help='Use valgrind on tests.')
parser.add_argument("tests",
                    nargs='*',
                    help='Tests in dir/test_name form.')

args=parser.parse_args()

def makeValgrindFile(pidStr):
    return "valgrind_ee_%s.xml" % pidStr

for test in args.tests:
    noValgrindTests = [ "CompactionTest", "CopyOnWriteTest", "harness_test", "serializeio_test" ]
    using_valgrind = args.valgrind && isValgrindTest(test)
    if not using_valgrind:
        pass
    else:
        valgrindFile = makeValgrindFile("%p")
        process = Popen(executable="valgrind",
                        args=["valgrind",
                              "--leak-check=full",
                              "--show-reachable=yes",
                              "--error-exitcode=-1",
                              "--suppressions=" + os.path.join(TEST_PREFIX,
                                                               "test_utils/vdbsuppressions.supp"),
                              "--xml=yes",
                              "--xml-file=" + valgrindFile,
                              targetpath], stderr=PIPE, bufsize=-1)
        out_err = process.stderr.readlines()
        retval = process.wait()
        fileName = makeValgrindFile("%d" % process.pid)
        errorState = ValgrindErrorState(expectNoMemLeaks, fileName)
        # If there are as many errors as we expect,
        # then delete the xml file.  Otherwise keep it.
        # It may be useful.
        if ( not errorState.isExpectedState()):
            try:
                os.remove(fileName)
            except ex:
                pass
        print errorState.errorMessage()
        retval = -1
        sys.stdout.flush()
