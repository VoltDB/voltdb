#!/usr/bin/env python3

# 1. make sure all test cases leave junit results
# 2. make sure no processes are stranded (fail and kill if so)

import sys,os,re,logging,time,shutil
from optparse import OptionParser

def cmd(cmd):
    """run a command line tool and collect the output"""
    fd = os.popen(cmd)
    retval = fd.read()
    fd.close()
    return retval

def cmd_readlines(cmd):
    """run a command line tool and collect the lines of output"""
    fd = os.popen(cmd)
    retval = fd.readlines()
    fd.close()
    return retval

class Result:
    def __init__(self, className, name, failure):
        self.className = className
        self.name = name
        self.failure = failure

def lameXmlEscape(input):
    input = input.replace('&', '&amp;')
    return input.replace('<', '&lt;')

def writeJUnitXml(fileobj, suite_name, elapsed_time, stdout, stderr, results):
    """Writes a JUnit test report in XML format to fileobj from results."""

    timestamp = time.time()
    iso_time = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(timestamp))

    tests = 0
    failures = 0
    for result in results:
        if result.failure is not None:
            failures += 1
        tests += 1

    fileobj.write('<?xml version="1.0" encoding="utf-8"?>\n')
    fileobj.write('<testsuite errors="0" failures="%d" name="%s" tests="%d" time="%.3f" timestamp="%s">\n' %
            (failures, suite_name, tests, elapsed_time, iso_time))

    for result in results:
        fileobj.write('<testcase classname="%s" name="%s" time="0.000">' %
                (suite_name + "." + result.className, result.name))
        if result.failure is not None:
            fileobj.write('<failure message="null" type="junit.framework.AssertionFailedError">')
            fileobj.write(lameXmlEscape(result.failure))
            fileobj.write('</failure>')
        fileobj.write('</testcase>\n')

    fileobj.write("<system-out>")
    fileobj.write(lameXmlEscape(stdout))
    fileobj.write("</system-out>\n")

    fileobj.write("<system-err>")
    fileobj.write(lameXmlEscape(stderr))
    fileobj.write("</system-err>\n")

    fileobj.write("</testsuite>\n")


class JavaProc:

    # signatures of running processes
    mains = {
        "org.apache.tools.ant.launch.Launcher"                        : "Ant Process",
        "org.voltdb.VoltDB"                                           : "VoltDB server process",
        "org.apache.tools.ant.taskdefs.optional.junit.JUnitTestRunner": "JUnit Test Runner process",
        "org.eclipse.jdt.junit4.runtime"                              : "Eclipse JUnit process",
    }

    def __init__(self, jpsline):
        self.pid, self.classname = (list(jpsline.split()) + ['_unknown_'])[:2]
        self.pid = int(self.pid)

        self.processType = "[unknown java process]"
        if self.classname in JavaProc.mains.keys():
            self.processType = JavaProc.mains[self.classname]

        self.tag = ""
        if self.processType == "VoltDB server process":
            match = re.search("(?<=tag )(\\S)*", jpsline)
            if match:
                self.tag = match.group(0)
        if len(self.tag) > 0:
            self.tag = " (" + self.tag + ")"

    def __str__(self):
        return "%d: %s%s" % (self.pid, self.processType, self.tag)

def getProcs():
    return [JavaProc(line) for line in cmd_readlines("jps -lm")]

def getPathToResultsFileForTest(testname, outputpath):
    return os.path.join(os.path.abspath(outputpath), "TEST-" + testname + ".xml")

if __name__ == "__main__":

    # parse two command line options
    parser = OptionParser()
    parser.add_option("-t", "--testname",
                      action="store", type="string", dest="testname",
                      help="name of test just that ran")
    parser.add_option("-o", "--outputpath",
                      action="store", type="string", dest="outputpath",
                      help="path to JUnit results")
    parser.add_option("-r", "--testresult",
                      action="store", type="string", dest="testresult",
                      help="did the test pass")
    (options, args) = parser.parse_args()

    # if the test failed touch the magic file to indicate this
    if options.testresult == "true":
        magicpath = os.path.join(os.path.abspath(options.outputpath), "JUNITHADFAILURES");
        os.system("touch %s" % (magicpath))

    # get the path to the xml output for this test
    resultPath = getPathToResultsFileForTest(options.testname, options.outputpath)

    logpath = os.path.join(os.path.abspath(options.outputpath), "post-process-log.txt")
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=logpath,level=logging.DEBUG)

    logging.info("Running post processing for test %s" % (options.testname))

    # check for stranded procs
    procs = getProcs()
    # collect stranded procs that shouldn't be stranded (and kill them)
    failures = []
    for proc in procs:
        #print(proc)
        if proc.processType in ["VoltDB server process", "JUnit Test Runner process"]:
            # a test was stranded
            logging.error("Found blacklisted process: %s" % (proc))
            # kill the straggler
            cmd("kill -9 %d" % (proc.pid))
            # memoize it
            failures.append(proc)
        else:
            logging.info("Found java process: %s" % (proc))

    # update the results xml file
    if len(failures) > 0:

        # move the original
        if not os.path.exists(resultPath):
            newpath = resultPath + "-orig.xml"
            logging.error("Moving original results xml file to: %s" % (newpath))
            os.rename(resultPath, newpath)

        # write a new results xml file
        f = open(resultPath, 'w')
        testout = "Test suite stranded some processes"
        result = Result(options.testname, "testPlaceholder", testout)

        procinfo = "Killed processes:\n\n"
        for proc in failures:
            procinfo += str(proc) + "\n"
        procinfo += "\nCommand Lines:\n\n"
        for proc in failures:
            procinfo += str(proc) + "\n"

        writeJUnitXml(f, options.testname, 1, procinfo, "", [result])
    else:
        # if the xml results file doesn't exist, make one
        if not os.path.exists(resultPath):
            logging.error("JUnit test runner didn't create expected results XML file: %s" % (resultPath))
            f = open(resultPath, 'w')
            testout = "JUnitRunner failed to write XML results file for suite"
            result = Result(options.testname, "testPlaceholder", testout)
            writeJUnitXml(f, options.testname, 1, testout, "", [result])

    # truncate the global log file file with this tests name
    junitlogfile = os.path.join(os.path.abspath(options.outputpath), "volt-junit-fulllog.txt")
    newlogpath = os.path.join(os.path.abspath(options.outputpath), "junit.log." + options.testname + ".txt")
    if os.path.exists(junitlogfile):
        shutil.move(junitlogfile, newlogpath)
        logging.info("Truncated and renamed log file")

    logging.info("Finished post processing for test %s" % (options.testname))
