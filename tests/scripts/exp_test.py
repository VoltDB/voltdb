#!/usr/bin/env python2.6

# This file is part of VoltDB.
# Copyright (C) 2008-2013 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import filecmp
import fnmatch
import getpass
import os.path
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib

from collections import defaultdict
from optparse import OptionParser
from subprocess import call # invoke unix/linux cmds
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement
# add the path to the volt python client, just based on knowing
# where we are now
sys.path.append('../../lib/python')
try:
    from voltdbclient import *
except ImportError:
    sys.path.append('./lib/python')
    from voltdbclient import *
from Query import VoltQueryClient
from XMLUtils import prettify # To create a human readable xml file

hostname = socket.gethostname()
pkgName = {'comm': 'LINUX-voltdb',
           'pro': 'LINUX-voltdb-ent'}
pkgDict = {'comm': 'Community',
           'pro': 'Enterprise',
           'all': "Community, Pro"}
suiteDict = {'helloworld': 'HelloWorld',
             'voltcache': 'Voltcache',
             'voltkv': 'Voltkv',
             'voter': 'Voter',
             'json-sessions': 'Json-sessions',
             }
tail = "tar.gz"
# http://volt0/kits/candidate/LINUX-voltdb-2.8.1.tar.gz
# http://volt0/kits/candidate/LINUX-voltdb-ent-2.8.1.tar.gz
root = "http://volt0/kits/branch/"
testname = os.path.basename(os.path.abspath(__file__)).replace(".py", "")
defaultHost = "localhost"
defaultPort = 21212
sectionBreak="====================================================="

def findSectionInFile(srce, start, end):
    flag = 0
    status = False
    ins = open(srce, "r" )
    str = ""
    for line in ins:
        if(flag == 0 and line.find(start) > -1):
            flag = 1
        if(flag == 1):
            str += line
        if(flag == 1 and line.find(end) > -1):
            flag = 0
            status = True
            break
    return (status, str)

# To read a srouce file 'srce' into an array
def readFileIntoArray(srce):
    content = []
    if(os.path.getsize(srce) > 0):
        with open(srce) as f:
            content = f.readlines()
    return content

# The release number can be optionally passed in from cmdline with -r switch
# If it's ommitted at cmdline, then this function is called to get the release
# number from 'version.txt'
def getReleaseNum():
    path = os.path.dirname(os.path.abspath(__file__))
    root = path.replace("tests/scripts", "")
    verFile = root + "version.txt"
    ver = readFileIntoArray(verFile)[0].rstrip()
    return ver

# Always create a fresh new subdir
def createAFreshDir(dir):
    ret = 0
    if os.path.exists(dir):
        shutil.rmtree(dir)
    if not os.path.exists(dir):
        os.makedirs(dir)
    if not os.path.exists(dir):
        ret = -1
    return ret

# To get a VoltDB tar ball file and untar it in a designated place
def installVoltDB(pkg, release):
    info = {}
    info["ok"] = False
    thispkg = pkgName[pkg] + '-' + release + "." + tail
    srce = root + thispkg
    dest = os.path.join('/tmp', thispkg)
    cmd = "wget " + srce + " -O " + dest + " 2>/dev/null"
    print sectionBreak
    print "Getting " + srce
    print "to " +dest

    ret = call(cmd, shell=True)
    if ret != 0 or not os.path.exists(dest):
        info["err"] = "Cannot download '%s'" % srce
        return info

    fsize = os.path.getsize(dest)
    if fsize == 0:
        info["err"] = "The pkg '%s' is blank!" % dest
        return info

    ret = createAFreshDir(workDir)
    if ret != 0:
        info["err"] = "Cannot create the working directory: '%s'" % workDir
        return info
    cmd = "tar zxf " + dest + " -C " + workDir + " 2>/dev/null"
    ret = call(cmd, shell=True)
    if ret == 0:
#        info["dest"] = dest
        info["srce"] = srce
        info["pkgname"] = thispkg
        info["workDir"] = workDir
        info["ok"] = True
    else:
        info["err"] = "VoltDB pkg '%s' installation FAILED at location '%s'" \
            % (dest, workDir)

    if "ent" in thispkg:
        info["license"] = getEnterpriseLicense(workDir, release)
    return info
# end of installVoltDB(pkg, release):


def getEnterpriseLicense(workDir, release):
    print workDir
    print release
    url = root + "license.xml"
    filename = os.path.join(workDir, "voltdb-ent-" + release, "voltdb","license.xml")
    urllib.urlretrieve(url,filename)
    print "Retrieved to " + filename
    return True


# Sample key/val pairs for testSuiteList are:
# key: voltcache,   val: /tmp/<user_name>_exp_test/voltdb-2.8.1/examples/voltcache
# key: voter,       val: /tmp/<user_name>_exp_test/voltdb-2.8.1/examples/voter
# key: voltkv,      val: /tmp/<user_name>_exp_test/voltdb-2.8.1/examples/voltkv
# key: helloworld', val: /tmp/<user_name>_exp_test/voltdb-2.8.1/doc/tutorials/helloworld
def setTestSuite(dname, suites):
    testSuiteList = {}
    #Find all the run.sh in the kit
    for root, dirs, files in os.walk(dname):
        for filename in fnmatch.filter(files,'run.sh'):
            path  = os.path.join(root, filename)
            appdir = os.path.split(os.path.dirname(path))[1]
            if appdir in suites:
                testSuiteList[appdir] = os.path.dirname(path)
    return testSuiteList

def stopPS(ps):
    print ps.returncode
    if ps.returncode != None:
        print "Process %d exited early with return code %d" % (ps.pid, ps.returncode)
    print "Going to kill this process: '%d'" % ps.pid
    ps.kill()

# To return a voltDB client
def getQueryClient(timeout=60):
    host = defaultHost
    port = defaultPort
    client = None
    endtime = time.time() + timeout
    while (time.time() < endtime):
        try:
            client = VoltQueryClient(host, port)
            client.set_quiet(True)
            client.set_timeout(5.0) # 5 seconds
            return client
        except socket.error:
            time.sleep(1)

    if client == None:
        print >> sys.stderr, "Unable to connect python client to server after %d seconds" % timeout
        sys.stderr.flush()
        return None

    return client

# Not used yet.
# Currently, both startService() and stopService() are implemented in
# execThisService(). However, if we wanted to run certain queries before
# shutdown VoltDB, we have to separate startService() and stopService(),
# so that we can add more implementations in between.
def startService(service, logS, logC):
    cmd = service + " > " + logS + " 2>&1"
    service_ps = subprocess.Popen(cmd, shell=True)
    time.sleep(2)
    client = getQueryClient()
    if not client:
        return None
    cmd = service + " client > " + logC + " 2>&1"
    ret = call(cmd, shell=True)
    print "returning results from service execution: '%s'" % ret
    time.sleep(1)
    return (service_ps, client)

# Not used yet. Refer to the comments for startService()
def stopService(ps, serviceHandle):
    serviceHandle.onecmd("shutdown")
    ps.communicate()

# To execute 'run.sh' and save the output in logS
# To execute 'run.sh client' and save the output in logC
def execThisService(service, logS, logC):
    cmd = service + " > " + logS + " 2>&1"
    print "   Server - Exec CMD: '%s'" % cmd
    service_ps = subprocess.Popen(cmd, shell=True)
    client = getQueryClient(timeout=90)
    service_ps.poll()
    if service_ps.returncode or not client:
        #TODO - write something in log
        print "   Server returned an error"
        stopPS(service_ps)
        return
    cmd = service + " client > " + logC + " 2>&1"
    print "   Client - Exec CMD: '%s'" % cmd
    ret = call(cmd, shell=True)
    print "   Returning results from service execution: '%s'" % ret
    client.onecmd("shutdown")
    service_ps.communicate()

# Further assertion is required
# We want to make sure that logFileC contains several key strings
# which is defined in 'staticKeyStr'
def assertvoltcache(mod, logC):
    return assertvoltkv(mod, logC)

def assertvoltkv(mod, logC):
    staticKeyStr = {
"Command Line Configuration":1,
"Setup & Initialization":1,
"Starting Benchmark":1,
"KV Store Results":1,
"Client Workload Statistics":1,
    }

    dynamicKeyStr = {}
    with open(logC) as f:
        content = f.readlines()

    cnt = 0
    for line in content:
        x = line.strip()
        dynamicKeyStr[x] = 1
        if x in staticKeyStr.keys():
            cnt += 1

    result = False
    msg = None
    keys = {}
    if(cnt == len(staticKeyStr)):
        msg = "The client output has all the expected key words"
        keys = staticKeyStr
        result = True
    else:
        result = False
        msg = "The client output does not have all the expected key words"
        for key in staticKeyStr:
            if key not in dynamicKeyStr.keys():
                keys[key] = key

    return (result, msg)

# We want to make sure that logFileC contains this KEY string:
# The Winner is: Edwina Burnam
def assertvoter(mod, logC):
    result = False
    aStr = "Voting Results"
    expected = "The Winner is: Edwina Burnam"
    # The 'section' returned by findSectionInFile is not used here,
    # However, this piece of info could be used by something else
    # which calls findSectionInFile().
    (result, section) = findSectionInFile(logC, aStr, expected)
    if(result == False):
        expected = "ERROR: The Winner is NOT Edwina Burnam!"
        # Again, 'section' is not used in this implementation
        # section += "\n\n" + expected

    # It could return 'section' in some other implementation
    # that calls findSectionInFile()
    return (result, expected)

# We want to make sure that logFileC contains this KEY string:
# The Winner is: Edwina Burnam
def assertjson(mod, logC):
    #Ordered list of things to find - make sure all queries found results
    patterns = [
        'A total of \d+ login requests were received',
        'SELECT username, json_data FROM user_session_table ORDER BY username LIMIT 10',
        'user-0      {"role":"\w+","site":"\w+","props":{"last-login":"\d+"}}'
        'SELECT username, json_data FROM user_session_table ORDER BY username LIMIT 10',
        'SELECT username, json_data FROM user_session_table WHERE field(json_data, \'site\')=\'VoltDB Forum\' ORDER BY username LIMIT 10',
        'SELECT username, json_data FROM user_session_table WHERE field(json_data, \'site\')=\'VoltDB Forum\' AND field(json_data, \'moderator\')=\'true\' ORDER BY username LIMIT 10;',
        'SELECT username, json_data FROM user_session_table WHERE field(field(json_data, \'props\'), \'download_version\')=\'v3.0\' and field(field(json_data, \'props\'), \'client_language\')=\'Java\' ORDER BY username LIMIT 10',
        'SELECT username, json_data FROM user_session_table WHERE field(field(json_data, \'props\'), \'download_version\') LIKE \'v2%\' ORDER BY username LIMIT 10',
        'SELECT json_data FROM user_session_table WHERE username=\'voltdb\'',
        'SELECT json_data FROM user_session_table WHERE username=\'voltdb\'',
        ]

    match_ct = 0
    with open (logC,'r') as f:
        for line in f:
            if re.match(patterns[match_ct]):
                match_ct += 1
                print "Found match %d" % match_ct
            if match_ct > len(patterns):
                break

    print "Found match %d" % match_ct



    # It could return 'section' in some other implementation
    # that calls findSectionInFile()
    return (result, expected)

# To make sure that we see the key string 'Hola, Mundo!'
def asserthelloworld(modulename, logC):
ls -l    expected = "Hola, Mundo!"
    buf = readFileIntoArray(logC)
    for line in buf:
        if(expected == line.rstrip()):
            msg = expected
            result = True
            break
    else:
        msg = "Expected '%s' for module '%s'. Actually returned: '%s'" % (expected, modulename, buf)
        result = False
    return (result, msg)

# To make sure the content of logC which is the output of 'run.sh client'
# is identical to the static baseline file.
# If True, the test is PASSED
# If False, then we need to parse the LogC more carefully before we declare
# this test is FAILED
def assertClient(e, logC):
    baselineD = origDir + "/plannertester/baseline/"
    baselineF = baselineD + e + "/client_output.txt"
#    print "baselineD = '%s', baselineF = '%s'" % (baselineD, baselineF)
    ret = False
    msg = None
    if(os.path.exists(baselineF)):
        ret = filecmp.cmp(baselineF, logC)
        if(ret == True):
            msg = "The client output matches the baseline:"
        else:
            msg = "Warning!! The client output does NOT match the baseline:"
        msg += "\nBaseline: %s" % baselineF
        msg += "\nThe client output: %s" % logC
    else:
        msg = "Warning!! Cannot find the baseline file:\n%s" % baselineF
    return (ret, msg)

def startTest(testSuiteList):
    statusBySuite = {}
    msgBySuite = {}
    keyWordsBySuite = {}
    msg = ""
    result = False
    # testSuiteList is a dictionary whose keys are test suite names, e.g. helloworld,
    # voter, voltkv, & voltcache and the corresponding values are paths where the
    # executable run.sh is in. Note that all run.sh can only be invoked as './run.sh
    # by design.
    for (suiteName, path) in testSuiteList.iteritems():
        keyStrSet = None
        os.chdir(path)
        currDir = os.getcwd()
        service = './run.sh'
        print ">>> Test: %s" % suiteName
        print "   Current Directory: '%s'" % currDir
        logFileS = os.path.join(logDir, suiteName + "_server")
        logFileC = os.path.join(logDir,suiteName + "_client")
        print "   Log File for VoltDB Server: '%s'" % logFileS
        print "   Log File for VoltDB Client: '%s'" % logFileC
        execThisService(service, logFileS, logFileC)
        #Check the results
        (result,msg) = globals()["assert" + suiteName.split('-')[0]](suiteName, logFileC)

        statusBySuite[suiteName] = result
        msgBySuite[suiteName] = msg
        keyWordsBySuite[suiteName] = keyStrSet

        os.chdir(origDir)
    # end of for e in testSuiteList:
    return (statusBySuite, msgBySuite, keyWordsBySuite)
# end of startTest(testSuiteList):

# status, msg, & keyStrings are all 2-D dictionaries, which have the same keys with
# different values.
# First level keys: module name, e.g. comm, pro, voltkv, voltcache
# Second level keys: suite name, e.g. helloworld, voter, voltkv, voltcache
# Values for status: True or False, which is the testing status for this suite in this package
# Values for msg: A descriptive testing message for this suite in this package
# Values for keyStrings: Only applied for package 'voltkv' & 'voltcache'. If a test suite is
#                        failed for package either 'voltkv' or 'voltcache', the final report
#                        will display a list missing strings that are expected in log files
#                        for client.
def create_rpt(info, status, msg, keyStrings, elapsed, rptf):
    testtime = "%.2f" % elapsed
    testsuites = Element('testsuites', {'time':testtime})
    for (mod, suiteNameDict) in status.iteritems():
        testsuite = SubElement(testsuites, 'testsuite',
                {'package':info["pkgname"],'URL':info["srce"],
                 'hostname':hostname, 'name':pkgDict[mod]})
        for (suitename, status4ThisSuite) in suiteNameDict.iteritems():
            failureCnt = "0"
            errCnt = "0"
            if(status4ThisSuite == False):
                failureCnt = "1"
            else:
                failureCnt = "0"

            print "==-->>Package Name: '%s', Suite Name: '%s', Status = '%s'" \
                % (mod, suitename, status4ThisSuite)
            if(info["ok"] == False):
                errCnt = "1"
            else:
                errCnt = "0"
            testcase = SubElement(testsuite, 'testcase',
                {'errors':errCnt,'failures':failureCnt, 'name':suitename})

            if(failureCnt == "1"):
                failure = SubElement(testcase, 'failure',
                        {'Message':msg[mod][suitename]})
                misStr = None
                if(keyStrings[mod][suitename] != None):
                    for j in keyStrings[mod][suitename]:
                        if(misStr == None):
                            misStr = j
                        else:
                            misStr += ", " + j
                    missing = SubElement(failure, 'Missing',
                            {'MissingString':misStr})
            else:
                failure = SubElement(testcase, 'info',
                        {'Message':msg[mod][suitename]})
            if(errCnt == "1"):
                error = SubElement(testcase, 'error',
                        {'Error':info["err"]})

    fo = open(rptf, "wb")
    fo.write(prettify(testsuites))
    fo.close()
    if not os.path.exists(rptf):
        reportfile = None
    return rptf

if __name__ == "__main__":
    start = time.time()
    usage = "Usage: %prog [options]"
    parser = OptionParser(usage="%prog [-b <branch name>] [-r <release #>] [-p <comm|pro|voltkv|voltcache|all> <-s all|helloworld|voter|voltkv|voltcache>]", version="%prog 1.0")
    parser.add_option("-r", "--release", dest="release",
                      help="VoltDB release number. If omitted, will be read from version.txt.")
    parser.add_option("-p", "--package", dest="pkg",
                      help="VoltDB package type: comm, pro, voltkv or voltcache. Default is comm. If not set, then this framework will take all packages.")
    parser.add_option("-s", "--suite", dest="suite",
                      help="Test suite name, if not set, then this framework will take all suites. If an incorrect suite name is passed in, then the test suite name is set to 'all' as a default value.")
    parser.add_option("-x","--reportxml", dest="reportfile", default="exp_test.xml",
                      help="Report file location")
    parser.add_option("-b","--branch", dest="branch", default="master",
                      help="Branch name to test")
    parser.add_option("-o","--output", dest="destDir", default='/tmp',
                      help="Output Directory")



    parser.set_defaults(pkg="all")
    parser.set_defaults(suite="all")
    (options, args) = parser.parse_args()
    destDir = options.destDir
    logDir = os.path.join(os.getcwd(), getpass.getuser() + "_" + testname + '_log')
    workDir = os.path.join(destDir,getpass.getuser() + "_" + testname)

    if not os.path.exists(logDir):
        os.makedirs(logDir)

    if options.suite == 'all':
        suites = suiteDict.keys()
    else:
        suites = options.suite

    origDir = os.getcwd()

    releaseNum = options.release
    if(releaseNum == None):
        releaseNum = getReleaseNum()

    branchName = options.branch
    root = root.replace("branch", branchName)

    list = None
    if(options.pkg in pkgDict):
        print sectionBreak
        print "Testing Branch in this RUN: %s" % branchName
        print "Testing Version in this RUN: %s" % releaseNum
        print "--------------------------------------"
        if(options.pkg == "all"):
            list = pkgName.keys()
            print "Testing all packages in this RUN:"
            print "---------------------------------"
            for item in pkgName:
                pkgFullName = pkgName[item] + '-' + releaseNum + "." + tail
                print "%s - %s" % (pkgDict[item], pkgFullName)
        else:
            list = [options.pkg]
            pkgFullName = pkgName[options.pkg] + '-' + releaseNum + "." + tail
            print "Testing this package only in this RUN:"
            print "--------------------------------------"
            print "%s - %s" % (pkgDict[options.pkg], pkgFullName)
    else:
        print "Unknown package name passed in from cmdline: %s" % options.pkg
        print "Select from: " + ', '.join((pkgDict.keys()))
        exit(1)

    tf = msg = keys = None
    tfD = defaultdict(dict)
    msgD = defaultdict(dict)
    keysD = defaultdict(dict)

    for p in list:
        ret = installVoltDB(p, releaseNum)
        if not ret["ok"]:
            print "Error!! %s" % ret["err"]
            exit(1)
        print suites
        testSuiteList = setTestSuite(ret["workDir"], suites)

        (tf, msg, keys) = startTest(testSuiteList)
        tfD[p] = tf
        msgD[p] = msg
        keysD[p] = keys

    status = True
    # tfD is a 2-D dictionary.
    # First level keys: module name, e.g. comm, pro, voltkv, voltcache
    # Second level keys: suite name, e.g. helloworld, voter, voltkv, voltcache
    # Values: True or False, which is the testing status for this suite in this package
    for (module, suiteNameDict) in tfD.iteritems():
        for (suitename, status4ThisSuite) in suiteNameDict.iteritems():
            if not status4ThisSuite: # status4ThisSuite == tfD[module][suitename]:
                status = False
                print >> sys.stderr, "The test suite '%s' in '%s' package is FAILED \
                    \n'%s'\n%s" \
                    % (suitename, module, msgD[module][suitename], sectionBreak)
    elapsed = (time.time() - start)
    reportXML = create_rpt(ret, tfD, msgD, keysD, elapsed, options.reportfile)
    print "Refer to the final report '%s' for details." % reportXML
    print "Total time consumed in this run: '%.2f'" % elapsed
    if(status == False):
        print "\nAt least one test suite is Failed!!\n"
        exit(1)
    print "######################"
    print "All tests are PASSED!!"
    print "######################"
    for p in msgD:
        for suitename in msgD[p]:
            print "%s - %s -> %s" % (pkgDict[p], suiteDict[suitename], msgD[p][suitename])
    exit(0)
