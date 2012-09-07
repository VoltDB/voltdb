#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
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

import sys
import os.path
import shutil
import fnmatch
import subprocess
import time
from optparse import OptionParser
from subprocess import call # invoke unix/linux cmds
# add the path to the volt python client, just based on knowing
# where we are now
sys.path.append('../../src/py_client')
try:
    from voltdbclient import *
except ImportError:
    sys.path.append('./src/py_client')
    from voltdbclient import *
from Query import VoltQueryClient
#
#import random
#import cPickle
#import imp
#import re
#from SQLCoverageReport import generate_summary
#from SQLGenerator import SQLGenerator
#from xml.etree import ElementTree
#from xml.etree.ElementTree import Element, SubElement
#from XMLUtils import prettify # To create a human readable xml file

pkgName = {'comm': 'LINUX-voltdb', 'pro': 'LINUX-voltdb-ent'}
tail = "tar.gz"
# http://volt0/kits/candidate/LINUX-voltdb-2.8.1.tar.gz
# http://volt0/kits/candidate/LINUX-voltdb-ent-2.8.1.tar.gz
root = 'http://volt0/kits/candidate/'
destDir = "/tmp/"
elem2Test = {'helloworld':'run.sh', 'voltcache':'run.sh', 'voltkv':'run.sh', 'voter':'run.sh'}
defaultHost = "localhost"
defaultPort = 21212

def findSectionInFile(srce, start, end):
    flag = 0
    status = False
    ins = open(srce, "r" )
    array = []
    for line in ins:
        if(line.find(start) > -1):
            flag = 1
        if(flag == 1):
            array.append( line )
        if(line.find(end) > -1):
            flag = 0
            status = True
    return (status, array)

def readFirstLine(srce):
    firstline = None
    if(os.path.getsize(srce) > 0):
        with open(srce) as f:
            content = f.readlines()
        firstline = content[0].rstrip()
        print "firstline = '%s'" % firstline
    return firstline

def getReleaseNum():
    cwd = os.path.realpath(__file__)
    path = os.path.dirname(os.path.abspath(__file__))
    root = path.replace("tests/scripts", "")
#    print "In getReleaseNum path = '%s'" % path
#    print "In getReleaseNum root = '%s'" % root
    verFile = root + "version.txt"
#    print "In getReleaseNum verFile = '%s'" % verFile
    ver = readFirstLine(verFile)
    return ver

def createDir(dir):
    ret = 0
    if os.path.exists(dir):
        shutil.rmtree(dir)
    if not os.path.exists(dir):
        os.makedirs(dir)
    if not os.path.exists(dir):
        ret = -1
    return ret

def installVoltDB(pkg, release):
    if(pkg not in pkgName):
        print "Invalid pkg name!"
        exit(1)

    if(release == None):
        release = getReleaseNum()

    pkgname = pkgName[pkg] + '-' + release + "." + tail
    srce = root + pkgname
    dest = destDir + pkgname
    cmd = "wget " + srce + " -O " + dest + " 2>/dev/null"

#    print "In installVoltDB, root = '%s', release = '%s'" % (root, release)
#    print "In installVoltDB, pkgname = '%s'" % (pkgname)
#    print "In installVoltDB, srce = '%s'" % (srce)
    ret = call(cmd, shell=True)
#    print "In installVoltDB, cmd = '%s'" % (cmd)
#    print "In installVoltDB, ret = '%s'" % (ret)
    info = {}
    info['ok'] = False
    if ret == 0 and os.path.exists(dest):
        fsize = os.path.getsize(dest)
        if fsize > 0:
#           print "File '%s' exists!! Filesize = '%d'" % (dest, fsize)
            ret = createDir(workDir)
            if ret == 0:
                cmd = "tar zxf " + dest + " -C " + workDir + " 2>/dev/null"
                print "cmd = '%s'" % cmd
                ret = call(cmd, shell=True)
                if ret == 0:
                    info['dest'] = dest
                    info['srce'] = srce
                    info['pkgname'] = pkgname
                    info['workDir'] = workDir
                    info['ok'] = True
    return info
# end of installVoltDB(pkg, release):

def setExecutables(dname, suite):
    testSuiteList = {}
    for dirname, dirnames, filenames in os.walk(dname):
        for subdirname in dirnames:
            if subdirname in elem2Test.keys():
                path = os.path.join(dirname, subdirname)
                run_sh = path + "/" + elem2Test[subdirname]
                if(os.access(run_sh, os.X_OK)):
                    if(suite != "all"):
                        if(path.find(suite) > -1):
#                            testSuiteList[suite] = run_sh
                            testSuiteList[suite] = path
                    else:
                        if(path.find(subdirname) > -1):
#                            testSuiteList[subdirname] = run_sh
                            testSuiteList[subdirname] = path
    return testSuiteList

def stopPS(ps):
    print "Going to kill this process: '%d'" % ps.id
    killer = subprocess.Popen("kill -9 %d" % (ps.pid), shell = True)
    killer.communicate()
    if killer.returncode != 0:
#        print >> sys.stderr, "Failed to kill the server process %d" % (server.pid)
        print "Failed to kill the server process %d" % (ps.pid)

def getClient():
    host = defaultHost
    port = defaultPort
    client = None
    for i in xrange(10):
        try:
            client = VoltQueryClient(host, port)
            client.set_quiet(True)
            client.set_timeout(5.0) # 5 seconds
            break
        except socket.error:
            time.sleep(1)

    if client == None:
        print >> sys.stderr, "Unable to connect/create client"
        sys.stderr.flush()
        exit(1)

    return client

def runVoter(service, logS, logC):
    print "In runVoter(), service = '%s'" % service
    (ps, serviceHandle) = startService(service, logS, logC)

    print "serviceHandle's type: '%s'" % type(serviceHandle)
    serviceHandle.onecmd("adhoc select count(*) from votes")
    time.sleep(41)
    stopService(ps, serviceHandle)

def runHelloWorld(service, logS, logC):
    print "In runHelloWorld(), service = '%s'" % service
    (ps, serviceHandle) = startService(service, logS, logC)
    stopService(ps, serviceHandle)

def startService(service, logS, logC):
    service_ps = subprocess.Popen(service + " > " + logS + " 2>&1", shell=True)
    time.sleep(2)
    client = getClient()
#    client_ps = subprocess.Popen(service + " client", shell = True)
    ret = call(service + " client > " + logC + " 2>&1", shell=True)
    print "returning results from service execution: '%s'" % ret
    time.sleep(1)
    return (service_ps, client)

def stopService(ps, serviceHandle):
    serviceHandle.onecmd("shutdown")
    ps.communicate()

def execThisService(service, logS, logC):
    service_ps = subprocess.Popen(service + " > " + logS + " 2>&1", shell=True)
    time.sleep(2)
    client = getClient()
    ret = call(service + " client > " + logC + " 2>&1", shell=True)
    print "returning results from service execution: '%s'" % ret
    client.onecmd("shutdown")
    service_ps.communicate()

#def assertVotecache(logC):
# Command Line Configuration
# Setup & Initialization
#Starting Benchmark
# KV Store Results
# Client Workload Statistics
# System Server Statistics

def assertVotekv_Votecache(logC):
    keyStr = {
"Command Line Configuration":1,
"Setup & Initialization":1,
"Starting Benchmark":1,
"KV Store Results":1,
"Client Workload Statistics":1,
"System Server Statistics":1,
    }
    with open(logC) as f:
        content = f.readlines()
    print "content length: '%d'" % len(content)
    cnt = 0
    for line in content:
        x = line.strip()
        if x in keyStr.keys():
#            print "==-->>x = '%s'" % x
            cnt += 1

#    print "keyStr length: '%d', cnt = '%d'" % (len(keyStr), cnt)
    result = False
    if(cnt == len(keyStr)):
        result = True
    return result

def assertVoter(logC):
    result = False
    aStr = "Voting Results"
    expected = "The Winner is: Edwina Burnam"
    (result, section) = findSectionInFile(logC, aStr, expected)
    print "In assertVoter: expected = '%s', section length: '%s'" % (expected, len(section))
    return result

def assertHelloWorld(logC):
    result = False
    expected = "Hola, Mundo!"
    actual = readFirstLine(logC)
    print "In assertThis: expected = '%s', actual = '%s'" % (expected, actual)
    if(expected == actual):
        result = True
    return result

def startTest(testSuiteList):
    retCode = {}
    for e in testSuiteList:
        os.chdir(testSuiteList[e])
        currDir = os.getcwd()
        service = elem2Test[e]
        print "currDir: '%s', service: '%s'" % (currDir, service)
        logFileS = "/tmp/" + e + "_server"
        logFileC = "/tmp/" + e + "_client"
        print "logFileS = '%s', logFileC = '%s'" % (logFileS, logFileC)
#        execThisService(service, logFileS, logFileC)
        if(e == "helloworld"):
#            runHelloWorld(service, logFileS, logFileC)
            result = assertHelloWorld(logFileC)
            retCode[e] = result
        elif(e == "voter"):
#            runVoter(service, logFileS, logFileC)
            result = assertVoter(logFileC)
            retCode[e] = result
        elif(e == "voltkv" or e == "voltcache"):
#            execThisService(service, logFileS, logFileC)
            result = assertVotekv_Votecache(logFileC)
            retCode[e] = result
        else:
            print "e = '%s' ==-->> to be implemented..." % e
            retCode[e] = False

#        print "origDir = '%s'" % origDir
        os.chdir(origDir)
#        currDir = os.getcwd()
#        print "should be back to the original dir: '%s'" % (currDir)
    # end of for e in testSuiteList:
    return retCode

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-r", "--release", dest="release",
                      help="VoltDB release no. If ommitted, it will find it from version.txt.")
    parser.add_option("-p", "--pkg", dest="pkg",
                      help="VoltDB package type: Community or Pro. Defalut is Community.")
    parser.add_option("-s", "--suite", dest="suite",
                      help="Test suite name, if not set, then take all suites")

    parser.set_defaults(pkg="comm")
    parser.set_defaults(suite="all")
    (options, args) = parser.parse_args()
    workDir = destDir + os.path.basename(os.path.abspath(__file__)).replace(".py", "")

    print "============================================================"
#    print "argv[0] = '%s'" % (sys.argv[0])
    print "options = '%s'" % options
    print "workDir = '%s'" % workDir
    print "options.release = '%s' options.pkg = '%s'" % (options.release, options.pkg)
    suite = options.suite
    if suite not in elem2Test.keys() and suite != "all":
        print "Warning: unknown suite name - '%s'" % suite
        print "Info: So we're going to cover all test suites in this run"

    origDir = os.getcwd()

    ret = installVoltDB(options.pkg, options.release)

    success = ret['ok']
    if not success:
        print "After installVoltDB() Test Failed!!"
#        print >> sys.stderr, "Test Failed!!"
        exit(1)

    testSuiteList = setExecutables(ret['workDir'], suite)
    for e in testSuiteList:
        executable = testSuiteList[e]
        print "==-->>elem: '%s', executable: '%s'" % (e, executable)

    success = startTest(testSuiteList)
    for k in success:
        if not success[k]:
#            print >> sys.stderr, "Test '%s' Failed!!" % k
            print "Test '%s' Failed!!" % k
#        exit(1)
        else:
            print "Test '%s' passed!!" % k
