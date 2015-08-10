#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2015 VoltDB Inc.
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

from multiprocessing import *
from datetime import *
import os, platform, sys, subprocess, re
import glob, pdb

output = {
            "Hostname" : "Unable to gather information",
            "OS" : "Unable to gather information",
            "OS release" : "Unable to gather information",
            "ThreadCount" : "Unable to gather information",
            "64 bit" : "Unable to gather information",
            "Memory" : "Unable to gather information"
          }

osName=""
threadCount=""
osRelease=""
osReleaseTest=""

def detectOSRelease():
    if isHostLINUX():
        osName = "Linux"
        output['OS'] = ["PASS", osName]
        # support for specific ubuntu centos
        osRelease = subprocess.Popen("lsb_release -d | tr -s '\t' ' ' | cut -d' ' -f2- | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()
        release = re.search("\s\d+(\.\d+)*\s", osRelease).group(0).strip()
        if "centos" in osRelease.lower():
            if float(release) >= 6.3:
                output['OS release'] = ["PASS", osRelease]
            else:
                output['OS release'] = ["WARN", "Unsupported release: " + osRelease]
        elif "ubuntu" in osRelease.lower():
            if '10.04' in release or '12.04' in release or '14.04' in release:
                output['OS release'] = ["PASS", osRelease]
            else:
                output['OS release'] = ["WARN", "Unsupported release: " + osRelease]
        elif "red hat" in osRelease.lower() or "rhel" in osRelease.lower():
            if float(release) >= 6.3:
                output['OS release'] = ["PASS", osRelease]
            else:
                output['OS release'] = ["WARN", "Unsupported release: " + osRelease]
        else:
            output['OS release'] = ["WARN", "Unsupported release: " + osRelease]
    else:
        osName=["WARN", "Only supports Linux based platforms"]
        osRelease = ["WARN","Support distributions are Ubuntu 10.04/12.04/14.04 and RedHat/CentOS 6.3 or later"]

def detectHostname():
    output['Hostname'] = ["", subprocess.Popen("hostname | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()]

def detectThreadCount():
    output['ThreadCount'] = [" ", subprocess.Popen("cat /proc/cpuinfo | grep -c processor | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()]

def detect64bitOS():
    if os.system("which uname >/dev/null 2>&1") == 0 and subprocess.Popen("uname -a | grep -c 'x86_64'", stdout=subprocess.PIPE, shell=True).stdout.read() > 0:
    #if os.system("uname -a | grep 'x86_64'"):
        output['64 bit'] = ["PASS", subprocess.Popen("uname -a | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()]
    else:
        output['64 bit'] = ["FAIL", "64-bit Linux-based operating system is required to run VoltDB"]      

def detectMemory():
    hostMemory = subprocess.Popen("free | grep 'Mem' | tr -s ' ' | cut -d' ' -f2 ", stdout=subprocess.PIPE, shell=True).stdout.read()
    if int(hostMemory) >= 4194304:
        output['Memory'] = ["PASS", str(hostMemory).rstrip('\n')]
    else:
        output['Memory'] = ["WARN", "Recommended memory is at least 4194304 kB but was " + str(hostMemory).rstrip('\n') + " kB"]
    if int(hostMemory) >= 67108864:
        memMaxMap = subprocess.Popen("cat /proc/sys/vm/max_map_count | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()
        if int(memMaxMap) >= 1048576:
            output['MemoryMapCount'] = ["PASS", "Virtual memory max map count is " + memMaxMap]
        else:
            output['MemoryMapCount'] = ["WARN", "Virtual memory max map count is " + memMaxMap +
                                        ", recommended is 1048576 for system with 64 GB or more memory"]
    

def detectNTP():
    numOfRunningNTP = subprocess.Popen("ps -ef | grep 'ntpd ' | grep -cv grep | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()
    returnCodeForNTPD = os.system("which ntpd >/dev/null 2>&1")
    if numOfRunningNTP == '1':
        output['NTP'] = ["PASS", "NTP is installed and running"]
        return
    elif numOfRunningNTP  == '0':
        if returnCodeForNTPD == 0:
            output['NTP'] = ["WARN", "NTP is installed but not running"]
        else:
            output['NTP'] = ["WARN", "NTP is not installed or not in PATH enviroment"]
    else:     
        output['NTP'] = ["WARN", "More then one NTP service is running"]

def detectJavaVersion():
    javaVersion = subprocess.Popen("java -version 2>&1 | grep 'java '", stdout=subprocess.PIPE, shell=True).stdout.read()
    javacVersion = subprocess.Popen("javac -version 2>&1", stdout=subprocess.PIPE, shell=True).stdout.read()
    if '1.7.' in javaVersion or '1.8.' in javaVersion:
        if '1.7.' in javacVersion:
            output['Java'] = ["PASS", javaVersion.strip() + ' ' + javacVersion.strip()]
        elif '1.8.' in javacVersion:
            output['Java'] = ["FAIL", "VoltDB can not be compiled with Java8, " + javacVersion]
        else:
            output['Java'] = ["FAIL", "Unsupported Javac version detected, " + javacVersion]
    else:
        output['Java'] = ["FAIL", "Please check if Java has been installed properly."]

def detectPythonVersion():
    pythonVersion = "Python " + str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '.' + str(sys.version_info[2])
    if sys.version_info[0] == 2 and sys.version_info[1] < 6:
        for dir in os.environ['PATH'].split(':'):
            for name in ('python2.7', 'python2.6'):
                path = os.path.join(dir, name)
                if os.path.exists(path):
                    pythonVersion = subprocess.Popen(path + " --version", stdout=subprocess.PIPE, shell=True).stdout.read()
                    output['Python'] = ["PASS", pythonVersion]
                    return
        output['Python'] = ["FAIL", "VoltDB requires Python 2.6 or newer."]
    else:
        output['Python'] = ["PASS", pythonVersion]

def detectTransparentHugePages():
    thp_filenames = glob.glob("/sys/kernel/mm/*transparent_hugepage/enabled")
    thp_filenames += glob.glob("/sys/kernel/mm/*transparent_hugepage/defrag")
    enableTHP = ""
    for filename in thp_filenames:
        with file(filename) as f:
            if '[always]' in f.read():
                enableTHP += filename + ", "
    if len(enableTHP) == 0:
        output['TransparentHugePage'] = ["PASS", "Transparent huge pages not set to always"]
    else:
        output['TransparentHugePage'] = ["FAIL", "Following transparent huge pages set to always: " + enableTHP[:-2]]
        
def detectSwapoff():
    swaponFiles = subprocess.Popen("cat /proc/swaps", stdout=subprocess.PIPE, shell=True).stdout.read().split('\n')[1:-1]
    if len(swaponFiles) == 0:
        output['Swapoff'] = ["PASS", "Swap is off"]
    else:
        swaponOut = []
        for x in swaponFiles:
            if len(x.split()) != 0:
                swaponOut.append(x.split()[0])
        output['Swapoff'] = ["WARN", "Swap is enabled for the filenames: " + ' '.join(swaponOut)]

def detectSwappiness():
    swappiness = subprocess.Popen("cat /proc/sys/vm/swappiness | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()
    if int(swappiness) == 0:
        output['Swappiness'] = ["PASS", "Swappiness is set to 0"]
    else:
        output['Swappiness'] = ["WARN", "Swappiness is set to " + swappiness]
        
def detectMemoryOvercommit():
    memOvercommit = subprocess.Popen("cat /proc/sys/vm/overcommit_memory | tr -d '\n'", stdout=subprocess.PIPE, shell=True).stdout.read()
    if int(memOvercommit) == 1:
        output['MemoryOvercommit'] = ["PASS", "Virtual memory overcommit is enabled"]
    else:
        output['MemoryOvercommit'] = ["WARN", "Virtual memory overcommit is disabled"]

def detectSegmentationOffload():
    tokenDevs = subprocess.Popen("ip link | grep -B 1 ether", stdout=subprocess.PIPE, shell=True).stdout.read().split('\n')[:-1][0::2]
    for x in tokenDevs:
        dev = x.split(':')[1].strip()
        offload = "ethtool --show-offload " + dev
        if subprocess.Popen(offload + " | grep 'tcp-segmentation-offload:'", stdout=subprocess.PIPE, shell=True).stdout.read().split()[1] == "off":
            output['TCPSegOffload_'+dev] = ["PASS", "TCP segmentation offload for " + dev + " is disabled"]
        else:
            output['TCPSegOffload_'+dev] = ["WARN", "TCP segmentation offload is recommended to be disabled, but is currently enabled for " + dev]
        if subprocess.Popen(offload + " | grep 'generic-receive-offload:'", stdout=subprocess.PIPE, shell=True).stdout.read().split()[1] == "off":
            output['GenRecOffload_'+dev] = ["PASS", "Generic receive offload for " + dev + " is disabled"]
        else:
            output['GenRecOffload_'+dev] = ["WARN", "Generic receive offload is recommended to be disabled, but is currently enabled for " + dev]

def isHostLINUX():
    if platform.system() == 'Linux':
        return True
    else:
        return False

def displayResults():
    fails = 0
    warns = 0
    for key,val in sorted(output.items()):
        if val[0] == "FAIL":
            fails += 1
        elif val[0] == "WARN":
            warns += 1
        print "Stats: %-25s %-8s %-9s" % ( key, val[0], val[1] )
    if fails > 0:
        print "\nCheck FAILED. Please review."
    elif warns > 0:
        print "\nCheck completed with " + str(warns) + " WARNINGS."
    else:
        print "\nCheck completed successfully."

@VOLT.Command(
    description = 'Check system properties.'
)
def check(runner):
    systemCheck()

def systemCheck():
    detectHostname()
    detect64bitOS()
    detectThreadCount()
    detectOSRelease()
    detectMemory()
    detectNTP()
    detectJavaVersion()
    detectPythonVersion()
    detectTransparentHugePages()
    detectSwapoff()
    detectSwappiness()
    detectMemoryOvercommit()
    detectSegmentationOffload()
    
    displayResults()
