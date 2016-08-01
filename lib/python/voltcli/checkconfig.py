# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import platform
import subprocess
import sys
import os.path
import os
from voltcli import utility

# Helper functions

def _check_thp_file(file_pattern, file_prefix):
    filename = file_pattern.format(file_prefix)
    if not os.path.isfile(filename):
        return None
    with file(filename) as f:
        if '[always]' in f.read():
            return True
    return False

def _check_thp_files(file_prefix):
    res = _check_thp_file("/sys/kernel/mm/{0}transparent_hugepage/enabled", file_prefix)
    if res is not False:
        return res
    return bool(_check_thp_file("/sys/kernel/mm/{0}transparent_hugepage/defrag", file_prefix))

def _check_thp_config():
    """ Returns None unless THP is configured to [always], in which case it returns an error
        string describing how to disable this unsupported kernel feature
    """
    file_prefix = "redhat_"
    has_error = _check_thp_files(file_prefix)
    if has_error is None:
        file_prefix = ""
        has_error = bool(_check_thp_files(file_prefix))
    if has_error:
        return "The kernel is configured to use transparent huge pages (THP). " \
            "This is not supported when running VoltDB. Use the following commands " \
            "to disable this feature for the current session:\n\n" \
            "sudo bash -c \"echo never > /sys/kernel/mm/{0}transparent_hugepage/enabled\"\n" \
            "sudo bash -c \"echo never > /sys/kernel/mm/{0}transparent_hugepage/defrag\"\n\n" \
            "To disable THP on reboot, add the preceding commands to the /etc/rc.local file.".format(file_prefix)

def _check_segmentation_offload():
    """ Returns a list mapping devices to tcp-segmentation-offload and
        generic-receive-offload settings
    """
    results = []
    tokenDevs = subprocess.Popen("ip link | grep -B 1 ether", stdout=subprocess.PIPE, shell=True).stdout.read().split('\n')[:-1][0::2]
    for d in map(lambda x: x.split(':'), tokenDevs):
        if len(d) < 2:
            continue
        dev = d[1].strip()
        tcpSeg = False
        genRec = False
        features = subprocess.Popen("ethtool --show-offload " + dev, stdout=subprocess.PIPE, shell=True).stdout.read().split('\n')
        for f in features:
            if "tcp-segmentation-offload" in f:
                tcpSeg = f.split()[1] == "off"
            elif "generic-receive-offload" in f:
                genRec = f.split()[1] == "off"
        results.append([dev, tcpSeg, genRec])
    return results

# Configuration tests

def test_os_release(output):
    supported = False
    distInfo = ""
    formatString = "{0} release {1} {2}"
    if platform.system() == "Linux":
        output['OS'] = ["PASS", "Linux"]
        distInfo = platform.dist()
        supported = False
        if distInfo[0] in ("centos", "redhat", "rhel"):
            releaseNum = ".".join(distInfo[1].split(".")[0:2]) # release goes to 3 parts in Centos/Redhat 7.x(.y)
            if releaseNum >= "6.6":
                supported = True
        elif "ubuntu" in distInfo[0].lower():
            if distInfo[1] in ("12.04", "14.04", "16.04"):
                supported = True
    elif platform.system() == "Darwin":
        output["OS"] = ["PASS", "MacOS X"]
        version = platform.uname()[2]
        distInfo = ("MacOS X", version, "") # on Mac, platform.dist() is empty
        if version >= "10.8.0":
            supported = True
    else:
        output['OS'] = ["WARN", "Only supports Linux based platforms"]
        output['OS release'] = ["WARN", "Supported distributions are Ubuntu 12.04/14.04 and RedHat/CentOS 6.6 or later"]
    if not supported:
        formatString = "Unsupported release: " + formatString
    output['OS release'] = ["PASS" if supported else "WARN", formatString.format(*distInfo)]

def test_64bit_os(output):
    if platform.machine().endswith('64'):
        output['64 bit'] = ["PASS", platform.uname()[2]]
    else:
        output['64 bit'] = ["FAIL", "64-bit Linux-based operating system is required to run VoltDB"]

def test_host_memory(output):
    hostMemory = subprocess.Popen("free | grep 'Mem' | tr -s ' ' | cut -d' ' -f2 ", stdout=subprocess.PIPE, shell=True).stdout.read().rstrip('\n')
    if int(hostMemory) >= 4194304:
        output['Memory'] = ["PASS", hostMemory]
        if int(hostMemory) >= 67108864:
            mmapCount = subprocess.Popen("cat /proc/sys/vm/max_map_count", stdout=subprocess.PIPE, shell=True).stdout.read().rstrip('\n')
            if int(mmapCount) >= 1048576:
                output['MemoryMapCount'] = ["PASS", "Virtual memory max map count is " + mmapCount]
            else:
                output['MemoryMapCount'] = ["WARN", "Virtual memory max map count is " + mmapCount +
                                                    ", recommended is 1048576 for system with 64 GB or more memory"]
    else:
        output['Memory'] = ["WARN", "Recommended memory is at least 4194304 kB but was " + hostMemory + " kB"]

def test_ntp(output):
    numOfRunningNTP = subprocess.Popen("ps -ef | grep 'ntpd ' | grep -cv grep", stdout=subprocess.PIPE, shell=True).stdout.read().rstrip('\n')
    returnCodeForNTPD = os.system("which ntpd >/dev/null 2>&1")
    if numOfRunningNTP == '1':
        output['NTP'] = ["PASS", "NTP is installed and running"]
    elif numOfRunningNTP  == '0':
        if returnCodeForNTPD == 0:
            output['NTP'] = ["WARN", "NTP is installed but not running"]
        else:
            output['NTP'] = ["WARN", "NTP is not installed or not in PATH enviroment"]
    else:
            output['NTP'] = ["WARN", "More then one NTP service is running"]

def test_java_version(output):
    if 'JAVA_HOME' in os.environ:
        java = os.path.join(os.environ['JAVA_HOME'], 'bin', 'java')
        jar = os.path.join(os.environ['JAVA_HOME'], 'bin', 'jar')
    else:
        java = utility.find_in_path('java')
        jar = utility.find_in_path('jar')
    if not java:
        utility.abort('Could not find java in environment, set JAVA_HOME or put java in the path.')
    javaVersion = utility.get_java_version(javaHome=java, verbose=True)
    if '1.8.' in javaVersion:
        output['Java'] = ["PASS", javaVersion.strip()]
    elif len(javaVersion) > 0:
        output['Java'] = ["FAIL", "Unsupported " + javaVersion + " Check if Java has been installed properly and JAVA_HOME has been setup correctly."]
    else:
        output['Java'] = ["FAIL", "Please check if Java has been installed properly and JAVA_HOME has been setup correctly."]

def test_python_version(output):
    pythonVersion = "Python " + str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '.' + str(sys.version_info[2])
    if sys.version_info[0] == 2 and sys.version_info[1] < 6:
        for dir in os.environ['PATH'].split(':'):
            for name in ('python2.7', 'python2.6'):
                path = os.path.join(dir, name)
                if os.path.exists(path):
                    pythonVersion = subprocess.Popen(path + " --version", stdout=subprocess.PIPE, shell=True).stdout.read()
                    break
            else:
                output['Python'] = ["FAIL", "VoltDB requires Python 2.6 or newer."]
                return
    output['Python'] = ["PASS", pythonVersion]

def test_thp_config(output):
    thpError = _check_thp_config()
    if thpError is not None:
        output['TransparentHugePage'] = ["FAIL", thpError]
    else:
        output['TransparentHugePage'] = ["PASS", "Transparent huge pages not set to always"]

def test_swap(output):
    swaponFiles = subprocess.Popen("cat /proc/swaps", stdout=subprocess.PIPE, shell=True).stdout.read().split('\n')[1:-1]
    if len(swaponFiles) > 0:
        swaponList = []
        for x in swaponFiles:
            l = x.split()
            if len(l) != 0:
                swaponList.append(l[0])
        output['Swapoff'] = ["WARN", "Swap is enabled for the filenames: " + ' '.join(swaponList)]
    else:
        output['Swapoff'] = ["PASS", "Swap is off"]

def test_swappinness(output):
    swappiness = subprocess.Popen("cat /proc/sys/vm/swappiness", stdout=subprocess.PIPE, shell=True).stdout.read().rstrip('\n')
    output['Swappiness'] = ["PASS" if int(swappiness) == 0 else "WARN", "Swappiness is set to " + swappiness]

def test_vm_overcommit(output):
    oc = subprocess.Popen("cat /proc/sys/vm/overcommit_memory", stdout=subprocess.PIPE, shell=True).stdout.read().rstrip('\n')
    if int(oc) == 1:
        output['MemoryOvercommit'] = ["PASS", "Virtual memory overcommit is enabled"]
    else:
        output['MemoryOvercommit'] = ["WARN", "Virtual memory overcommit is disabled"]

def test_segmentation_offload(output):
    devInfoList = _check_segmentation_offload()
    if devInfoList is None:
        return
    for devInfo in devInfoList:
        dev = devInfo[0]
        if devInfo[1]:
            output['TCPSegOffload_'+dev] = ["PASS", "TCP segmentation offload for " + dev + " is disabled"]
        else:
            output['TCPSegOffload_'+dev] = ["WARN", "TCP segmentation offload is recommended to be disabled, but is currently enabled for " + dev]
        if devInfo[2]:
            output['GenRecOffload_'+dev] = ["PASS", "Generic receive offload for " + dev + " is disabled"]
        else:
            output['GenRecOffload_'+dev] = ["WARN", "Generic receive offload is recommended to be disabled, but is currently enabled for " + dev]

def test_full_config(output):
    """ Runs a full set of configuration tests and writes the results to output
    """
    test_os_release(output)
    test_64bit_os(output)
    test_ntp(output)
    test_java_version(output)
    test_python_version(output)
    if platform.system() == "Linux":
        test_host_memory(output)
        test_swappinness(output)
        test_vm_overcommit(output)
        test_thp_config(output)
        test_swap(output)
        test_segmentation_offload(output)

def test_hard_requirements():
    """ Returns any errors resulting from hard config requirement violations
    """
    output = {}
    for k in hardRequirements:
        hardRequirements[k](output)
    return output

# (Moved to end since Python does not handle forward reference.)
# Define HardRequirements (full name : checker method)
# and possible SkippableleRequirements(fullname:init)
hardRequirements = {
        'TransparentHugePage' : test_thp_config,
        "Java" : test_java_version,
        "OS Release" : test_os_release,
}
skippableRequirements = {'TransparentHugePage':'thp'}
