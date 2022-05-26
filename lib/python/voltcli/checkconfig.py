# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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
    with open(filename) as f:
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
    tokenDevs = subprocess.Popen("ip link | grep -B 1 ether | grep -E -v '^--'", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").split('\n')[:-1][0::2]
    for d in [x.split(':') for x in tokenDevs]:
        if len(d) < 2:
            continue
        dev = d[1].strip()
        tcpSeg = False
        genRec = False
        features = subprocess.Popen("ethtool --show-offload " + dev, stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").split('\n')
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
    osName = platform.system()
    osVersion = None
    note = None
    if osName == 'Linux':
        output['OS'] = ['PASS', 'Linux']
        distro = distro_info()
        if distro:
            osId, osName, osVersion = distro
            if osId == 'ubuntu':
                supported = osVersion in ['16.04', '18.04', '20.04']
                note = 'needs 16.04/18.04/20.04'
            elif osId in ['centos', 'redhat', 'rhel', 'rocky']:
                supported = version_number(osVersion, 0) >= 7
                note = 'needs 7.0 or later'
            elif osId == 'alpine':
                supported = version_number(osVersion, 0) == 3 and version_number(osVersion, 1) >= 8;
                note = 'needs 3.8 or later'
            else:
                note = 'needs Ubuntu or RedHat/CentOS'
        else:
            note = 'unknown distribution'
    elif osName == 'Darwin':
        output['OS'] = ['PASS', 'MacOS X']
        osName = 'MacOS X'
        osVersion = platform.uname().release
        supported = osVersion >= '10.8.0'
        note = 'needs 10.8.0 or later'
    else:
        output['OS'] = ['WARN', 'Unsupported platform ' + osName + ' (needs Linux-based system)']
    if supported:
        output['OS release'] = ['PASS', osName + ' ' + osVersion]
    else:
        fail = 'Unsupported release ' + osName
        if osVersion:
            fail += ' ' + osVersion
        if note:
            fail += ' (' + note + ')'
        output['OS release'] = ['WARN', fail]

def distro_info():
    # Try Python LSB module as it has the best-looking content
    try:
        import lsb_release
        info = lsb_release.get_distro_information()
        return (info['ID'].lower(), info['ID'], info['RELEASE'])
    except Exception:
        pass
    # Not got that? Try os-release file
    try:
        with open('/etc/os-release') as file:
            info = { kv[0]: kv[1].strip('"') for kv in
                     ( line.strip().split('=',1) for line in file )
                     if len(kv) == 2 }
        if '.' not in info['VERSION_ID']:  # redhat: single digit
            v2 = lsb_release_by_command()
            if v2: info['VERSION_ID'] = v2
        return (info['ID'], info['NAME'], info['VERSION_ID'])
    except Exception:
        pass
    return None

def lsb_release_by_command():
    try:
        ver = subprocess.run(['lsb_release', '-sr'], timeout=2.0, check=True,
                             stdout=subprocess.PIPE, encoding='utf-8')
        return ver.stdout.strip()
    except Exception:
        return None

def version_number(str, n):
    try:
        return int(str.split('.')[n])
    except Exception:
        return 0

def test_64bit_os(output):
    if platform.machine().endswith('64'):
        output['64 bit'] = ["PASS", platform.uname()[2]]
    else:
        output['64 bit'] = ["FAIL", "64-bit Linux-based operating system is required to run VoltDB"]

def test_host_memory(output):
    hostMemory = subprocess.Popen("free | grep 'Mem' | tr -s ' ' | cut -d' ' -f2 ", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").rstrip('\n')
    if int(hostMemory) >= 4194304:
        output['Memory'] = ["PASS", hostMemory]
        if int(hostMemory) >= 67108864:
            mmapCount = subprocess.Popen("cat /proc/sys/vm/max_map_count", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").rstrip('\n')
            if int(mmapCount) >= 1048576:
                output['MemoryMapCount'] = ["PASS", "Virtual memory max map count is " + mmapCount]
            else:
                output['MemoryMapCount'] = ["WARN", "Virtual memory max map count is " + mmapCount +
                                                    ", recommended is 1048576 for system with 64 GB or more memory"]
    else:
        output['Memory'] = ["WARN", "Recommended memory is at least 4194304 kB but was " + hostMemory + " kB"]

def test_ntp(output):
    numOfRunningNTP = subprocess.Popen("ps -ef | grep 'ntpd ' | grep -cv grep", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").rstrip('\n')
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
    javaVersion = utility.get_java_version(javaHome=java, verbose=True).decode("utf-8")
    if '1.8.' in javaVersion or '11.0' in javaVersion or '17.0' in javaVersion:
        output['Java'] = ["PASS", javaVersion.strip()]
    elif len(javaVersion) > 0:
        output['Java'] = ["FAIL", "Unsupported " + javaVersion + " Check if Java has been installed properly and JAVA_HOME has been setup correctly."]
    else:
        output['Java'] = ["FAIL", "Please check if Java has been installed properly and JAVA_HOME has been setup correctly."]

def test_python_version(output):
    pythonVersion = "Python " + str(sys.version_info[0]) + '.' + str(sys.version_info[1]) + '.' + str(sys.version_info[2])
    if sys.hexversion < 0x03060000:
        for dir in os.environ['PATH'].split(':'):
            # Hunt for some python 3.x (but not too far into future versions), though if
            # we're running from 'voltdb check' we are already running under python 3.
            for n in range(6, 12):
                name = 'python3.%d' % n
                path = os.path.join(dir, name)
                if os.path.exists(path):
                    pythonVersion = subprocess.Popen(path + " --version", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8")
                    break
            else:
                output['Python'] = ["FAIL", "VoltDB requires Python 3.6 or newer."]
                return
    output['Python'] = ["PASS", pythonVersion]

def test_username(output):
    if os.geteuid()==0:
        output['RootUser'] = ["WARN", "Running the VoltDB server software from the system root account is not recommended"]
    else:
        output['RootUser'] = ["PASS", "Not running as root"]

def test_thp_config(output):
    thpError = _check_thp_config()
    if thpError is not None:
        output['TransparentHugePage'] = ["FAIL", thpError]
    else:
        output['TransparentHugePage'] = ["PASS", "Transparent huge pages not set to always"]

def test_swap(output):
    swaponFiles = subprocess.Popen("cat /proc/swaps", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").split('\n')[1:-1]
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
    swappiness = subprocess.Popen("cat /proc/sys/vm/swappiness", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").rstrip('\n')
    output['Swappiness'] = ["PASS" if int(swappiness) == 0 else "WARN", "Swappiness is set to " + swappiness]

def test_vm_overcommit(output):
    oc = subprocess.Popen("cat /proc/sys/vm/overcommit_memory", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").rstrip('\n')
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

def test_tcp_retries2(output):
    tcpretries2=subprocess.Popen("sysctl  net.ipv4.tcp_retries2 | cut -d'=' -f2", stdout=subprocess.PIPE, shell=True).stdout.read().decode("utf-8").strip()
    if int(tcpretries2) >= 8:
        output['TCP Retries2'] = ["PASS", "net.ipv4.tcp_retries2 is set to " + tcpretries2]
    else:
        output['TCP Retries2'] = ["WARN", "net.ipv4.tcp_retries2 is recommended to be 8 or higher, but is currently set to " + tcpretries2]

def test_full_config(output):
    """ Runs a full set of configuration tests and writes the results to output
    """
    test_os_release(output)
    test_64bit_os(output)
    test_ntp(output)
    test_java_version(output)
    test_python_version(output)
    test_username(output)
    if platform.system() == "Linux":
        test_host_memory(output)
        test_swappinness(output)
        test_vm_overcommit(output)
        test_thp_config(output)
        test_swap(output)
        test_segmentation_offload(output)
        test_tcp_retries2(output)
        # Compare Swapoff and Swappiness (Swappiness wins)
        if output["Swapoff"][0] == "WARN" and output["Swappiness"][0] == "PASS":
           output["Swapoff"][0] = "PASS"
           output["Swapoff"][1] = output["Swapoff"][1]  + ", however swappiness supercedes it"

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
