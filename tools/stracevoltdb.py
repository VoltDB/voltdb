#!/usr/bin/python

import argparse
import re
import shlex
import subprocess
import sys

def find_voltdb_pid():
    p = subprocess.Popen(['jps'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out,err = p.communicate()
    m = re.search('(\d+)\s+VoltDB$', out, flags=re.MULTILINE)
    if m:
        return m.group(1)
    else:
        return None


desc = """Create an strace command for some or all threads in a JVM
 The best way to run this is:
    eval $(./stracethread.py)
 If you want to add strace arguments, then it would be:
    eval $(./stracethread.py -s "-tt -o strace.out -e trace=\!futex") &
"""

parser = argparse.ArgumentParser(description=desc, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-l','--list', action='store_true', help = 'List the matching threads and pids')
parser.add_argument('-p','--pid', default=find_voltdb_pid(), help = 'Pid of java process')
parser.add_argument('-s','--strace_args', help='Strace args, for example "-o strace.out"')
parser.add_argument('-v','--verbose', action='store_true', help = 'Verbose output - not suitable for eval')
parser.add_argument('regex', nargs='?', default='.*', help = 'Regex for thread names in the jstack output' )

args=parser.parse_args()

if not args.pid:
    sys.exit ('ERROR: No VoltDB process found and no pid was specified')
if args.list:
    args.verbose = True


try:
    pattern_thread=re.compile(args.regex)
except:
    sys.exit ('ERROR: ' + args.regex + ' is not a valid regex')


threads={}
pattern_jstack=re.compile('"(.+)".*nid=(0x\S+)\s')

#Get the thread names and pids from jstack and place them in threads dictionary
p = subprocess.Popen(['jstack', args.pid], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

with p.stdout:
    for line in iter(p.stdout.readline,b''):
        m = pattern_jstack.match(line)
        if m:
            #We need the pids in base10, but need them as strings
            threads[m.group(1)] = str(int(m.group(2),16))
p.wait()

#Find the ones that match the regex
matching_threads = {key:value for key, value in threads.items() if re.search(pattern_thread, key)}

if not matching_threads:
    sys.exit ('ERROR: No threads matched your regex: ' + args.regex)

if args.verbose:
    print 'Matched threads for pid=%s regex=%s' % (args.pid, args.regex)
    listformat = '\t%-8s  %s'
    print listformat % ( 'PID', 'Thread Name')
    for k in sorted(matching_threads.keys()):
        print listformat % ( matching_threads[k], k)

if not args.list:
    strace_cmd = 'sudo strace %s -p %s' % (args.strace_args, ' -p '.join(matching_threads.values()))
    if args.verbose:
        print
        print "Strace command:"
    print strace_cmd
