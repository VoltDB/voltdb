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


def find_threads(pid):
# Jstack the pid and create a dictionary of threads/pids

    threads={}
    jstack_cmd = 'jstack ' + args.pid
    pattern_jstack=re.compile('"(.+)".*nid=(0x\S+)\s')

    try:
        for line in run_cmd(jstack_cmd, False):
            m = pattern_jstack.match(line)
            if m:
                #We need the pids in base10, but need them as strings
                threads[m.group(1)] = str(int(m.group(2),16))

    #TODO: Do some better error handling here.
    except subprocess.CalledProcessError as e:
        print e
        sys.exit("Cannot jstack pid " + args.pid)

    return threads


def run_cmd (cmd, stderr_to_stdout=False):
    if stderr_to_stdout:
        stderrflag = subprocess.STDOUT
    else:
        stderrflag = None
    popen = subprocess.Popen(shlex.split(cmd), stdout=subprocess.PIPE, stderr=stderrflag, universal_newlines=True)
    stdout_lines = iter(popen.stdout.readline, b'')
    for stdout_line in stdout_lines:
        #print stdout_line
        yield stdout_line

    popen.stdout.close()
    return_code = popen.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, cmd)


desc = """Run strace on VoltDB (or another JVM-based process) and show output with
thread names for all the activity.  Threads can be narrowed down by a regex.
    Example - strace all threads with "Snap" in the name, but exclude futex calls
    /stracethread.py -s "-e trace=\!futex" Snap.\*
"""

parser = argparse.ArgumentParser(description=desc, formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-l','--list', action='store_true', help = 'List the matching threads and pids')
parser.add_argument('-p','--pid', default=find_voltdb_pid(), help = 'Pid of java process')
parser.add_argument('-s','--strace_args', default = "-tt", help='Strace args, for example "-e trace=file". -tt added by default')
parser.add_argument('-v','--verbose', action='store_true', help = 'Verbose output - not suitable for eval')
parser.add_argument('regex', nargs='?', default='.*', help = 'Regex for thread names in the jstack output' )

args=parser.parse_args()

# Find a pid?
if not args.pid:
    sys.exit ('ERROR: No VoltDB process found and no pid was specified')
if args.list:
    args.verbose = True


# Valid regex?
try:
    pattern_thread=re.compile(args.regex)
except:
    sys.exit ('ERROR: ' + args.regex + ' is not a valid regex')


# Find the threads and make dict of pids.
threads = find_threads(args.pid)


#Find the ones that match the regex
matching_threads = {key:value for key, value in threads.items() if re.search(pattern_thread, key)}

if not matching_threads:
    sys.exit ('ERROR: No threads matched your regex: ' + args.regex)

# Make a dict of pid:thread.
pids = {value:key for key, value in matching_threads.items()}
#print pids

if args.verbose:
    print 'Matched threads for pid=%s regex=%s' % (args.pid, args.regex)
    listformat = '\t%-8s  %s'
    print listformat % ( 'PID', 'Thread Name')
    for k in sorted(matching_threads.keys()):
        print listformat % ( matching_threads[k], k)


strace_cmd = 'strace %s -p %s' % (args.strace_args, ' -p '.join(matching_threads.values()))
if args.verbose:
    print
    print "Strace command:"
    print strace_cmd

if args.list:
    exit()


def replacepid(m):
    return ('{0:5} - {1:25}'.format(m.group(1), pids[m.group(1)][:25]))

#TODO: This could use better error handling for ctrl-c
for line in run_cmd(strace_cmd, stderr_to_stdout=True):
    sys.stdout.write( re.sub(r'pid\s+(\d+)', replacepid, line))
    sys.stdout.flush()
