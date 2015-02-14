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

import errno
import os
import re
import subprocess
import sys
import tempfile
import time

from optparse import OptionParser


# recursive directory creator like linux "mkdirs -p"
# Credit: cribbed from stackoverflow.com/questions/600268
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


# kill the VoltDB server process, if any, running on the local machine.
# The process is identified by its command line text via the ps command.
def kill_voltdb():
    # ps - find all running java commands.
    # fgrep - filter out any headers or command lines that do not reference org.voltdb.VoltDB
    # tr - compress multi-space delimiters to a single space.
    # cut - extract the second space-delimited field, which is the PID.
    # xargs - execute a kill command on each PID (hoping there's only the 1 launched from this script).
    killed = subprocess.call(
            "ps -fwwC java | grep org.voltdb.VoltDB | tr -s ' ' | cut -d ' ' -f 2 | xargs /bin/kill -KILL", shell=True)
    if killed != 0:
        print >> sys.stderr, \
                "Failed to kill the VoltDB server process"

# given a script_subdir somewhere within a script_dir,
# return the path of the corresponding dir within baseline_dir.
# For example, for ('my/scripts/a/b/c', 'my/scripts', 'your/baselines'),
# return 'your/baselines/a/b/c'.
def replace_parent_dir_prefix(script_subdir, script_dir, baseline_dir):
    if script_subdir[0: len(script_dir)] == script_dir:
        return baseline_dir + script_subdir[len(script_dir):]
    # If the subdir relationship between script_subdir and script_dir is
    # non-obvious, do some guesswork.
    # If this becomes a problem, we may want to try to funnel more cases
    # into the above conditional by first canonicalizing script_subdir and
    # script_dir. Failing that, there may be ways to improve this "guessing"
    # algorithm.
    # If script_subdir starts with an alphanumeric,
    # assume that it is relative to script_dir.
    if re.match('\w', script_subdir):
        return os.path.join(baseline_dir, script_subdir)
    # Otherwise, assume that its path up to the first separator is equivalent
    # to script_dir so that everything beyond that is relative to script_dir.
    subdir_path_part = script_subdir[script_subdir.find(os.pathsep)+1:]
    return os.path.join(baseline_dir, subdir_path_part)


def launch_and_wait_on_voltdb(reportout):
    # Launch a single local voltdb server to serve all scripted sqlcmd runs.
    # The scripts are expected to clean up after themselves  -- and/or defensively
    # drop and create all of their tables up front.
    subprocess.Popen(['../../bin/voltdb', 'create'], shell=False)
    # give server a little startup time.
    time.sleep(5)

    empty_input = tempfile.TemporaryFile()
    empty_input.write("\n")
    empty_input.flush()
    # Patiently wait for the server to initialize, using a dummy run of sqlcmd as a readiness test
    # -- at least for a few minutes.
    # This may need to change if we ever allowed sqlcmd to launch a connection-less session
    # that could connect later in response to a directive.
    for waited in xrange(0, 20):
        if waited == 20:
            reportout.write("voltdb server not responding -- so giving up\n")
            kill_voltdb()
            return
        empty_input.seek(0)
        waiting = subprocess.call(['../../bin/sqlcmd'], stdin=empty_input)
        if not waiting:
            break
        # give the server a little more setup time.
        time.sleep(10)
    empty_input.close()

def do_main():
    parser = OptionParser()
    parser.add_option("-s", "--scripts", dest="script_dir", default="./scripts",
                      help="top level test case script directory")
    parser.add_option("-b", "--baselines", dest="baseline_dir", default="./baselines",
                      help="top level test output baseline directory")
    parser.add_option("-o", "--reportfile", dest="reportfile",
                      default="./sqlcmdtest.report",
                      help="report output file")
    parser.add_option("-r", "--refresh", dest="refresh",
                      action="store_true", default=False,
                      help="enable baseline refresh")
    # TODO add a way to pass non-default options to the VoltDB server and tweak the sqlcmd
    # command line options if/when these settings effect the connection string
    # (non-default ports. security, etc.)
    # TODO add a way to pass sqlcmd command line options to be used with all test scripts.
    (options, args) = parser.parse_args()
    reportout = open(options.reportfile, 'w+')

    launch_and_wait_on_voltdb(reportout)

    try:
        for parent, dirs, files in os.walk(options.script_dir):
            # Process each ".in" file found in the recursive directory walk.
            # Ignore other files -- these may be scratch files that (FIXME) really should be
            # written to a temp directory instead, or they may be backup files (like from a text editor)
            # or in the future they may be other kinds of input like a ".options" file that
            # could provide sqlcmd command line options to use with a corresponding ".in" file.
            for input in files:
                if not input.endswith(".in"):
                    continue
                print "Running ", os.path.join(parent, input)
                prefix = input[:-3]
                childin = open(os.path.join(parent, input))
                # TODO use temp scratch files instead of local files to avoid polluting the git
                # workspace. Ideally they would be self-cleaning except in failure cases or debug
                # modes when they may contain useful diagnostic detail.
                childout = open(os.path.join(parent, prefix + '.out'), 'w+')
                childerr = open(os.path.join(parent, prefix + '.err'), 'w+')
                subprocess.call(['../../bin/sqlcmd'],
                        stdin=childin, stdout=childout, stderr=childerr)

                # fuzz the sqlcmd output for reliable comparison
                outbackin = open(os.path.join(parent, prefix + '.out'), 'r')
                cleanedpath = os.path.join(parent, prefix + '.outclean')
                cleanedout = open(cleanedpath, 'w+')
                # Currently, the only fuzzing required is to allow different latency numbers to be
                # reported like "(Returned 3 rows in 9.99s)" vs. "(Returned 3 rows in 10.01s)".
                # These both get "fuzzed" into the same generic string "(Returned 3 rows in #.##s)".
                # This produces identical 'baseline` results on platforms and builds that
                # may run at different speeds.
                latency_matcher = re.compile(r"""
                        ([0-9]\srows\sin\s)  # required to match a latency report line, survives as \g<1>
                        [0-9]+\.[0-9]+s      # also required, replaced with #.##s
                        """, re.VERBOSE)
                for line in outbackin:
                    cleanedline = latency_matcher.sub("\g<1>#.##s", line)
                    # # enable for debug #print cleanedline
                    cleanedout.write(cleanedline)
                cleanedout.flush()
                # # enable for debug
                # subprocess.call(
                #             ['cat',
                #              cleanedpath],
                #             stdin=cleanedout)
                # #
                baseparent = replace_parent_dir_prefix(parent, options.script_dir, options.baseline_dir)
                # The ".outbaseline" extension was chosen with the thought that for some test cases
                # we may be interested in validating sqlcmd's stderr output against a ".errbaseline"
                # (TODO).
                baselinepath = os.path.join(baseparent, prefix + '.outbaseline')
                gotdiffs = True  # default in case baseline does not exist.
                if os.path.isfile(baselinepath):
                    outdiffspath = os.path.join(parent, prefix + '.outdiffs')
                    diffout = open(outdiffspath, 'w+')
                    gotdiffs = subprocess.call(['diff', cleanedpath, baselinepath],
                            stdout=diffout)
                    if gotdiffs:
                        print >> sys.stderr, \
                                'See diffs in ', outdiffspath
                        # Would it be better to append the diffs into the report file?
                        reportout.write('See diffs in ' + os.path.abspath(outdiffspath) + "\n")
                        reportout.write(os.path.join(parent, input) +
                                " failed to match its output baseline.\n")
                    else:
                        reportout.write(os.path.join(parent, input) +
                                " matched its output baseline.\n")
                        # clean up
                        subprocess.call(['rm', outdiffspath])
                else:
                    reportout.write("Did not find baseline file: " +
                            os.path.abspath(baselinepath) + "\n")
                # If requested, rewrite baseline files that are missing or have changed.
                if gotdiffs and options.refresh:
                    mkdir_p(baseparent)
                    subprocess.call(['mv', cleanedpath, baselinepath])
                    reportout.write(os.path.join(parent, input) +
                            " refreshed its baseline output: " +
                            os.path.abspath(baselinepath) + "\n")
    finally:
        kill_voltdb()
        print "Summary report written to ", os.path.abspath(options.reportfile)
        # Would it be useful to dump the report file content to stdout?

if __name__ == "__main__":
    do_main()
