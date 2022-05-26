#!/usr/bin/env python
# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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
    # Works for Linux and Mac.
    # ps - find all running java commands.
    # awk - finds commands ending in "/java" that refer to the org.voltdb.VoltDB class and outputs the PID(s)
    # xargs - execute a kill command on each PID (hoping there's only the 1 launched from this script).
    return subprocess.call('''\
ps -fww | awk '
$8 ~ "/java$" {
    for (i = 9; i <= NF; i++) {
        if ($i == "org.voltdb.VoltDB") {
            print $2;
        }
    }
}' | xargs /bin/kill -KILL''', shell=True)
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
    voltenv=os.environ.copy()
    sqlcmdenv=os.environ.copy()
    sqlcmdopts=[]
    if os.environ.has_key("ENABLE_SSL") and os.environ["ENABLE_SSL"].lower() == "true":
        prop_pfx="-Djavax.net.ssl."
        keystore=os.path.realpath("../../tests/frontend/org/voltdb/keystore")
        voltenv["VOLTDB_OPTS"] = (prop_pfx + "keyStore=" + keystore + " " + prop_pfx +
                                  "keyStorePassword=password " + prop_pfx + "trustStore=" +
                                  keystore + " " + prop_pfx + "trustStorePassword=password")
        sqlcmdenv["ENABLE_SSL"] = "false"
        sqlcmdopts = ["-J" + prop_pfx + "trustStore=" + keystore]
        sqlcmdopts = sqlcmdopts + ["-J" + prop_pfx + "trustStorePassword=password", "--ssl"]
    print "Initializing directory..."
    subprocess.call(['../../bin/voltdb', 'init', '--force'], shell=False)
    subprocess.Popen(['../../bin/voltdb', 'start'], shell=False)
    # give server a little startup time.
    time.sleep(5)

    empty_input = tempfile.TemporaryFile()
    empty_input.write("\n")
    empty_input.flush()
    # Patiently wait for the server to initialize, using a dummy run of sqlcmd as a readiness test
    # -- at least for a few minutes.
    # This may need to change if we ever allowed sqlcmd to launch a connection-less session
    # that could connect later in response to a directive.
    for waited in xrange(0, 19):
        empty_input.seek(0)
        waiting = subprocess.call(['../../bin/sqlcmd'] + sqlcmdopts, stdin=empty_input, env=sqlcmdenv)
        if not waiting:
            break
        if waited == 19:
            reportout.write("voltdb server not responding -- so giving up\n")
            kill_voltdb()
            return
        print "Connection will be retried shortly."
        sys.stdout.flush()
        # give the server a little more setup time.
        time.sleep(10)
    empty_input.close()

purge_only_count = 0

# The purgeonly utility mode was enabled. Purge anything that matches the pattern of a generated file,
def purgeonly(script_dir):
    global purge_only_count
    for parent, dirs, files in os.walk(script_dir):
        for inpath in files:
            # Exempt the expected ".in" files up front as a common case,
            # but DO NOT ASSUME that other files are non-persistent or expendable.
            if inpath.endswith(".in"):
                continue
            # Rely on explicit positive pattern matching of generated fie extensions to identify garbage.
            if (inpath.endswith(".err") or
                inpath.endswith(".errclean") or
                inpath.endswith(".errdiffs") or
                inpath.endswith(".out") or
                inpath.endswith(".outclean") or
                inpath.endswith(".outdiffs")):
                scratchpath = os.path.join(parent, inpath)
                purge_only_count += 1
                subprocess.call(['rm', scratchpath])

def clean_output(parent, path):
    # fuzz the sqlcmd output for reliable comparison
    outbackin = open(os.path.join(parent, path), 'r')
    cleanedpath = os.path.join(parent, path + 'clean')
    cleanedout = open(cleanedpath, 'w+')
    # Currently, the following cases of fuzzing are required:
    # 1. Allow consistent baselines across different builds with java memcheck enabled
    # or disabled by filtering out the warning that gets issued when it is enabled.
    memory_check_matcher = re.compile(r"""
            ^(WARN:\s)?Strict\sjava\smemory\schecking.*$  # Match the start.
            """, re.VERBOSE)
    # 2. Allow different latency numbers to be reported, like
    # "(Returned 3 rows in 9.99s)" vs. "(Returned 3 rows in 10.01s)".
    # These both get "fuzzed" into the same generic string "(Returned 3 rows in #.##s)".
    # This produces identical 'baseline` results on platforms and builds that
    # may run at different speeds.
    latency_matcher = re.compile(r"""
            ([0-9]\srows\sin\s)  # required to match a latency report line,
                                 # survives as \g<1>
            [0-9]+\.[0-9]+s      # also required, replaced with #.##s
            """, re.VERBOSE)
    # 3. Allow different query timeout periods to be reported, like
    # "A SQL query was terminated after 1.000 seconds because it exceeded the query timeout period." vs.
    # "A SQL query was terminated after 1.001 seconds because it exceeded the query timeout period.".
    # These both get "fuzzed" into the same generic string:
    # "A SQL query was terminated after 1.00# seconds because it exceeded the query timeout period."
    # ignoring the final milliseconds digit.
    # This produces identical 'baseline` results on platforms and builds that
    # may terminate a query after a slightly different number of milliseconds.
    query_timeout_matcher = re.compile(r"""
            (terminated\safter\s[0-9]+\.[0-9][0-9])  # required to match a query timeout
                                                     # line, survives as \g<1>
            [0-9]                                    # final digit, replaced with #
            (\sseconds)                              # survives as \g<2>
            """, re.VERBOSE)
    # 4. Allow stack trace differences that might normally arise from different versions of
    # voltdb or other library (e.g. reflection) source code.
    stack_frame_matcher = re.compile(r"""
            (at\s.+\.java\:)                         # required to match a stack trace line
                                                     # line, survives as \g<1>
            [0-9]+                                   # line number digits, replaced with #
            (\))                                     # survives as \g<2>
            """, re.VERBOSE)

    for line in outbackin:
        # Note len(cleanedline) here counts 1 EOL character.
        # Preserve blank lines as is -- there's no need to try cleaning them.
        if len(line) == 1:
            cleanedout.write(line)
            continue
        cleanedline = memory_check_matcher.sub("", line)
        cleanedline = latency_matcher.sub("\g<1>#.##s", cleanedline)
        cleanedline = query_timeout_matcher.sub("\g<1>#\g<2>", cleanedline)
        cleanedline = stack_frame_matcher.sub("\g<1>#\g<2>", cleanedline)
        # # enable for debug print "DEBUG line length %d" % (len(cleanedline))
        # # enable for debug #print cleanedline
        # Here, a blank line resulted from a total text replacement,
        # so skip it.
        # This allows us to eliminate without a trace lines that are optional
        # from one run to the next. This is a known use case,
        # involving memory_checker_matcher.
        # The alternative use case where the intent is that a non-blank line
        # from one run be fuzzed to match a blank line from another run has not
        # yet surfaced, so, for now, we just hope it doesn't.
        if len(cleanedline) > 1:
            cleanedout.write(cleanedline)
    cleanedout.flush()
    # # enable for debug
    # subprocess.call(
    #             ['cat',
    #              cleanedpath],
    #             stdin=cleanedout)
    # #


def compare_cleaned_to_baseline(parent, baseparent, path, inpath, do_refresh, reportout):
    cleanedpath = os.path.join(parent, path + 'clean')
    baselinepath = os.path.join(baseparent, path + 'baseline')
    gotdiffs = True  # default in case baseline does not exist.
    if os.path.isfile(baselinepath):
        outdiffspath = os.path.join(parent, path + 'diffs')
        diffout = open(outdiffspath, 'w+')
        gotdiffs = subprocess.call(['diff', baselinepath, cleanedpath],
                stdout=diffout)
        if gotdiffs:
            print >> sys.stderr, \
                    'See diffs in ', outdiffspath
            # Would it be better to append the diffs into the report file?
            reportout.write('See diffs in ' +
                    os.path.abspath(outdiffspath) + "\n")
            reportout.write(os.path.join(parent, inpath) +
                    " failed to match its baseline.\n")
        else:
            reportout.write(os.path.join(parent, inpath) +
                    " matched its baseline.\n")
            # clean up
            subprocess.call(['rm', outdiffspath])
    else:
        reportout.write("Did not find baseline file: " +
                os.path.abspath(baselinepath) + "\n")
    # If requested, rewrite baseline files that are missing or have changed.
    if gotdiffs:
        if do_refresh:
            mkdir_p(baseparent)
            subprocess.call(['mv', cleanedpath, baselinepath])
            reportout.write(os.path.join(parent, inpath) +
                    " refreshed its baseline: " +
                    os.path.abspath(baselinepath) + "\n")
        else:
            return True
    return False

def delete_proc(pfile):
    # drop any procedures left in between any tests
    defaultstoredprocedures = {".insert",".update",".select",".delete",".upsert"}
    procset = set()

    for line in pfile.splitlines():
        columns = line.split(',')
        # column[2] gives the procedure name. using set to discard duplicates among partition.
        try:
            if columns[2] != ' ' :
                procname = columns[2].replace('\"','')
                if any( procname.endswith(defaultprocedure) for defaultprocedure in defaultstoredprocedures) :
                    continue
                print "user procedure : " + procname  #debug
                procset.add(procname)
        except IndexError:
            pass

    if len(procset) :
        sb = ''
        sb += 'file -inlinebatch EOB'+ '\n' + '\n'
        for procname in procset:
            sb += 'DROP PROCEDURE ' + procname + ' IF EXISTS;' + '\n'
        sb += '\n' + 'EOB'

        proc = subprocess.Popen(['../../bin/sqlcmd'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        proc.communicate(sb)
        rc = proc.wait()

        if(rc != 0) :
            # debug
            (stdoutprocdata, stderrdata) = proc.communicate()
            print "sqlcmdtest error \n"
            print "Detail output : " +  stdoutprocdata
            print "Detail error : " +  stderrdata


def delete_table_and_view(pfile):
    # drop table (unique) and all its views left in between any tests
    tableset = set()
    for line in pfile.splitlines():
        columns = line.split(',')
        # column[5] gives the table and views. using set to discard duplicates among partition.
        try:
            if columns[5] != ' ' :
                tablename = columns[5].replace('\"','')
                tableset.add(tablename)
        except IndexError:
            pass

    if len(tableset) :
        sb = ''
        sb += 'file -inlinebatch EOB'+ '\n' + '\n'
        for tablename in tableset:
            print "user table/view : " + tablename  #debug
            sb += 'DROP TABLE ' + tablename + ' IF EXISTS CASCADE;' + '\n'
        sb += '\n' + 'EOB'

        proc = subprocess.Popen(['../../bin/sqlcmd'],
                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        proc.communicate(sb)
        rc = proc.wait()

        if(rc != 0) :
            # debug
            (stdouttabledata, stderrdata) = proc.communicate()
            print "sqlcmdtest error \n"
            print "Detail output : " +  stdouttabledata
            print "Detail error : " +  stderrdata


def do_main():
    parser = OptionParser()
    parser.add_option("-s", "--scripts", dest="script_dir", default="./scripts",
                      help="top level test case script directory")
    parser.add_option("-b", "--baselines", dest="baseline_dir", default="./baselines",
                      help="top level test output baseline directory")
    parser.add_option("-o", "--report_file", dest="report_file",
                      default="./sqlcmdtest.report",
                      help="report output file")
    parser.add_option("-r", "--refresh", dest="refresh",
                      action="store_true", default=False,
                      help="enable baseline refresh")
    parser.add_option("-p", "--purge_only", dest="purge_only",
                      action="store_true", default=False,
                      help="instead of running tests, purge temp scratch files from prior runs")
    # TODO add a way to pass non-default options to the VoltDB server and tweak the sqlcmd
    # command line options if/when these settings effect the connection string
    # (non-default ports. security, etc.)
    # TODO add a way to pass sqlcmd command line options to be used with all test scripts.
    (options, args) = parser.parse_args()

    if options.purge_only:
        purgeonly(options.script_dir)
        sys.exit("The -p/--purge_only option does not run tests. It purged %d scratch files." % (purge_only_count))

    # TODO Output jenkins-friendly html-formatted report artifacts as an alternative to plain text.
    reportout = open(options.report_file, 'w+')

    # TODO -- support different server modes  -- either by explicit command line
    # option or automatic-but-verbosely -- to detect and use an already running
    # VoltDB server and remember to leave it running on exit.
    launch_and_wait_on_voltdb(reportout)


    # Except in refresh mode, any diffs change the scripts exit code to fail ant/jenkins
    haddiffs = False
    try:
        for parent, dirs, files in os.walk(options.script_dir):
            # Process each ".in" file found in the recursive directory walk.
            # Ignore other files -- these may be scratch files that (FIXME) really should be
            # written to a temp directory instead, or they may be backup files (like from a text editor)
            # or in the future they may be other kinds of input like a ".options" file that
            # could provide sqlcmd command line options to use with a corresponding ".in" file.
            for inpath in files:
                if not inpath.endswith(".in"):
                    continue
                prefix = inpath[:-3]

                config_params = ""
                prompt = "Running " + os.path.join(parent, inpath)
                if os.path.isfile(os.path.join(parent, prefix + '.config')):
                    with open (os.path.join(parent, prefix + '.config'), "r") as configFile:
                        config_params=configFile.read().strip()
                    prompt += " with configuration:" + config_params.replace('\n', ' ')
                print prompt

                childin = open(os.path.join(parent, inpath))
                # TODO use temp scratch files instead of local files to avoid polluting the git
                # workspace. Ideally they would be self-purging except in failure cases or debug
                # modes when they may contain useful diagnostic detail.
                childout = open(os.path.join(parent, prefix + '.out'), 'w+')
                childerr = open(os.path.join(parent, prefix + '.err'), 'w+')


                if config_params:
                    subprocess.call(['../../bin/sqlcmd'] + config_params.split("\n"),
                        stdin=childin, stdout=childout, stderr=childerr)
                else:
                    subprocess.call(['../../bin/sqlcmd'],
                        stdin=childin, stdout=childout, stderr=childerr)

                # Verify a clean database by dropping any procedure,views and table after each test to prevent cross-contamination.
                proc = subprocess.Popen(['../../bin/sqlcmd', '--query=exec @SystemCatalog procedures', '--output-skip-metadata', '--output-format=csv'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                rc = proc.wait()
                (stdoutprocdata, stdoutprocerr) = proc.communicate()


                if (rc != 0) :
                    # debug
                    print "sqlcmdtest error \n"
                    print "Detail output : " +  stdoutprocdata
                    print "Detail error : " +  stdoutprocerr
                else :
                    delete_proc(stdoutprocdata)

                proc = subprocess.Popen(['../../bin/sqlcmd', '--query=exec @Statistics table 0', '--output-skip-metadata', '--output-format=csv'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                rc = proc.wait()
                (stdouttabledata, stdouttableerr) = proc.communicate()


                if (rc != 0) :
                    # debug
                    print "sqlcmdtest error \n"
                    print "Detail output : " +  stdouttabledata
                    print "Detail error : " +  stdouttableerr
                else :
                    delete_table_and_view(stdouttabledata)

                # fuzz the sqlcmd output for reliable comparison
                clean_output(parent, prefix + '.out')
                clean_output(parent, prefix + '.err')

                baseparent = replace_parent_dir_prefix(parent, options.script_dir, options.baseline_dir)
                if compare_cleaned_to_baseline(parent, baseparent,
                        prefix + '.out', inpath,
                        options.refresh, reportout):
                    haddiffs = True;
                if compare_cleaned_to_baseline(parent, baseparent,
                        prefix + '.err', inpath,
                        options.refresh, reportout):
                    haddiffs = True;
    finally:
        kill_voltdb()
        print "Summary report written to file://" + os.path.abspath(options.report_file)
        # Would it be useful to dump the report file content to stdout?
        # Except in refresh mode, any diffs change the scripts exit code to fail ant/jenkins
        if haddiffs:
            sys.exit("One or more sqlcmdtest script failures or errors was detected.")

if __name__ == "__main__":
    do_main()
