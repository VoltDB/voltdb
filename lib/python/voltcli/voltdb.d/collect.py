import sys, os, subprocess
# This file is part of VoltDB.

# Copyright (C) 2008-2016 VoltDB Inc.
#
# This file contains original code and/or modifications of original code.
# Any modifications made by VoltDB Inc. are licensed under the following
# terms and conditions:
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

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

@VOLT.Command(
    description = 'Collect logs on the current node for problem analysis.',
    options = (
        VOLT.StringOption (None, '--prefix', 'prefix',
                           'file name prefix for uniquely identifying collection',
                           default = 'voltdb_logs'),
        VOLT.StringOption (None, '--upload', 'host',
                           'upload resulting collection to HOST via SFTP',
                           default = ''),
        VOLT.StringOption (None, '--username', 'username',
                           'user name for SFTP upload',
                           default = ''),
        VOLT.StringOption (None, '--password', 'password',
                           'password for SFTP upload',
                           default = ''),
        VOLT.BooleanOption(None, '--no-prompt', 'noprompt',
                           'automatically upload collection (without user prompt)',
                           default = False),
        VOLT.BooleanOption(None, '--dry-run', 'dryrun',
                           'list the log files without collecting them',
                           default = False),
        VOLT.BooleanOption(None, '--skip-heap-dump', 'skipheapdump',
                           'exclude heap dump file from collection',
                           default = False),
        VOLT.IntegerOption(None, '--days', 'days',
                           'number of days of files to collect (files included are log, crash files), Current day value is 1',
                           default = 14)
    ),
    arguments = (
        VOLT.PathArgument('voltdbroot', 'the voltdbroot path', absolute = True)
    )
)

def collect(runner):
    if int(runner.opts.days) == 0:
	print >> sys.stderr, "ERROR: '0' is invalid entry for option --days"
        sys.exit(-1)

    os.environ["PATH"] += os.pathsep + os.pathsep.join(s for s in sys.path if os.path.join("voltdb", "bin") in s)
    checkFD = os.open(os.path.join(runner.opts.voltdbroot, "systemcheck"), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
    checkOutput = os.fdopen(checkFD, 'w')
    subprocess.call("voltdb check", stdout=checkOutput, shell=True)
    checkOutput.close()

    runner.args.extend(['--voltdbroot='+runner.opts.voltdbroot, '--prefix='+runner.opts.prefix, '--host='+runner.opts.host, '--username='+runner.opts.username, '--password='+runner.opts.password,
    '--noprompt='+str(runner.opts.noprompt), '--dryrun='+str(runner.opts.dryrun), '--skipheapdump='+str(runner.opts.skipheapdump), '--days='+str(runner.opts.days)])
    runner.java_execute('org.voltdb.utils.Collector', None, *runner.args)
