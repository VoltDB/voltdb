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

import sys, os, subprocess

@VOLT.Command(
    description = 'Collect logs on the current node for problem analysis.',
    options = (
        VOLT.StringOption (None, '--prefix', 'prefix',
                           'file name prefix for uniquely identifying collection',
                           default = ''),
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
                           default = True),
        VOLT.IntegerOption(None, '--days', 'days',
                           'number of days of files to collect (files included are log, crash files), Current day value is 1',
                           default = 7)
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
    if os.path.isdir(runner.opts.voltdbroot) and os.access(runner.opts.voltdbroot, os.R_OK|os.W_OK|os.X_OK):
        checkFD = os.open(os.path.join(runner.opts.voltdbroot, "systemcheck"), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
        checkOutput = os.fdopen(checkFD, 'w')
        subprocess.call("voltdb check", stdout=checkOutput, shell=True)
        checkOutput.close()
    else:
        print >> sys.stderr, "ERROR: Invalid voltdbroot path", runner.opts.voltdbroot
        sys.exit(-1);

    runner.args.extend(['--voltdbroot='+runner.opts.voltdbroot, '--prefix='+runner.opts.prefix, '--host='+runner.opts.host, '--username='+runner.opts.username, '--password='+runner.opts.password,
    '--noprompt='+str(runner.opts.noprompt), '--dryrun='+str(runner.opts.dryrun), '--skipheapdump='+str(runner.opts.skipheapdump), '--days='+str(runner.opts.days)])
    runner.java_execute('org.voltdb.utils.Collector', None, *runner.args)
