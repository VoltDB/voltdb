# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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

from voltcli import utility

collect_help = ('Collect logs on the current node for problem analysis.')

dir_spec_help = ('Specifies the root directory for the database. The default is the current working directory.')
output_help = ('Specifies the path and file name for the collect output file.')


@VOLT.Command(
    description = collect_help,
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
                           default = 7),
        VOLT.StringOption('-o', '--output', 'output', output_help, default=None),
        VOLT.StringOption('-D', '--dir', 'directory_spec', dir_spec_help, default=None),
        VOLT.BooleanOption('-f', '--force', 'force', 'Overwrites existing collect file.', default = False)
    ),
    arguments = (
        VOLT.PathArgument('voltdbroot', 'the voltdbroot path', absolute = True, optional=True, default=None)
    )
)

def collect(runner):
    if int(runner.opts.days) == 0:
        print >> sys.stderr, "ERROR: '0' is invalid entry for option --days"
        sys.exit(-1)

    process_voltdbroot_args(runner)
    process_outputfile_args(runner)

    runner.args.extend(['--host='+runner.opts.host, '--username='+runner.opts.username, '--password='+runner.opts.password,
    '--noprompt='+str(runner.opts.noprompt), '--dryrun='+str(runner.opts.dryrun), '--skipheapdump='+str(runner.opts.skipheapdump), '--days='+str(runner.opts.days)])

    runner.java_execute('org.voltdb.utils.Collector', None, *runner.args)


def process_voltdbroot_args(runner) :
    if (runner.opts.directory_spec) and (runner.opts.voltdbroot):
        utility.abort("Provide either root directory of for the database or the voltdbroot path.")

    print(os.environ["PATH"])
    os.environ["PATH"] += os.pathsep + os.pathsep.join(s for s in sys.path if os.path.join("voltdb", "bin") in s)
    print(os.environ["PATH"])

    databaseDir = ''
    if runner.opts.directory_spec:
        runner.args.extend(['--databaseDir='+runner.opts.directory_spec])
        databaseDir = runner.opts.directory_spec
        if os.path.isdir(runner.opts.directory_spec) and os.access(runner.opts.directory_spec, os.R_OK|os.W_OK|os.X_OK):
            databaseDir = os.path.join(runner.opts.directory_spec + 'voltdbroot')
        else:
            print >> sys.stderr, "ERROR: specified database directory is not valid", runner.opts.directory_spec
            sys.exit(-1)
        print ("process the directory spec %s, system check parent dir %s" % (runner.opts.directory_spec, databaseDir))
    elif(runner.opts.voltdbroot):
        databaseDir = runner.opts.voltdbroot
        runner.args.extend(['--voltdbroot='+runner.opts.voltdbroot])
        print ("process the voltdbroot %s, system check parent dir %s" % (runner.opts.voltdbroot, databaseDir))
    else:
        systemCheckParentDir = os.path.join(os.getcwd(), 'voltdbroot')
        print ("no dir specified, system check parent dir will be %s" % databaseDir)

    performSystemCheck(databaseDir)



def process_outputfile_args(runner):
    if runner.opts.output and runner.opts.prefix:
        utility.abort("Provide either root output filename or prefix to filename.")

    if runner.opts.output:
        runner.args.extend(['--outputFile='+runner.opts.output])
    elif runner.opts.prefix:
        runner.args.extend(['--prefix='+runner.opts.prefix])

def performSystemCheck(dirPath):
    if os.path.isdir(dirPath) and os.access(dirPath, os.R_OK|os.W_OK|os.X_OK):
        checkFD = os.open(os.path.join(dirPath, "systemcheck"), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
        checkOutput = os.fdopen(checkFD, 'w')
        subprocess.call("voltdb check", stdout=checkOutput, shell=True)
        checkOutput.close()
    else:
        print >> sys.stderr, "ERROR: Invalid voltdbroot path", dirPath
        sys.exit(-1);
