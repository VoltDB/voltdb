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

import sys, os, subprocess

from voltcli import utility

collect_help = ('Collect logs on the current node for problem analysis')

dir_spec_help = ('root directory for the database. The default is the current working directory.')
output_help = ('file name to store collect data in compressed format. \'-\' means standard output. '
                'The default is the \'voltdb_collect_<hostname or IP>.zip\' in the current working directory.')


@VOLT.Command(
    description = collect_help,
    options = (
        VOLT.StringOption (None, '--prefix', 'prefix',
                           'file name prefix for uniquely identifying collection. (Deprecated. Please use --output).',
                           default = ''),
        VOLT.PathOption('-o', '--output', 'output', output_help, default=''),
        VOLT.BooleanOption(None, '--dry-run', 'dryrun',
                           'list the log files without collecting them.',
                           default = False),
        VOLT.BooleanOption(None, '--skip-heap-dump', 'skipheapdump',
                           'exclude heap dump file from collection.',
                           default = True),
        VOLT.IntegerOption(None, '--days', 'days',
                           'number of days of files to collect (files included are log, crash files), Current day value is 1.',
                           default = 7),
        VOLT.PathOption('-D', '--dir', 'directory_spec', dir_spec_help, default=''),
        VOLT.BooleanOption('-f', '--force', 'force', 'Overwrite the existing file.', default = False),
    ),
    arguments = (
        VOLT.PathArgument('voltdbroot', 'the voltdbroot path. (Deprecated. Please use --dir).', absolute = True, optional=True, default=None)
    ),
    log4j_default = ('utility-log4j.xml', 'log4j.xml')
)

def collect(runner):
    if int(runner.opts.days) == 0:
        utility.abort(' \'0\' is invalid entry for option --days')

    process_voltdbroot_args(runner)
    process_outputfile_args(runner)

    runner.args.extend(['--dryrun=' + str(runner.opts.dryrun), '--skipheapdump=' + str(runner.opts.skipheapdump),
                        '--days=' + str(runner.opts.days), '--force=' + str(runner.opts.force)])
    runner.java_execute('org.voltdb.utils.Collector', None, *runner.args)


def process_voltdbroot_args(runner) :
    if (runner.opts.directory_spec) and (runner.opts.voltdbroot):
        utility.abort('Cannot specify both --dir and command line argument. Please use --dir option.')

    os.environ['PATH'] += os.pathsep + os.pathsep.join(s for s in sys.path if os.path.join('voltdb', 'bin') in s)
    # If database directory is given, derive voltdbroot path to store results of systemcheck in voltdbroot directory
    if runner.opts.directory_spec:
        if os.path.isdir(runner.opts.directory_spec) and os.access(runner.opts.directory_spec, os.R_OK|os.W_OK|os.X_OK):
            voltdbrootDir = os.path.join(runner.opts.directory_spec, 'voltdbroot')
        else:
            utility.abort('Specified database directory is not valid', runner.opts.directory_spec)
    elif runner.opts.voltdbroot:
        utility.warning('Specifying voltdbroot directory using command argument is deprecated. Consider using --dir '
                        'option to specify database directory.');
        voltdbrootDir = runner.opts.voltdbroot
    else:
        voltdbrootDir = os.path.join(os.getcwd(), 'voltdbroot')

    runner.args.extend(['--voltdbroot=' + voltdbrootDir])
    performSystemCheck(runner, voltdbrootDir)


def performSystemCheck(runner, dirPath):
    if os.path.isdir(dirPath) and os.access(dirPath, os.R_OK|os.W_OK|os.X_OK):
        checkFD = os.open(os.path.join(dirPath, 'systemcheck'), os.O_WRONLY|os.O_CREAT|os.O_TRUNC)
        checkOutput = os.fdopen(checkFD, 'w')
        subprocess.call('voltdb check', stdout=checkOutput, shell=True)
        checkOutput.close()
    else:
        if runner.opts.directory_spec:
            utility.abort('Invalid database directory ' + runner.opts.directory_spec +
                          '. Specify valid database directory using --dir option.')
        elif runner.opts.voltdbroot:
            utility.abort('Invalid voltdbroot path ' + runner.opts.voltdbroot +
                          '. Specify valid database directory using --dir option.')
        else:
            utility.abort('Invalid database directory ' + os.getcwd() +
                          '. Specify valid database directory using --dir option.')

def process_outputfile_args(runner):
    if runner.opts.output and runner.opts.prefix:
        utility.abort('Cannot specify both --output and --prefix. Please use --output option.')

    if runner.opts.output:
        runner.args.extend(['--outputFile=' + runner.opts.output])
    elif runner.opts.prefix:
        utility.warning('Specifying prefix for outputfile name is deprecated. Consider using --output option to specify'
                        ' output file name.')
        runner.args.extend(['--prefix=' + runner.opts.prefix])
