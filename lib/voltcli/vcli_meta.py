# This file is part of VoltDB.

# Copyright (C) 2008-2012 VoltDB Inc.
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

# Provides static meta-data.

__author__ = 'scooper'

import sys
import os

# IMPORTANT: Do not import vcli_* modules here. _util and _volt are internal
# and are guaranteed not to create circular dependencies.
import _util
import _volt

# Gets set through vcli_run.main()
version = "???"

bin_dir, bin_name = os.path.split(os.path.realpath(sys.argv[0]))
lib_dir = os.path.dirname(__file__)
verbs_subdir = '%s.d' % bin_name

# Add the voltcli directory to the Python module load path so that verb modules
# can import anything from here.
sys.path.insert(0, lib_dir)

# Verbs support the possible actions.
# Look for Verb subclasses in the .d subdirectory (relative to this file and to
# the working directory).
verbs = _util.find_and_load_subclasses(_volt.Verb,
                                       (lib_dir, '.'),
                                       verbs_subdir,
                                       VOLT = _volt)
verbs.sort(cmp = lambda x, y: cmp(x.name, y.name))

# Options define the CLI behavior modifiers.
cli = _volt.CLISpec(
    description = '''\
Specific actions are provided by subcommands.  Run "%prog help SUBCOMMAND" to
display full usage for a subcommand, including its options and arguments.  Note
that some subcommands are fully handled in Java.  For these subcommands help is
usually available by running the subcommand with no additional arguments or
followed by the -h option.
''',
    usage = '%prog [OPTIONS] COMMAND [ARGUMENTS ...]',
    options = (
        _volt.CLIOption('-d', '--debug', action = 'store_true', dest = 'debug',
                        help = 'display debug messages'),
        _volt.CLIOption('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                        help = 'dry run displays actions without executing them'),
        _volt.CLIOption('-p', '--pause', action = 'store_true', dest = 'pause',
                        help = 'pause before significant actions'),
        _volt.CLIOption('-v', '--verbose', action = 'store_true', dest = 'verbose',
                        help = 'display verbose messages, including external command lines'),
    )
)
