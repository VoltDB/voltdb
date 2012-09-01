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

# IMPORTANT: Do not import other voltcli modules here. _util is the only one
# guaranteed not to create a circular dependency.
import _util

version = "0.9"
version_string = '%%prog version %s' % version

bin_dir, bin_name = os.path.split(os.path.realpath(sys.argv[0]))
lib_dir = os.path.dirname(__file__)
verbs_subdir = 'vcli.d'

# Add the voltcli directory to the Python module load path so that verb modules
# can import anything from here.
sys.path.insert(0, lib_dir)

#### Metadata class

class CLISpec(object):
    def __init__(self, **kwargs):
        if 'description' not in kwargs or 'usage' not in kwargs:
            _util.abort('Metadata must have both "description" and "usage" members.')
        self._kwargs = kwargs
    def __getattr__(self, name):
        return self._kwargs.get(name, None)

#### Option class

class CLIOption(object):
    def __init__(self, *args, **kwargs):
        self.args   = args
        self.kwargs = kwargs

#### Verbs

class BaseVerb(object):
    def __init__(self, name, project_needed, **kwargs):
        self.name           = name
        self.project_needed = project_needed
        self.metadata       = CLISpec(**kwargs)
    def execute(self, env):
        _util.abort('%s "%s" object does not implement the required execute() method.'
                        % (self.__class__.__name__, self.name))

class Verb(BaseVerb):
    """
    ProjectVerb should be used for verbs that run outside the context of a
    project. They must not need information from project.xml files.
    """
    def __init__(self, name, **kwargs):
        BaseVerb.__init__(self, name, False, **kwargs)

class ProjectVerb(BaseVerb):
    """
    ProjectVerb should be used for verbs that run inside the context of a
    project. They likely will need information from project.xml files.
    """
    def __init__(self, name, **kwargs):
        BaseVerb.__init__(self, name, True, **kwargs)

# Verbs support the possible actions.
# Look for Verb subclasses in the .d subdirectory (relative to this file and to
# the working directory). Also add ProjectVerb to the symbol table provided to
# verb modules.
verbs = _util.find_and_load_subclasses(BaseVerb, (lib_dir, '.'), verbs_subdir,
                                       Verb = Verb,
                                       ProjectVerb = ProjectVerb)
verbs.sort(cmp = lambda x, y: cmp(x.name, y.name))

# Options define the CLI behavior modifiers.
cli = CLISpec(
    description = 'This serves as a comprehensive command line interface to VoltDB.',
    usage       = '%prog [OPTIONS] COMMAND [ARGUMENTS ...]',
    options     = (
        CLIOption('-d', '--debug', action = 'store_true', dest = 'debug',
                  help = 'display debug messages'),
        CLIOption('-n', '--dry-run', action = 'store_true', dest = 'dryrun',
                  help = 'perform dry run without executing actions'),
        CLIOption('-p', '--pause', action = 'store_true', dest = 'pause',
                  help = 'pause before significant actions'),
        CLIOption('-v', '--verbose', action = 'store_true', dest = 'verbose',
                  help = 'display verbose messages, including external command lines'),
    )
)
