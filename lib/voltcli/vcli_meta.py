# This file is part of VoltDB.

# Copyright (C) 2008-2011 VoltDB Inc.
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

# IMPORTANT: Do not import other voltcli modules here. simpleutils is the
# only one guaranteed not to create a circular dependency.
import _util

version = "0.9"
version_string = '%%prog version %s' % version

mydir, myname = os.path.split(os.path.realpath(sys.argv[0]))

#### Metadata class

class CLISpec(object):
    def __init__(self, **kwargs):
        if 'description' not in kwargs or 'usage' not in kwargs:
            _util.abort('Metadata must have both "description" and "usage" members.')
        self._kwargs = kwargs
    def __getattr__(self, name):
        return self._kwargs.get(name, None)

#### Verb: abstract base class

#TODO: Verbs should be compiled dynamically by scanning the verbs.d directory.

class Verb(object):
    def __init__(self, name, **kwargs):
        self.name     = name
        self.metadata = CLISpec(**kwargs)
    def execute(self, env):
        _util.abort('Verb "%s" object does not implement the required execute() method.' % self.name)

#### Verb: compile

class VerbCompile(Verb):
    def __init__(self):
        Verb.__init__(self, 'compile',
                      description = 'Run the VoltDB compiler to build the catalog',
                      usage       = '%prog compile CLASSPATH PROJECT JAR')
    def execute(self, runner):
        if len(runner.args) != 3:
            runner.abort('3 arguments are required, %d were provided.' % len(runner.args))
        runner.classpath_prepend(runner.args[0])
        runner.classpath_append(runner.args[0])
        runner.java('org.voltdb.compiler.VoltCompiler', *runner.args[1:])

#### Verb: start

class VerbStart(Verb):
    def __init__(self):
        Verb.__init__(self, 'start',
                      description = 'Start the VoltDB server',
                      usage       = '%prog start [OPTIONS] JAR')
    def execute(self, env):
        pass

#### Option class

class CLIOption(object):
    def __init__(self, *args, **kwargs):
        self.args   = args
        self.kwargs = kwargs

# Verbs support the possible actions.
verbs = (
    VerbCompile(),
    VerbStart(),
    )

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
