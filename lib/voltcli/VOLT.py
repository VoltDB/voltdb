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

# Provides the VOLT namespace to commands.

__author__ = 'scooper'

# IMPORTANT: Do not import other voltcli modules here. vcli_util is the only one
# guaranteed not to create a circular dependency.
import vcli_util

#### CLI metadata class

class CLISpec(object):
    def __init__(self, **kwargs):
        if 'description' not in kwargs:
            self.description = '(description missing)'
        if 'usage' not in kwargs:
            self.usage = ''
        self._kwargs = kwargs
    def __getattr__(self, name):
        return self._kwargs.get(name, None)

#### Option class

class CLIOption(object):
    def __init__(self, *args, **kwargs):
        self.args   = args
        self.kwargs = kwargs

#### Verb class

class Verb(object):
    """
    Base class for verb implementations that provide the available sub-commands.
    """
    def __init__(self, name, **kwargs):
        self.name     = name
        self.metadata = CLISpec(**kwargs)
    def execute(self, runner):
        vcli_util.abort('%s "%s" object does not implement the required execute() method.'
                            % (self.__class__.__name__, self.name))
    def __cmp__(self, other):
        return cmp(self.name, other.name)
    def help(self, runner):
        runner.help(self.name)

#### FunctionVerb class

class FunctionVerb(Verb):
    """
    Verb that wraps any function. Used by @Command decorator.
    """
    def __init__(self, name, function, **kwargs):
        Verb.__init__(self, name, **kwargs)
        self.function = function
    def execute(self, runner):
        self.function(runner)

#### JavaFunctionVerb class

class JavaFunctionVerb(Verb):
    """
    Verb that wraps any function implemented in Java. Used by @JavaCommand decorator.
    """
    def __init__(self, name, java_class, function, **kwargs):
        Verb.__init__(self, name, **kwargs)
        self.java_class         = java_class
        self.function           = function
        self.java_opts_override = kwargs.get('java_opts_override', None)
    def execute(self, runner):
        runner.set_go_default(self.go)
        self.function(runner)
    def go(self, runner, *args):
        java_args = list(args) + list(runner.args)
        runner.java(self.java_class, self.java_opts_override, *java_args)
