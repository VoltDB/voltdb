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

__author__ = 'scooper'

import os
import subprocess

import vcli_util
import vcli_env
import vcli_opt

def run_cmd(cmd, *args):
    """Run external program without capturing or suppressing output and check return code."""
    fullcmd = cmd
    for arg in args:
        if len(arg.split()) > 1:
            fullcmd += ' "%s"' % arg
        else:
            fullcmd += ' %s' % arg
    if vcli_opt.dryrun:
        print fullcmd
    else:
        retcode = os.system(fullcmd)
        if retcode != 0:
            vcli_util.abort('return code %d: %s' % (retcode, fullcmd))

def pipe_cmd(*args):
    """Run external program, capture its output, and yield each output line for iteration."""
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in iter(proc.stdout.readline, ''):
            yield line.rstrip()
        proc.stdout.close()
    except Exception, e:
        vcli_util.warning('Exception running command: %s' % ' '.join(args), e)

#### Verb runner class

class VerbRunner(object):

    def __init__(self, name, options, args, parser):
        self.name      = name
        self.options   = options
        self.args      = args
        self.parser    = parser
        self.classpath = vcli_env.classpath

    def abort(self, *msgs):
        vcli_util.error('Fatal error in "%s" command.' % self.name, *msgs)
        print ''
        self.parser.print_help()
        print ''
        vcli_util.abort()

    def classpath_prepend(self, *paths):
        self.classpath = list(paths) + self.classpath

    def classpath_append(self, *paths):
        self.classpath = self.classpath + list(paths)

    def java(self, java_class, *args):
        java_args = [vcli_env.java]
        java_args.extend(vcli_env.java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.append('-classpath')
        java_args.append(':'.join(self.classpath))
        java_args.append(java_class)
        java_args.extend(args)
        return run_cmd(*java_args)