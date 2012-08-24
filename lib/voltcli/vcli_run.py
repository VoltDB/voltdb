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

import vcli_util
import vcli_env
import vcli_project

#### Verb runner class

class VerbRunner(object):

    def __init__(self, name, options, args, parser, project_path):
        self.name         = name
        self.options      = options
        self.args         = args
        self.parser       = parser
        self.project_path = project_path
        self.classpath    = ':'.join(vcli_env.classpath)
        if project_path:
            if not os.path.exists(self.project_path):
                vlcli_util.abort('Project file "%s" does not exist.' % self.project_path)
            self.project = vcli_project.parse(self.project_path)
            cpext = self.project.get_config('classpath')
            if cpext:
                self.classpath += ':'.join((self.classpath, cpext))
        else:
            self.project = None

    def java(self, java_class, *args):
        java_args = [vcli_env.java]
        java_args.extend(vcli_env.java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.extend(('-classpath', self.classpath))
        java_args.append(java_class)
        java_args.extend(args)
        return vcli_util.run_cmd(*java_args)

    def run_cmd(self, *args):
        vcli_util.run_cmd(*args)

    def java_compile(self, outdir, *srcfiles):
        vcli_util.run_cmd('javac', '-target', '1.6', '-source', '1.6',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

    def abort(self, *msgs):
        vcli_util.error('Fatal error in "%s" command.' % self.name, *msgs)
        self.help()
        vcli_util.abort()

    def help(self, *args):
        print ''
        self.parser.print_help()
        print ''
