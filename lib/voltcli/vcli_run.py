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

__author__ = 'scooper'

import sys
import os
import copy

import vcli_meta
import vcli_opt
import vcli_env
import vcli_util
import vcli_project

#### Verb runner class

class VerbRunner(object):

    def __init__(self, name, options, args, main_parser, verb_parser, project_path):
        self.name         = name
        self.options      = options
        self.args         = args
        self.main_parser  = main_parser
        self.verb_parser  = verb_parser
        self.project_path = project_path
        self.classpath    = ':'.join(vcli_env.classpath)
        if self.project_path:
            if not os.path.exists(self.project_path):
                vlcli_util.abort('Project file "%s" does not exist.' % self.project_path)
            self.project = vcli_project.parse(self.project_path)
            cpext = self.project.get_config('classpath')
            if cpext:
                self.classpath += ':'.join((self.classpath, cpext))
        else:
            self.project = None

    def java(self, java_class, java_opts_override, *args):
        """
        Run a Java command line with option overrides.
        """
        java_args = [vcli_env.java]
        java_opts = vcli_util.merge_java_options(vcli_env.java_opts, java_opts_override)
        java_args.extend(java_opts)
        java_args.append('-Dlog4j.configuration=file://%s' % os.environ['LOG4J_CONFIG_PATH'])
        java_args.extend(('-classpath', self.classpath))
        java_args.append(java_class)
        java_args.extend(args)
        return vcli_util.run_cmd(*java_args)

    def volt(self, name, options, args):
        runner = VerbRunner(name, options, args, self.main_parser, self.verb_parser, self.project_path)
        for verb in vcli_meta.verbs:
            if verb.name == name:
                break
        else:
            abort('Verb "%s" (being called by "%s") was not found.' % (name, self.name))
        verb.execute(runner)

    def shell(self, *args):
        """
        Run a shell command.
        """
        vcli_util.run_cmd(*args)

    def java_compile(self, outdir, *srcfiles):
        vcli_util.run_cmd('javac', '-target', '1.6', '-source', '1.6',
                          '-classpath', self.classpath, '-d', outdir, *srcfiles)

    def get_project(self):
        if self.project is None:
            vcli_util.abort('Verb "%s" is not configured to need a project file, but is asking for one.' % self.name)
        return self.project

    def get_catalog(self):
        catalog = self.get_project().get_config('catalog', required = True)
        # Compile it if the catalog doesn't exist.
        if not os.path.exists(catalog):
            self.volt('compile')

    def abort(self, *msgs):
        vcli_util.error('Fatal error in "%s" command.' % self.name, *msgs)
        self.help()
        vcli_util.abort()

    def help(self, *args):
        for name in args:
            for verb in vcli_meta.verbs:
                if verb.name == name:
                    print ''
                    self.verb_parser.print_help()
                    print ''
                else:
                    error('Verb "%s" was not found.' % (name, self.name))

    def usage(self, *args):
        print ''
        self.main_parser.print_help()
        print ''

def main():
    """
    Called by main script to run using command line arguments.
    """
    # Initialize the environment
    vcli_env.initialize()

    # Run the command
    volt(*sys.argv[1:])

def volt(*cmdargs):
    """
    Run a command after parsing the command line arguments provided.
    """
    # Parse the command line.
    parser = vcli_opt.VoltCLIOptionParser(vcli_meta.verbs,
                                          vcli_meta.cli.options,
                                          vcli_meta.cli.usage,
                                          vcli_meta.cli.description,
                                          vcli_meta.version_string)
    verb, options, args, main_parser, verb_parser = parser.parse(cmdargs)

    # Run the command.
    if verb.project_needed:
        project_path = os.path.join(os.getcwd(), 'project.xml')
        runner = VerbRunner(verb.name, options, args, main_parser, verb_parser, project_path)
    else:
        runner = VerbRunner(verb.name, options, args, main_parser, verb_parser, None)
    verb.execute(runner)
