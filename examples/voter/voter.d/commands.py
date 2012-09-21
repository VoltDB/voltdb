# This file is part of VoltDB.
# Copyright (C) 2008-2012 VoltDB Inc.
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
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

# Contains all the commands provided by the "voter" command.

import os
import vcli_util

class VoterAsync(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'async',
                           description = 'Run the Voter asynchronous benchmark. Use --help for usage.',
                           passthrough = True)
    def execute(self, runner):
        if not os.path.exists('voter.jar'):
            runner.run('compile')
        runner.java('voter.AsyncBenchmark', None, *runner.args)

class VoterJDBC(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'jdbc',
                           description = 'Run the Voter JDBC benchmark. Use --help for usage.',
                           passthrough = True)
    def execute(self, runner):
        if not os.path.exists('voter.jar'):
            runner.run('compile')
        runner.java('voter.JDBCBenchmark', None, *runner.args)

class VoterBenchmarkSimple(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'simple',
                           description = 'Run the Voter simple benchmark. Use --help for usage.',
                           passthrough = True)
    def execute(self, runner):
        if not os.path.exists('voter.jar'):
            runner.run('compile')
        runner.java('voter.SimpleBenchmark', None, 'localhost', *runner.args)

class VoterBenchmarkSync(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'sync',
                           description = 'Run the Voter synchronous benchmark. Use --help for usage.',
                           passthrough = True)
    def execute(self, runner):
        if not os.path.exists('voter.jar'):
            runner.run('compile')
        runner.java('voter.SyncBenchmark', None, *runner.args)

class VoterClean(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'clean',
                           description = 'Clean Voter build output.')
    def execute(self, runner):
        catalog = runner.config.get_required('volt', 'catalog')
        runner.shell('rm', '-rfv', 'obj', 'debugoutput', catalog, 'voltdbroot')

class VoterCompile(VOLT.Verb):
    def __init__(self):
        VOLT.Verb.__init__(self, 'compile',
                           description = 'Build the Voter application and catalog.')
    def execute(self, runner):
        runner.shell('mkdir', '-p', 'obj')
        vcli_util.info('Compiling application...')
        runner.java_compile('obj', 'src/voter/*.java', 'src/voter/procedures/*.java')
        vcli_util.info('Compiling catalog...')
        runner.run('compile')
        vcli_util.info('Voter compilation succeeded.')
