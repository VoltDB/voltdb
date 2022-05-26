# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
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

# All the commands supported by the Voter application.

import os

@VOLT.Command(description = 'Build the Voter application and catalog.',
              options = VOLT.BooleanOption('-C', '--conditional', 'conditional',
                                           'only build when the catalog file is missing'))
def build(runner):
    if not runner.opts.conditional or not os.path.exists('voter.jar'):
        runner.java.compile('obj', 'src/voter/*.java', 'src/voter/procedures/*.java')
    runner.call('volt.compile', '-c', 'obj', '-o', 'voter.jar', 'ddl.sql')

@VOLT.Command(description = 'Clean the Voter build output.')
def clean(runner):
    runner.shell('rm', '-rfv', 'obj', 'debugoutput', 'voter.jar', 'voltdbroot')

@VOLT.Server('create',
             description = 'Start the Voter VoltDB server.',
             command_arguments = 'voter.jar',
             classpath = 'obj')
def server(runner):
    runner.call('build', '-C')
    runner.go()

@VOLT.Java('voter.AsyncBenchmark', classpath = 'obj',
           description = 'Run the Voter asynchronous benchmark.')
def async(runner):
    runner.call('build', '-C')
    runner.go()

@VOLT.Java('voter.SyncBenchmark', classpath = 'obj',
           description = 'Run the Voter synchronous benchmark.')
def sync(runner):
    runner.call('build', '-C')
    runner.go()

@VOLT.Java('voter.JDBCBenchmark', classpath = 'obj',
           description = 'Run the Voter JDBC benchmark.')
def jdbc(runner):
    runner.call('build', '-C')
    runner.go()

@VOLT.Java('voter.SimpleBenchmark', classpath = 'obj',
           description = 'Run the Voter simple benchmark.')
def simple(runner):
    runner.call('build', '-C')
    runner.go()
