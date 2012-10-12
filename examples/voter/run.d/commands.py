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

# All the commands supported by the "voter" command.

@Command(description = 'Build the Voter application and catalog.')
def build(runner):
    runner.mkdir('obj')
    runner.java_compile('obj', 'src/voter/*.java', 'src/voter/procedures/*.java')
    runner.volt.compile()

@Command(description = 'Build the Voter application and catalog only as needed.')
def build_as_needed(runner):
    if not runner.catalog_exists():
        build(runner)

@Command(description = 'Clean the Voter build output.')
def clean(runner):
    runner.shell('rm', '-rfv', 'obj', 'debugoutput', runner.get_catalog(), 'voltdbroot')

@Command(description = 'Start the Voter VoltDB server.')
def server(runner):
    runner.voltadmin.start()

@Java_Command('voter.JDBCBenchmark',
             description = 'Run the Voter JDBC benchmark.',
             depends = build_as_needed)
def jdbc(runner):
    runner.go()

@Java_Command('voter.SimpleBenchmark',
             description = 'Run the Voter simple benchmark.',
             depends = build_as_needed)
def simple(runner):
    runner.go()

@Java_Command('voter.AsyncBenchmark',
             description = 'Run the Voter asynchronous benchmark.',
             depends = build_as_needed)
def async(runner):
    runner.go()

@Java_Command('voter.SyncBenchmark',
             description = 'Run the Voter synchronous benchmark.',
             depends = build_as_needed)
def sync(runner):
    runner.go()
