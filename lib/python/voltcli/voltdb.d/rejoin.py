# This file is part of VoltDB.

# Copyright (C) 2008-2013 VoltDB Inc.
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

@VOLT.Command(
    description = 'Rejoin host to the VoltDB cluster.',
    options = (
        VOLT.StringOption ('-H', '--host', 'host',
                           'Host to Join the Cluster'),
        VOLT.BooleanOption ('-L', '--live', 'live',
                           'IS this a Live Rejoin?'),
    )
)
def rejoin(runner):
    if runner.opts.host == None: 
        runner.abort_with_help('Host must be specified in rejoin operation.')
    if runner.opts.live:
        runner.args.extend(['live', 'rejoin', 'host', runner.opts.host])
    else:
        runner.args.extend(['rejoin', 'host', runner.opts.host])
    
    runner.java.execute('org.voltdb.VoltDB', None, *runner.args)
