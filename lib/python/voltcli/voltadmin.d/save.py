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

import os
import urllib
from voltcli import utility

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Save a VoltDB database snapshot.',
    options = (
        VOLT.BooleanOption('-b', '--blocking', 'blocking',
                           'block transactions and wait until the snapshot completes',
                           default = False),
        VOLT.EnumOption('-f', '--format', 'format',
                        'snapshot format', 'native', 'csv',
                        default = 'native')
    ),
    arguments = (
        VOLT.StringArgument('directory', 'the local snapshot directory path'),
        VOLT.StringArgument('nonce', 'the unique snapshot identifier (nonce)')
    )
)
def save(runner):
    uri   = 'file://%s' % urllib.quote(os.path.realpath(runner.opts.directory))
    nonce = runner.opts.nonce.replace('"', '\\"')
    if runner.opts.blocking:
        blocking = 'true'
    else:
        blocking = 'false'
    json_opts = ['{uripath:"%s",nonce:"%s",block:%s,format:"%s"}'
                    % (uri, nonce, blocking, runner.opts.format)]
    utility.verbose_info('@SnapshotSave "%s"' % json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    response = runner.call_proc('@SnapshotSave', columns, json_opts)
    print response.table(0).format_table(caption = 'Snapshot Save Results')
