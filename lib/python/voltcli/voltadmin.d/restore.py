# This file is part of VoltDB.

# Copyright (C) 2008-2015 VoltDB Inc.
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

import urllib

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Restore a VoltDB database snapshot.',
    options = (
        # Hidden option to restore the hashinator in addition to the tables.
        VOLT.BooleanOption('-S', '--hashinator', 'hashinator', None, default = False),
    ),
    arguments = (
        VOLT.PathArgument('directory', 'the snapshot server directory', absolute = True),
        VOLT.StringArgument('nonce', 'the unique snapshot identifier (nonce)')
    )
)
def restore(runner):
    nonce = runner.opts.nonce.replace('"', '\\"')
    if runner.opts.hashinator:
        hashinator = 'true'
    else:
        hashinator = 'false'
    json_opts = ['{path:"%s",nonce:"%s",hashinator:"%s"}' % (runner.opts.directory, nonce, hashinator)]
    runner.verbose_info('@SnapshotRestore "%s"' % json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    print 'voltadmin: Snapshot restore has been started. Check the server logs for ongoing status of the restore operation.'
    response = runner.call_proc('@SnapshotRestore', columns, json_opts)
    print response.table(0).format_table(caption = 'Snapshot Restore Results')
