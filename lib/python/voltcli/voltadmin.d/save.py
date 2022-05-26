# This file is part of VoltDB.
# Copyright (C) 2008-2022 Volt Active Data Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.

import os
import urllib.request, urllib.parse, urllib.error
import sys

@VOLT.Command(
    bundles = VOLT.AdminBundle(),
    description = 'Save a VoltDB database snapshot.',
    options = (
        VOLT.BooleanOption('-b', '--blocking', 'blocking',
                           'block transactions and wait until the snapshot completes',
                           default = False),
        VOLT.EnumOption('-f', '--format', 'format',
                        'snapshot format', 'native', 'csv',
                        default = 'native'),
        VOLT.StringListOption(None, '--tables', 'tables',
                              'tables to include in the snapshot',
                              default = None),
        VOLT.StringListOption(None, '--skiptables', 'skip_tables',
                              'tables to skip in the snapshot',
                              default = None)
    ),
    arguments=(
            VOLT.PathArgument('directory', 'the snapshot server directory', absolute=True, optional=True),
            VOLT.StringArgument('nonce', 'the unique snapshot identifier (nonce)', optional=True)
    )
)
def save(runner):
    uri = None
    dir_specified = False
    if runner.opts.directory is not None:
        uri = 'file://%s' % urllib.parse.quote(runner.opts.directory)
        dir_specified = True

    nonce = None
    if runner.opts.nonce is not None:
        nonce = runner.opts.nonce.replace('"', '\\"')
    elif dir_specified:
        runner.abort('When a DIRECTORY is given a NONCE must be specified as well.')
    else:
        runner.opts.format = 'native'
        runner.opts.tables = None
        runner.opts.skip_tables = None

    if runner.opts.blocking:
        blocking = 'true'
    else:
        blocking = 'false'
    if uri is not None:
        if nonce is not None:
            raw_json_opts = ['uripath:"%s"' % (uri),
                             'nonce:"%s"' % (nonce),
                             'block:%s' % (blocking),
                             'format:"%s"' % (runner.opts.format)]
        else:
            raw_json_opts = ['uripath:"%s"' % (uri),
                             'block:%s' % (blocking),
                             'format:"%s"' % (runner.opts.format)]
    else:
        if nonce is not None:
            raw_json_opts = ['uripath:"%s"' % (uri),
                             'nonce:"%s"' % (nonce),
                             'block:%s' % (blocking),
                             'format:"%s"' % (runner.opts.format)]
        else:
            raw_json_opts = ['block:%s' % (blocking),
                             'format:"%s"' % (runner.opts.format)]
    if runner.opts.tables:
        raw_json_opts.append('tables:%s' % (runner.opts.tables))
    if runner.opts.skip_tables:
        raw_json_opts.append('skiptables:%s' % (runner.opts.skip_tables))
    runner.verbose_info('@SnapshotSave "%s"' % raw_json_opts)
    columns = [VOLT.FastSerializer.VOLTTYPE_STRING]
    response = runner.call_proc('@SnapshotSave', columns,
                                ['{%s}' % (','.join(raw_json_opts))])
    res_table = response.table(0)
    has_failure = any([t[3] != 'SUCCESS' for t in res_table.tuples()])
    print(res_table.format_table(caption = 'Snapshot Save Results'))
    if has_failure:
        sys.exit(1)
