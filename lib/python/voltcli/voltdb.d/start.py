# This file is part of VoltDB.
# Copyright (C) 2008-2016 VoltDB Inc.
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

voltdbroot_help = ('Specifies the root directory for the database. The default '
                   'is voltdbroot under the current working directory.')
server_list_help = ('{hostname-or-ip[,...]}, '
             'Specifies the leader(s) for coordinating cluster startup. ')

@VOLT.Command(
    bundles = VOLT.ServerBundle('probe',
                                needs_catalog=False,
                                supports_live=False,
                                default_host=False,
                                safemode_available=True,
                                supports_daemon=True,
                                supports_multiple_daemons=True,
                                check_environment_config=True,
                                force_voltdb_create=False,
                                supports_paused=True,
                                is_legacy_verb=False),
    options = (
        VOLT.StringListOption('-H', '--host', 'server_list', server_list_help, default = ''),
        VOLT.IntegerOption('-c', '--count', 'hostcount', 'number of hosts in the cluster'),
        VOLT.StringOption('-D', '--dir', 'directory_spec', voltdbroot_help, default = None),
        VOLT.BooleanOption('-r', '--replica', 'replica', 'start replica cluster', default = False),
        VOLT.BooleanOption('-A', '--add', 'enableadd', 'allows the server to elastically expand the cluster if the cluster is already complete', default = False),
        VOLT.IntegerOption('-s', '--sitesperhost', 'sitesperhost', None),
    ),
    description = 'Starts a database, which has been initialized.'
)
def start(runner):
    if runner.opts.directory_spec:
        runner.args.extend(['voltdbroot', runner.opts.directory_spec])
    if not runner.opts.server_list:
        runner.abort_with_help('You must specify the --host option.')
    runner.args.extend(['mesh', ','.join(runner.opts.server_list)])
    if runner.opts.hostcount:
        runner.args.extend(['hostcount', runner.opts.hostcount])
    if runner.opts.sitesperhost:
        runner.args.extend(['sitesperhost', runner.opts.sitesperhost])
    if runner.opts.enableadd:
        runner.args.extend(['enableadd'])
    runner.go()
