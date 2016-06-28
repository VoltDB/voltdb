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
        VOLT.StringListOption('-H', '--hosts', 'mesh',
                              'HOST1[:PORT], HOST2:PORT, ..., HOSTn:PORT, (default HOST=localhost, PORT=3021)',
                              default = ''),
        VOLT.IntegerOption('-c', '--count', 'hostcount', 'number of hosts in the cluster', default=1)
    ),
    description = 'Starts a database, which has been initialized.'
)
def start(runner):
    if not runner.opts.mesh:
        runner.abort_with_help('You must specify the --cluster option.')
    runner.args.extend(['mesh', ','.join(runner.opts.mesh)])
    runner.args.extend(['hostcount', runner.opts.hostcount])
    runner.go()
