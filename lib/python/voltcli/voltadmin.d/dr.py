# This file is part of VoltDB.
# Copyright (C) 2008-2017 VoltDB Inc.
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

def reset_local(runner):
    status = runner.call_proc('@ResetDR', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT], [runner.opts.clusterId, runner.opts.clusterId * 1]).table(0).tuple(0).column_integer(0)
    if status == 0:
        runner.info('Conversation log is reset.')
    else:
        runner.error('The cluster failed to reset conversation log with status: %d' % status)

def reset_remote(runner):
    status = runner.call_proc('@ResetDR', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT], [runner.opts.clusterId, runner.opts.clusterId * 1]).table(0).tuple(0).column_integer(0)
    if status == 0:
        runner.info('Conversation log is reset.')
    else:
        runner.error('The cluster failed to reset conversation log with status: %d' % status)

@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'DR control command.',
    options = (
            VOLT.BooleanOption('-f', '--force', 'forcing', 'bypass precheck', default = False),
            VOLT.BooleanOption('-c', '--cluster', 'clusterId', 'drCluster Id', default = -1),
    ),
    modifiers = (
            VOLT.Modifier('resetlocal', reset_local, 'remove local cluster from its dr cluster mesh.'),
            VOLT.Modifier('reset', reset_remote, 'reset one/all remote dr cluster(s).'),
    )
)

def dr(runner):
    runner.go()
