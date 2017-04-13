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

def reset_remote(runner):
    result = runner.call_proc('@ResetDR', [VOLT.FastSerializer.VOLTTYPE_TINYINT, VOLT.FastSerializer.VOLTTYPE_TINYINT], [runner.opts.clusterId, runner.opts.forcing * 1]).table(0)
    status = result.tuple(0).column_integer(0)
    message = result.tuple(0).column_string(1)
    if status == 0:
        runner.info(message)
    else:
        runner.error(message)

@VOLT.Multi_Command(
    bundles = VOLT.AdminBundle(),
    description = 'DR control command.',
    options = (
            VOLT.BooleanOption('-f', '--force', 'forcing', 'bypass precheck', default = False),
            VOLT.IntegerOption('-c', '--cluster', 'clusterId', 'dr cluster Id', default = -1),
    ),
    modifiers = (
            VOLT.Modifier('reset', reset_remote, 'reset one/all remote dr cluster(s).'),
    )
)

def dr(runner):
    runner.go()
