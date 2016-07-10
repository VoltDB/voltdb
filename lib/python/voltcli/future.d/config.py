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

@VOLT.Command(description = 'Configure project settings.',
              arguments = (
                  VOLT.StringArgument('keyvalue', 'KEY=VALUE assignment',
                                      min_count = 1, max_count = None),))
def config(runner):
    bad = []
    for arg in runner.opts.keyvalue:
        if arg.find('=') == -1:
            bad.append(arg)
    if bad:
        runner.abort('Bad arguments (must be KEY=VALUE format):', bad)
    for arg in runner.opts.keyvalue:
        key, value = [s.strip() for s in arg.split('=', 1)]
        # Default to 'volt.' if simple name is given.
        if key.find('.') == -1:
            key = 'volt.%s' % key
        runner.config.set_local(key, value)
        runner.info('Configuration: %s=%s' % (key, value))
