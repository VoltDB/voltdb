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

# Main Java Class
CatalogPasswordScrambler = 'org.voltdb.utils.CatalogPasswordScrambler'

@VOLT.Command(
    # Descriptions for help screen.
    description  = 'Mask user passwords in VoltDB deployment file.',
    description2 = 'At least one deployment file is required.',

    # Command line arguments.
    arguments = (
        VOLT.PathArgument(
            'deploymentfile',
            'Source and optionally a destination masked deployment file(s)',
            min_count=1, max_count=2
            )
    ),

    # Console-only logging
    log4j_default = ('utility-log4j.xml', 'log4j.xml')
)

# Command implementation
def mask(runner):

    # Check that there's something to compile.
    if not runner.opts.deploymentfile:
        runner.abort_with_help('At least one deployment file must be specified.')

    # Verbose argument display.
    if runner.is_verbose():
        params = ['Deployment file: %s' % runner.opts.deploymentfile[0]]
        if len(runner.opts.deploymentfile) == 2:
            params.append('Masked deployment file: %s' % runner.opts.deploymentfile[1])
        runner.verbose_info('Mask parameters:', params)

    # Build the positional and keyword argument lists and invoke the scrambler
    args = runner.opts.deploymentfile
    runner.java_execute(CatalogPasswordScrambler, None, *args)

