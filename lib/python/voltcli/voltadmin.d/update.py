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

import os
from voltcli import utility

@VOLT.Command(
    bundles=VOLT.AdminBundle(),
    description='Update the schema of a running database.',
    description2='Either a catalog (extension .jar), a deployment file (extension .xml), or both, must be provided.',
    arguments=(
        VOLT.StringArgument(
            'catalog_or_deployment',
            'application catalog (extension .jar) and/or deployment configuration (extension .xml) file path(s)',
            min_count=1, max_count=2),
    )
)
def update(runner):
    columns = [VOLT.FastSerializer.VOLTTYPE_NULL, VOLT.FastSerializer.VOLTTYPE_NULL]
    catalog = None
    deployment = None
    for catalog_or_deployment in runner.opts.catalog_or_deployment:
        extension = os.path.splitext(catalog_or_deployment)[1].lower()
        if extension == '.jar':
            if not catalog is None:
                runner.abort('More than one catalog .jar file was specified.')
            catalog = VOLT.utility.File(catalog_or_deployment).read_hex()
            columns[0] = VOLT.FastSerializer.VOLTTYPE_STRING
        elif extension == '.xml':
            if not deployment is None:
                runner.abort('More than one deployment .xml file was specified.')
            deployment = VOLT.utility.File(catalog_or_deployment).read()
            columns[1] = VOLT.FastSerializer.VOLTTYPE_STRING
        else:
            runner.abort(
                'The "%s" extension is not recognized as either a catalog or a deployment file'
                    % extension)
    if catalog is None and deployment is None:
        runner.abort('At least one catalog .jar or deployment .xml file is required.')
    params = [catalog, deployment]
    # call_proc() aborts with an error if the update failed.
    runner.call_proc('@UpdateApplicationCatalog', columns, params)
    runner.info('The catalog update succeeded.')
