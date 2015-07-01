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
