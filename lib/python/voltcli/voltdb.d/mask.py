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
    )
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

