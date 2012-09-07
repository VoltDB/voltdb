# This file is part of VoltDB.

# Copyright (C) 2008-2012 VoltDB Inc.
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

import vcli_util

class VerbShow(Verb):
    def __init__(self):
        Verb.__init__(self, 'configshow',
                      description = 'Show project settings.',
                      usage       = '[NAME ...]')
    def execute(self, runner):
        if not runner.args:
            # All labels.
            for section in runner.config.sections():
                for name, value in runner.config.items(section):
                    print '%s.%s=%s' % (section, name, value)
        else:
            # Specific section.name items requested.
            for full_name in runner.args:
                try:
                    section, name = full_name.split('.')
                except ValueError:
                    vcli_util.abort('Option name format is section.name, e.g. volt.catalog.')
                value = runner.config.get(section, name)
                if value is None:
                    print '%s.%s=%s  *not found*' % (section, name, value)
                else:
                    print '%s.%s=%s' % (section, name, value)
