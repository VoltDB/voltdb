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

__author__ = 'scooper'

import os
import copy
import ConfigParser
import _util

# Configuration object

class PersistentConfig(ConfigParser.SafeConfigParser):
    """Persistent access to configuration data."""

    def __init__(self, path):
        self.path = path
        self.loaded = False
        ConfigParser.SafeConfigParser.__init__(self)

    def load(self):
        self.read(self.path)
        self.loaded = True

    def save(self):
        with open(self.path, 'w') as f:
            ConfigParser.SafeConfigParser.write(self, f)

    def get(self, section, option):
        if not self.loaded:
            self.load()
        try:
            return ConfigParser.SafeConfigParser.get(self, section, option)
        except ConfigParser.NoSectionError:
            return None
        except ConfigParser.NoOptionError:
            return None

    def get_required(self, section, option):
        if not self.loaded:
            self.load()
        value = self.get(section, option)
        if value is None:
            self.abort('Configuration parameter "%s.%s" was not found.' % (section, option),
                       'Set parameters using the "config" command.')
        return value

    def items(self, section):
        if not self.loaded:
            self.load()
        try:
            items = ConfigParser.SafeConfigParser.items(self, section)
            items.sort(cmp = lambda x, y: cmp(x[0], y[0]))
            return items
        except ConfigParser.NoSectionError:
            return []

    def set(self, section, option, value):
        if not self.loaded:
            self.load()
        if not self.has_section(section):
            self.add_section(section)
        ConfigParser.SafeConfigParser.set(self, section, option, value)
        self.save()

    def __enter__(self):
        self.load()

    def __exit__(self, type, value, traceback):
        self.save()

    def abort(self, *msgs_in):
        msgs = list(copy.copy(msgs_in))
        if not os.path.exists(self.path):
            msgs.append('Configuration file "%s" does not exist.' % self.path)
        _util.abort(*msgs)
