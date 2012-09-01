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

import ConfigParser

# Configuration object

class VoltCLIConfiguration(ConfigParser.RawConfigParser):
    """Persistent access to configuration data."""
    def __init__(self, path):
        self.path = path
        ConfigParser.RawConfigParser.__init__(self)
    def load(self):
        self.read(self.path)
    def save(self):
        with open(self.path, 'w') as f:
            ConfigParser.RawConfigParser.write(self, f)
    def get(self, section, option):
        return ConfigParser.RawConfigParser.get(self, section, option)
    def set(self, section, option, value):
        ConfigParser.RawConfigParser.set(section, option, value)
        self.save()
    def __enter__(self):
        self.load()
    def __exit__(self, type, value, traceback):
        self.save()
