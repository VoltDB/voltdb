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
import time
import shutil
from xml.etree import ElementTree

import vcli_util

class Project(object):

    def __init__(self, root, project_path):
        self.root = root
        self.project_path = project_path

    def set_config(self, tag, value):
        info = self.root.find('info')
        e = info.find(tag)
        if e is not None:
            e.text = value
        else:
            child = ElementTree.Element(tag)
            child.text = value
            info.append(child)

    def get_config(self, tag, required = False):
        text = self.root.findtext("info/%s" % tag)
        if required and text is None:
            vcli_util.abort('"%s" is not configured in "%s".' % (tag, self.project_path),
                            'Use the "config" command to set (or manually edit).')
        return text

    def iter_config_all(self):
        for e in self.root.findall("info/*"):
            yield e.tag, e.text

    def save(self):
        backup = '%s.%s' % (self.project_path, time.strftime('%y%m%d%H%M'))
        vcli_util.info('Backing up up to "%s"...' % os.path.basename(backup))
        shutil.copy(self.project_path, backup)
        try:
            with open(self.project_path, 'w') as f:
                #TODO: Find a better way to pretty-print XML to a file.
                # For now don't want to assume a newer/better version of lxml is installed.
                f.write('<?xml version="1.0"?>\n')
                f.write(ElementTree.tostring(self.root))
        except (IOError, OSError), e:
            vcli_util.abort('Unable to save "%s".' % self.project_path, e)

def parse(project_path):
    return Project(vcli_util.parse_xml(project_path), project_path)
