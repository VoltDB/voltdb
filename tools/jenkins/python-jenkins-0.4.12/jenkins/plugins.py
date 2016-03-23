# Software License Agreement (BSD License)
#
# Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# 'AS IS' AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
# Authors:
# Darragh Bailey <dbailey@hp.com>

'''
.. module:: jenkins.plugins
    :platform: Unix, Windows
    :synopsis: Class for interacting with plugins
'''

import operator
import re

import pkg_resources


class Plugin(dict):
    '''Dictionary object containing plugin metadata.'''

    def __init__(self, *args, **kwargs):
        '''Populates dictionary using json object input.

        accepts same arguments as python `dict` class.
        '''
        version = kwargs.pop('version', None)

        super(Plugin, self).__init__(*args, **kwargs)
        self['version'] = version

    def __setitem__(self, key, value):
        '''Overrides default setter to ensure that the version key is always
        a PluginVersion class to abstract and simplify version comparisons
        '''
        if key == 'version':
            value = PluginVersion(value)
        super(Plugin, self).__setitem__(key, value)


class PluginVersion(str):
    '''Class providing comparison capabilities for plugin versions.'''

    _VERSION_RE = re.compile(r'(.*)-(?:SNAPSHOT|BETA)')

    def __init__(self, version):
        '''Parse plugin version and store it for comparison.'''

        self._version = version
        self.parsed_version = pkg_resources.parse_version(
            self.__convert_version(version))

    def __convert_version(self, version):
        return self._VERSION_RE.sub(r'\g<1>.preview', str(version))

    def __compare(self, op, version):
        return op(self.parsed_version, pkg_resources.parse_version(
            self.__convert_version(version)))

    def __le__(self, version):
        return self.__compare(operator.le, version)

    def __lt__(self, version):
        return self.__compare(operator.lt, version)

    def __ge__(self, version):
        return self.__compare(operator.ge, version)

    def __gt__(self, version):
        return self.__compare(operator.gt, version)

    def __eq__(self, version):
        return self.__compare(operator.eq, version)

    def __ne__(self, version):
        return self.__compare(operator.ne, version)

    def __str__(self):
        return str(self._version)

    def __repr__(self):
        return str(self._version)
