# This file is part of VoltDB.
# Copyright (C) 2008-2019 VoltDB Inc.
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

# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice, this
#   list of conditions and the following disclaimer in the documentation and/or
#   other materials provided with the distribution.
#
# * Neither the name of the {organization} nor the names of its
#   contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
# ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import re
from collections import defaultdict

import sqlparse
from sqlparse.tokens import Name

from voltliterals.literals import get_literals

white_space_regex = re.compile('\\s+', re.MULTILINE)


def _compile_regex(keyword):
    # Surround the keyword with word boundaries and replace interior whitespace
    # with whitespace wildcards
    pattern = '\\b' + white_space_regex.sub(r'\\s+', keyword) + '\\b'
    return re.compile(pattern, re.MULTILINE | re.IGNORECASE)


keywords = get_literals('keywords')
keyword_regexs = dict((kw, _compile_regex(kw)) for kw in keywords)


# TODO: haven't enable this feature yet
class PrevalenceCounter(object):
    """
    Allow Completer to learn user's preferred keywords from history
    """

    def __init__(self):
        self.keyword_counts = defaultdict(int)
        self.name_counts = defaultdict(int)

    def update(self, text):
        self.update_keywords(text)
        self.update_names(text)

    def update_names(self, text):
        for parsed in sqlparse.parse(text):
            for token in parsed.flatten():
                if token.ttype in Name:
                    self.name_counts[token.value] += 1

    def clear_names(self):
        self.name_counts = defaultdict(int)

    def update_keywords(self, text):
        # Count keywords. Can't rely for sqlparse for this, because it's
        # database agnostic
        for keyword, regex in keyword_regexs.items():
            for _ in regex.finditer(text):
                self.keyword_counts[keyword] += 1

    def keyword_count(self, keyword):
        return self.keyword_counts[keyword]

    def name_count(self, name):
        return self.name_counts[name]
