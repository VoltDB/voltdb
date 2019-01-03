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

from __future__ import unicode_literals

from functools import partial

from mock import Mock
from prompt_toolkit.completion import Completion
from prompt_toolkit.document import Document

from voltsql.voltcompleter import VoltCompleter


def completion(display_meta, text, pos=0):
    return Completion(text, start_position=pos, display_meta=display_meta)


def get_result(completer, text, position=None):
    position = len(text) if position is None else position
    return completer.get_completions(
        Document(text=text, cursor_position=position), Mock()
    )


def result_set(completer, text, position=None):
    return set(get_result(completer, text, position))


def escape(name):
    if not name.islower() or name in ('select', 'localtimestamp'):
        return '"' + name + '"'
    return name


def function(text, pos=0, display=None):
    return Completion(
        text,
        display=display or text,
        start_position=pos,
        display_meta='function'
    )


table = partial(completion, 'table')
view = partial(completion, 'view')
column = partial(completion, 'column')
keyword = partial(completion, 'keyword')
datatype = partial(completion, 'datatype')
alias = partial(completion, 'table alias')
name_join = partial(completion, 'name join')
fk_join = partial(completion, 'fk join')
join = partial(completion, 'join')


def wildcard_expansion(cols, pos=-1):
    return Completion(
        cols, start_position=pos, display_meta='columns', display='*')


class MetaData(object):
    def __init__(self, metadata):
        self.metadata = metadata

    def builtin_functions(self, pos=0):
        return [function(f, pos) for f in self.completer.functions]

    def builtin_datatypes(self, pos=0):
        return [datatype(dt, pos) for dt in self.completer.datatypes]

    def keywords(self, pos=0):
        return [keyword(kw, pos) for kw in self.completer.keywords_tree.keys()]

    def columns(self, tbl, typ='tables', pos=0):
        cols = self.metadata[typ][tbl]
        return [column(col, pos) for col in cols]

    def datatypes(self, parent='public', pos=0):
        return [
            datatype(escape(x), pos)
            for x in self.metadata.get('datatypes', {}).get(parent, [])]

    def tables(self, parent='public', pos=0):
        return [
            table(escape(x), pos)
            for x in self.metadata.get('tables', {}).get(parent, [])]

    def views(self, parent='public', pos=0):
        return [
            view(escape(x), pos)
            for x in self.metadata.get('views', {}).get(parent, [])]

    def functions(self, pos=0):
        return [
            function(
                x,
                pos
            )
            for x in self.metadata.get('functions', [])
        ]

    def functions_and_keywords(self, pos=0):
        return (
                self.functions(pos) + self.builtin_functions(pos) +
                self.keywords(pos)
        )

    # Note that the filtering parameters here only apply to the columns
    def columns_functions_and_keywords(
            self, tbl, typ='tables', pos=0
    ):
        return (
                self.functions_and_keywords(pos=pos) +
                self.columns(tbl, typ, pos)
        )

    def from_clause_items(self, parent='public', pos=0):
        return (
                self.functions(parent, pos) + self.views(parent, pos) +
                self.tables(parent, pos)
        )

    def schemas_and_from_clause_items(self, parent='public', pos=0):
        return self.from_clause_items(parent, pos) + self.schemas(pos)

    def types(self, parent='public', pos=0):
        return self.datatypes(parent, pos) + self.tables(parent, pos)

    @property
    def completer(self):
        return self.get_completer()

    def get_completer(self, casing=None):
        get_casing = lambda words: dict((word.lower(), word) for word in words)

        comp = VoltCompleter(smart_completion=True)
        comp.dbmetadata = self.metadata
        if casing:
            comp.casing = get_casing(casing)

        return comp
