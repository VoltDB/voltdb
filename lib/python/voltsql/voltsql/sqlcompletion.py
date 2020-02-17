# This file is part of VoltDB.
# Copyright (C) 2008-2020 VoltDB Inc.
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

from __future__ import print_function

import re
import sys
from collections import namedtuple

import sqlparse
from sqlparse.sql import Comparison, Identifier, Where

from parseutils.ctes import isolate_query_ctes
from parseutils.tables import extract_tables
from parseutils.utils import (
    last_word, find_prev_keyword, parse_partial_identifier)

PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

if PY3:
    string_types = str
else:
    string_types = basestring

# FromClauseItem is a table/view/function used in the FROM clause
# `table_refs` contains the list of tables/... already in the statement,
# used to ensure that the alias we suggest is unique
FromClauseItem = namedtuple('FromClauseItem', 'table_refs local_tables')
Table = namedtuple('Table', 'table_refs local_tables')
View = namedtuple('View', ['table_refs'])
# JoinConditions are suggested after ON, e.g. 'foo.barid = bar.barid'
JoinCondition = namedtuple('JoinCondition', ['table_refs', 'parent'])
# Joins are suggested after JOIN, e.g. 'foo ON foo.barid = bar.barid'
Join = namedtuple('Join', ['table_refs'])

Function = namedtuple('Function', ['table_refs', 'usage'])
# For convenience, don't require the `usage` argument in Function constructor
Function.__new__.__defaults__ = (tuple(), None)
Table.__new__.__defaults__ = (tuple(), tuple())
View.__new__.__defaults__ = (None,)
FromClauseItem.__new__.__defaults__ = (tuple(), tuple())

Column = namedtuple(
    'Column',
    ['table_refs', 'require_last_table', 'local_tables', 'qualifiable', 'context']
)
Column.__new__.__defaults__ = (None, None, tuple(), False, None)

Keyword = namedtuple('Keyword', ['last_token'])
Keyword.__new__.__defaults__ = (None,)
Datatype = namedtuple('Datatype', [])
Alias = namedtuple('Alias', ['aliases'])

Procedure = namedtuple('Procedure', [])


class SqlStatement(object):
    def __init__(self, full_text, text_before_cursor):
        self.identifier = None
        self.word_before_cursor = word_before_cursor = last_word(
            text_before_cursor, include='many_punctuations')

        full_text, text_before_cursor, self.local_tables = \
            isolate_query_ctes(full_text, text_before_cursor)

        self.text_before_cursor_including_last_word = text_before_cursor

        # If we've partially typed a word then word_before_cursor won't be an
        # empty string. In that case we want to remove the partially typed
        # string before sending it to the sqlparser. Otherwise the last token
        # will always be the partially typed string which renders the smart
        # completion useless because it will always return the list of
        # keywords as completion.
        if self.word_before_cursor:
            if word_before_cursor[-1] == '(' or word_before_cursor[0] == '\\':
                parsed = sqlparse.parse(text_before_cursor)
            else:
                text_before_cursor = text_before_cursor[:-len(word_before_cursor)]
                parsed = sqlparse.parse(text_before_cursor)
                self.identifier = parse_partial_identifier(word_before_cursor)
        else:
            parsed = sqlparse.parse(text_before_cursor)

        full_text, text_before_cursor, parsed = \
            _split_multiple_statements(full_text, text_before_cursor, parsed)

        self.full_text = full_text
        self.text_before_cursor = text_before_cursor
        self.parsed = parsed

        self.last_token = parsed and parsed.token_prev(len(parsed.tokens))[1] or ''

    def is_insert(self):
        return self.parsed.token_first().value.lower() == 'insert'

    def get_tables(self, scope='full'):
        """
        Gets the tables available in the statement.
        param `scope:` possible values: 'full', 'insert', 'before'
        If 'insert', only the first table is returned.
        If 'before', only tables before the cursor are returned.
        If not 'insert' and the stmt is an insert, the first table is skipped.
        """
        tables = extract_tables(
            self.full_text if scope == 'full' else self.text_before_cursor)
        if scope == 'insert':
            tables = tables[:1]
        elif self.is_insert():
            tables = tables[1:]
        return tables

    def get_previous_token(self, token):
        return self.parsed.token_prev(self.parsed.token_index(token))[1]

    def reduce_to_prev_keyword(self, n_skip=0):
        prev_keyword, self.text_before_cursor = \
            find_prev_keyword(self.text_before_cursor, n_skip=n_skip)
        return prev_keyword


def suggest_type(full_text, text_before_cursor):
    """Takes the full_text that is typed so far and also the text before the
    cursor to suggest completion type and scope.

    Returns a tuple with a type of entity ('table', 'column' etc) and a scope.
    A scope for a column category will be a list of tables.
    """

    # This is a temporary hack; the exception handling
    # here should be removed once sqlparse has been fixed
    try:
        stmt = SqlStatement(full_text, text_before_cursor)
    except (TypeError, AttributeError):
        return []

    return suggest_based_on_last_token(stmt.last_token, stmt)


function_body_pattern = re.compile(r'(\$.*?\$)([\s\S]*?)\1', re.M)


def _find_function_body(text):
    split = function_body_pattern.search(text)
    return (split.start(2), split.end(2)) if split else (None, None)


def _statement_from_function(full_text, text_before_cursor, statement):
    current_pos = len(text_before_cursor)
    body_start, body_end = _find_function_body(full_text)
    if body_start is None:
        return full_text, text_before_cursor, statement
    if not body_start <= current_pos < body_end:
        return full_text, text_before_cursor, statement
    full_text = full_text[body_start:body_end]
    text_before_cursor = text_before_cursor[body_start:]
    parsed = sqlparse.parse(text_before_cursor)
    return _split_multiple_statements(full_text, text_before_cursor, parsed)


def _split_multiple_statements(full_text, text_before_cursor, parsed):
    if len(parsed) > 1:
        # Multiple statements being edited -- isolate the current one by
        # cumulatively summing statement lengths to find the one that bounds
        # the current position
        current_pos = len(text_before_cursor)
        stmt_start, stmt_end = 0, 0

        for statement in parsed:
            stmt_len = len(str(statement))
            stmt_start, stmt_end = stmt_end, stmt_end + stmt_len

            if stmt_end >= current_pos:
                text_before_cursor = full_text[stmt_start:current_pos]
                full_text = full_text[stmt_start:]
                break

    elif parsed:
        # A single statement
        statement = parsed[0]
    else:
        # The empty string
        return full_text, text_before_cursor, None

    token2 = None
    if statement.get_type() in ('CREATE', 'CREATE OR REPLACE'):
        token1 = statement.token_first()
        if token1:
            token1_idx = statement.token_index(token1)
            token2 = statement.token_next(token1_idx)[1]
    if token2 and token2.value.upper() == 'FUNCTION':
        full_text, text_before_cursor, statement = _statement_from_function(
            full_text, text_before_cursor, statement
        )
    return full_text, text_before_cursor, statement


def suggest_based_on_last_token(token, stmt):
    if isinstance(token, string_types):
        token_v = token.lower()
    elif token.value.lower() == "exec":
        # if exec, then it must be a Stored Procedure
        return (Procedure(),)
    elif isinstance(token, Comparison):
        # If 'token' is a Comparison type such as
        # 'select * FROM abc a JOIN def d ON a.id = d.'. Then calling
        # token.value on the comparison type will only return the lhs of the
        # comparison. In this case a.id. So we need to do token.tokens to get
        # both sides of the comparison and pick the last token out of that
        # list.
        token_v = token.tokens[-1].value.lower()
    elif isinstance(token, Where):
        # sqlparse groups all tokens from the where clause into a single token
        # list. This means that token.value may be something like
        # 'where foo > 5 and '. We need to look "inside" token.tokens to handle
        # suggestions in complicated where clauses correctly
        prev_keyword = stmt.reduce_to_prev_keyword()
        return suggest_based_on_last_token(prev_keyword, stmt)
    elif isinstance(token, Identifier):
        # If the previous token is an identifier, we can suggest datatypes if
        # we're in a parenthesized column/field list, e.g.:
        #       CREATE TABLE foo (Identifier <CURSOR>
        #       CREATE FUNCTION foo (Identifier <CURSOR>
        # If we're not in a parenthesized list, the most likely scenario is the
        # user is about to specify an alias, e.g.:
        #       SELECT Identifier <CURSOR>
        #       SELECT foo FROM Identifier <CURSOR>
        prev_keyword, _ = find_prev_keyword(stmt.text_before_cursor)
        if prev_keyword and prev_keyword.value == '(':
            # Suggest datatypes
            return suggest_based_on_last_token('type', stmt)
        else:
            return (Keyword(),)
    else:
        token_v = token.value.lower()

    if not token:
        return (Keyword(),)
    elif token_v.endswith('('):
        p = sqlparse.parse(stmt.text_before_cursor)[0]

        if p.tokens and isinstance(p.tokens[-1], Where):
            # Four possibilities:
            #  1 - Parenthesized clause like "WHERE foo AND ("
            #        Suggest columns/functions
            #  2 - Function call like "WHERE foo("
            #        Suggest columns/functions
            #  3 - Subquery expression like "WHERE EXISTS ("
            #        Suggest keywords, in order to do a subquery
            #  4 - Subquery OR array comparison like "WHERE foo = ANY("
            #        Suggest columns/functions AND keywords. (If we wanted to be
            #        really fancy, we could suggest only array-typed columns)

            column_suggestions = suggest_based_on_last_token('where', stmt)

            # Check for a subquery expression (cases 3 & 4)
            where = p.tokens[-1]
            prev_tok = where.token_prev(len(where.tokens) - 1)[1]

            if isinstance(prev_tok, Comparison):
                # e.g. "SELECT foo FROM bar WHERE foo = ANY("
                prev_tok = prev_tok.tokens[-1]

            prev_tok = prev_tok.value.lower()
            if prev_tok == 'exists':
                return (Keyword(),)
            else:
                return column_suggestions

        # Get the token before the parens
        prev_tok = p.token_prev(len(p.tokens) - 1)[1]

        if (prev_tok and prev_tok.value
                and prev_tok.value.lower().split(' ')[-1] == 'using'):
            # tbl1 INNER JOIN tbl2 USING (col1, col2)
            tables = stmt.get_tables('before')

            # suggest columns that are present in more than one table
            return (Column(table_refs=tables,
                           require_last_table=True,
                           local_tables=stmt.local_tables),)

        elif p.token_first().value.lower() == 'select':
            # If the lparen is preceeded by a space chances are we're about to
            # do a sub-select.
            if last_word(stmt.text_before_cursor,
                         'all_punctuations').startswith('('):
                return (Keyword(),)
        prev_prev_tok = prev_tok and p.token_prev(p.token_index(prev_tok))[1]
        if prev_prev_tok and prev_prev_tok.normalized == 'INTO':
            return (
                Column(table_refs=stmt.get_tables('insert'), context='insert'),
            )
        # We're probably in a function argument list
        return (Column(table_refs=extract_tables(stmt.full_text),
                       local_tables=stmt.local_tables, qualifiable=True),)
    elif token_v == 'set':
        return (Column(table_refs=stmt.get_tables(),
                       local_tables=stmt.local_tables),)
    elif token_v in ('select', 'where', 'having', 'by', 'distinct'):
        # Check for a table alias
        tables = stmt.get_tables()
        return (Column(table_refs=tables, local_tables=stmt.local_tables,
                       qualifiable=True),
                Function(),
                Keyword(token_v.upper()),)
    elif token_v == 'as':
        # Don't suggest anything for aliases
        return ()
    elif (token_v.endswith('join') and token.is_keyword) or (token_v in
                                                             ('copy', 'from', 'update', 'into', 'describe',
                                                              'truncate')):
        tables = extract_tables(stmt.text_before_cursor)
        is_join = token_v.endswith('join') and token.is_keyword

        suggest = []

        if token_v == 'from' or is_join:
            suggest.append(FromClauseItem(table_refs=tables,
                                          local_tables=stmt.local_tables))
        elif token_v == 'truncate':
            suggest.append(Table())
        else:
            suggest.extend((Table(), View()))

        if is_join and _allow_join(stmt.parsed):
            tables = stmt.get_tables('before')
            suggest.append(Join(table_refs=tables))

        return tuple(suggest)

    elif token_v == 'function':
        # stmt.get_previous_token will fail for e.g. `SELECT 1 FROM functions WHERE function:`
        try:
            prev = stmt.get_previous_token(token).value.lower()
            if prev in ('drop', 'alter', 'create', 'create or replace'):
                return (Function(usage='signature'),)
        except ValueError:
            pass
        return tuple()

    elif token_v in ('table', 'view'):
        # E.g. 'ALTER TABLE <tablname>'
        rel_type = {'table': Table, 'view': View, 'function': Function}[token_v]
        return (rel_type(),)

    elif token_v == 'column':
        # E.g. 'ALTER TABLE foo ALTER COLUMN bar
        return (Column(table_refs=stmt.get_tables()),)

    elif token_v == 'on':
        tables = stmt.get_tables('before')
        parent = (stmt.identifier and stmt.identifier.get_parent_name()) or None
        if parent:
            # "ON parent.<suggestion>"
            # parent can be a table alias
            filteredtables = tuple(t for t in tables if identifies(parent, t))
            sugs = [Column(table_refs=filteredtables,
                           local_tables=stmt.local_tables),
                    Table(),
                    View(),
                    Function()]
            if filteredtables and _allow_join_condition(stmt.parsed):
                sugs.append(JoinCondition(table_refs=tables,
                                          parent=filteredtables[-1]))
            return tuple(sugs)
        else:
            # ON <suggestion>
            # Use table alias if there is one, otherwise the table name
            aliases = tuple(t.ref for t in tables)
            if _allow_join_condition(stmt.parsed):
                return (Alias(aliases=aliases), JoinCondition(
                    table_refs=tables, parent=None))
            else:
                return (Alias(aliases=aliases),)

    elif token_v.endswith(',') or token_v in ('=', 'and', 'or'):
        prev_keyword = stmt.reduce_to_prev_keyword()
        if prev_keyword:
            return suggest_based_on_last_token(prev_keyword, stmt)
        else:
            return ()
    elif token_v in 'type':
        suggestions = [Datatype()]
        return tuple(suggestions)
    elif token_v in {'alter', 'create', 'drop'}:
        return (Keyword(token_v.upper()),)
    elif token.is_keyword:
        # token is a keyword we haven't implemented any special handling for
        # go backwards in the query until we find one we do recognize
        prev_keyword = stmt.reduce_to_prev_keyword(n_skip=1)
        if prev_keyword:
            return suggest_based_on_last_token(prev_keyword, stmt)
        else:
            return (Keyword(token_v.upper()),)
    else:
        return (Keyword(),)


def identifies(id, ref):
    """Returns true if string `id` matches TableReference `ref`"""

    return id == ref.alias or id == ref.name


def _allow_join_condition(statement):
    """
    Tests if a join condition should be suggested

    We need this to avoid bad suggestions when entering e.g.
        select * from tbl1 a join tbl2 b on a.id = <cursor>
    So check that the preceding token is a ON, AND, or OR keyword, instead of
    e.g. an equals sign.

    :param statement: an sqlparse.sql.Statement
    :return: boolean
    """

    if not statement or not statement.tokens:
        return False

    last_tok = statement.token_prev(len(statement.tokens))[1]
    return last_tok.value.lower() in ('on', 'and', 'or')


def _allow_join(statement):
    """
    Tests if a join should be suggested

    We need this to avoid bad suggestions when entering e.g.
        select * from tbl1 a join tbl2 b <cursor>
    So check that the preceding token is a JOIN keyword

    :param statement: an sqlparse.sql.Statement
    :return: boolean
    """

    if not statement or not statement.tokens:
        return False

    last_tok = statement.token_prev(len(statement.tokens))[1]
    return (last_tok.value.lower().endswith('join')
            and last_tok.value.lower() not in ('cross join', 'natural join'))
