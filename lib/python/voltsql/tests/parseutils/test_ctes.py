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

import pytest
from sqlparse import parse

from voltsql.parseutils.ctes import token_start_pos, extract_ctes, extract_column_names as _extract_column_names


def extract_column_names(sql):
    p = parse(sql)[0]
    return _extract_column_names(p)


def test_token_str_pos():
    sql = 'SELECT * FROM xxx'
    p = parse(sql)[0]
    idx = p.token_index(p.tokens[-1])
    assert token_start_pos(p.tokens, idx) == len('SELECT * FROM ')

    sql = 'SELECT * FROM \nxxx'
    p = parse(sql)[0]
    idx = p.token_index(p.tokens[-1])
    assert token_start_pos(p.tokens, idx) == len('SELECT * FROM \n')


def test_single_column_name_extraction():
    sql = 'SELECT abc FROM xxx'
    assert extract_column_names(sql) == ('abc',)


def test_aliased_single_column_name_extraction():
    sql = 'SELECT abc def FROM xxx'
    assert extract_column_names(sql) == ('def',)


def test_aliased_expression_name_extraction():
    sql = 'SELECT 99 abc FROM xxx'
    assert extract_column_names(sql) == ('abc',)


def test_multiple_column_name_extraction():
    sql = 'SELECT abc, def FROM xxx'
    assert extract_column_names(sql) == ('abc', 'def')


def test_missing_column_name_handled_gracefully():
    sql = 'SELECT abc, 99 FROM xxx'
    assert extract_column_names(sql) == ('abc',)

    sql = 'SELECT abc, 99, def FROM xxx'
    assert extract_column_names(sql) == ('abc', 'def')


def test_aliased_multiple_column_name_extraction():
    sql = 'SELECT abc def, ghi jkl FROM xxx'
    assert extract_column_names(sql) == ('def', 'jkl')


def test_table_qualified_column_name_extraction():
    sql = 'SELECT abc.def, ghi.jkl FROM xxx'
    assert extract_column_names(sql) == ('def', 'jkl')


@pytest.mark.parametrize('sql', [
    'INSERT INTO foo (x, y, z) VALUES (5, 6, 7) RETURNING x, y',
    'DELETE FROM foo WHERE x > y RETURNING x, y',
    'UPDATE foo SET x = 9 RETURNING x, y',
])
def test_extract_column_names_from_returning_clause(sql):
    assert extract_column_names(sql) == ('x', 'y')


def test_simple_cte_extraction():
    sql = 'WITH a AS (SELECT abc FROM xxx) SELECT * FROM a'
    start_pos = len('WITH a AS ')
    stop_pos = len('WITH a AS (SELECT abc FROM xxx)')
    ctes, remainder = extract_ctes(sql)

    assert tuple(ctes) == (('a', ('abc',), start_pos, stop_pos),)
    assert remainder.strip() == 'SELECT * FROM a'


def test_cte_extraction_around_comments():
    sql = '''--blah blah blah
            WITH a AS (SELECT abc def FROM x)
            SELECT * FROM a'''
    start_pos = len('''--blah blah blah
            WITH a AS ''')
    stop_pos = len('''--blah blah blah
            WITH a AS (SELECT abc def FROM x)''')

    ctes, remainder = extract_ctes(sql)
    assert tuple(ctes) == (('a', ('def',), start_pos, stop_pos),)
    assert remainder.strip() == 'SELECT * FROM a'


def test_multiple_cte_extraction():
    sql = '''WITH
            x AS (SELECT abc, def FROM x),
            y AS (SELECT ghi, jkl FROM y)
            SELECT * FROM a, b'''

    start1 = len('''WITH
            x AS ''')

    stop1 = len('''WITH
            x AS (SELECT abc, def FROM x)''')

    start2 = len('''WITH
            x AS (SELECT abc, def FROM x),
            y AS ''')

    stop2 = len('''WITH
            x AS (SELECT abc, def FROM x),
            y AS (SELECT ghi, jkl FROM y)''')

    ctes, remainder = extract_ctes(sql)
    assert tuple(ctes) == (
        ('x', ('abc', 'def'), start1, stop1),
        ('y', ('ghi', 'jkl'), start2, stop2))
