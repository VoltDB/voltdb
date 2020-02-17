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

from __future__ import unicode_literals

import pytest

from utils import (MetaData, table, view, function, column, get_result, result_set, keyword)

parametrize = pytest.mark.parametrize

metadata = {
    'tables': {
        'USERS': ['id', 'parentid', 'email', 'first_name', 'last_name'],
        'ORDERS': ['id', 'ordered_date', 'status', 'email'],
        'SELECT': ['id', 'insert', 'ABC']},
    'views': {
        'USER_EMAILS': ['id', 'email'],
        'FUNCTIONS': ['function'],
    },
    'functions': ['custom_fun', '_custom_fun', 'custom_func1', 'custom_func2', 'set_returning_func'],
    'datatypes': ['custom_type1', 'custom_type2'],
}

# metadata = dict((k, {'public': v}) for k, v in metadata.items())

testdata = MetaData(metadata)

cased_users_col_names = ['ID', 'PARENTID', 'Email', 'First_Name', 'last_name']
cased_users2_col_names = ['UserID', 'UserName']
cased_func_names = [
    'Custom_Fun', '_custom_fun', 'Custom_Func1', 'custom_func2', 'set_returning_func'
]
cased_tbls = ['Users', 'Orders']
cased_views = ['User_Emails', 'Functions']
casing = (
        ['SELECT', 'PUBLIC'] + cased_func_names + cased_tbls + cased_views
        + cased_users_col_names + cased_users2_col_names
)
# Lists for use in assertions
cased_funcs = [
                  function(f) for f in ('Custom_Fun', '_custom_fun', 'Custom_Func1', 'custom_func2')
              ] + [function('set_returning_func')]
cased_tbls = [table(t) for t in (cased_tbls + ['"Users"', '"select"'])]
cased_rels = [view(t) for t in cased_views] + cased_funcs + cased_tbls
cased_users_cols = [column(c) for c in cased_users_col_names]
aliased_rels = [
                   table(t) for t in ('users u', '"Users" U', 'orders o', '"select" s')
               ] + [view('user_emails ue'), view('functions f')] + [
                   function(f) for f in (
        '_custom_fun() cf', 'custom_fun() cf', 'custom_func1() cf',
        'custom_func2() cf'
    )
               ] + [function(
    'set_returning_func(x := , y := ) srf',
    display='set_returning_func(x, y) srf'
)]
cased_aliased_rels = [
                         table(t) for t in ('Users U', '"Users" U', 'Orders O', '"select" s')
                     ] + [view('User_Emails UE'), view('Functions F')] + [
                         function(f) for f in (
        '_custom_fun() cf', 'Custom_Fun() CF', 'Custom_Func1() CF', 'custom_func2() cf'
    )
                     ] + [function(
    'set_returning_func(x := , y := ) srf',
    display='set_returning_func(x, y) srf'
)]

completers = [testdata.get_completer(casing)]
no_casing_completers = [testdata.get_completer()]


# Just to make sure that this doesn't crash
@parametrize('completer', completers)
def test_function_column_name(completer):
    for l in range(
            len('SELECT * FROM Functions WHERE function:'),
            len('SELECT * FROM Functions WHERE function:text') + 1
    ):
        assert [] == get_result(
            completer, 'SELECT * FROM Functions WHERE function:text'[:l]
        )


@parametrize('action', ['ALTER', 'DROP', 'CREATE', 'CREATE OR REPLACE'])
@parametrize('completer', completers)
def test_drop_alter_function(completer, action):
    assert get_result(completer, action + ' FUNCTION set_ret') == [
        function('set_returning_func', -len('set_ret'))
    ]


@parametrize('completer', completers)
def test_empty_string_completion(completer):
    result = result_set(completer, '')
    assert set(testdata.keywords()) == result


@parametrize('completer', completers)
def test_select_keyword_completion(completer):
    result = result_set(completer, 'SEL')
    assert result == {keyword('SELECT', -3)}


@parametrize('completer', completers)
def test_builtin_function_name_completion(completer):
    result = result_set(completer, 'SELECT MAX')
    assert result == {function('MAX', -3), function('MAX_VALID_TIMESTAMP()', -3)}


@parametrize('completer', completers)
def test_builtin_function_matches_only_at_start(completer):
    text = 'SELECT IN'

    result = [c.text for c in get_result(completer, text)]

    assert 'MIN' not in result


@parametrize('completer', completers)
def test_user_function_name_completion(completer):
    result = result_set(completer, 'SELECT cu')
    assert result == {function('Custom_Fun', -2), function('Custom_Func1', -2), function('custom_func2', -2),
                      function('CURRENT_TIMESTAMP()', -2)}


@parametrize('completer', completers)
def test_user_function_name_completion_matches_anywhere(completer):
    result = result_set(completer, 'SELECT om')
    assert len(result) == 0


@parametrize('completer', completers)
def test_suggested_cased_column_names(completer):
    result = result_set(completer, 'SELECT  from users', len('SELECT '))
    assert result == set(cased_funcs + cased_users_cols
                         + testdata.builtin_functions() + testdata.keywords())


@parametrize('completer', completers)
@parametrize('text', [
    'UPDATE users SET ',
    'INSERT INTO users(',
])
def test_no_column_qualification(text, completer):
    cols = [column(c) for c in cased_users_col_names]
    result = result_set(completer, text)
    assert result == set(cols)


@parametrize('completer', completers)
def test_suggested_cased_always_qualified_column_names(
        completer
):
    text = 'SELECT  from users'
    position = len('SELECT ')
    cols = [column(c) for c in cased_users_col_names]
    result = result_set(completer, text, position)
    assert result == set(cased_funcs + cols
                         + testdata.builtin_functions() + testdata.keywords())


@parametrize('completer', no_casing_completers)
def test_suggested_column_names_in_function(completer):
    result = result_set(
        completer, 'SELECT MAX( from users', len('SELECT MAX(')
    )
    assert result == set(testdata.columns('USERS'))


@parametrize('completer', no_casing_completers)
def test_suggested_multiple_column_names(completer):
    result = result_set(
        completer, 'SELECT id,  from users u', len('SELECT id, ')
    )
    assert result == set(testdata.columns_functions_and_keywords('USERS'))


@parametrize('completer', no_casing_completers)
def test_suggest_columns_after_three_way_join(completer):
    text = '''SELECT * FROM users u1
              INNER JOIN users u2 ON u1.id = u2.id
              INNER JOIN users u3 ON u2.id = u3.'''
    result = result_set(completer, text)
    assert (column('id') in result)


@parametrize('completer', no_casing_completers)
@parametrize('text', [
    'SELECT * FROM users INNER JOIN orders USING (',
    'SELECT * FROM users INNER JOIN orders USING(',
])
def test_join_using_suggests_common_columns(completer, text):
    result = result_set(completer, text)
    cols = [column(c) for c in metadata['tables']['USERS']]
    cols += [column(c) for c in metadata['tables']['ORDERS']]
    assert result == set(cols)


@parametrize('completer', no_casing_completers)
@parametrize('text', [
    'SELECT * FROM users INNER JOIN orders USING (id,',
    'SELECT * FROM users INNER JOIN orders USING(id,',
])
def test_join_using_suggests_columns_after_first_column(completer, text):
    result = result_set(completer, text)
    cols = [column(c) for c in metadata['tables']['USERS']]
    cols += [column(c) for c in metadata['tables']['ORDERS']]
    assert result == set(cols)


@parametrize('completer', no_casing_completers)
def test_auto_escaped_col_names(completer):
    result = result_set(completer, 'SELECT  from "select"', len('SELECT '))
    assert result == set(testdata.columns_functions_and_keywords('SELECT'))


@parametrize('completer', no_casing_completers)
def test_columns_before_keywords(completer):
    text = 'SELECT * FROM orders WHERE s'
    completions = get_result(completer, text)

    col = column('status', -1)
    kw = keyword('SELECT', -1)

    assert completions.index(col) < completions.index(kw)


@parametrize('completer', no_casing_completers)
@parametrize('text', [
    'INSERT INTO users ()',
    'INSERT INTO users()',
    'INSERT INTO users () SELECT * FROM orders;',
    'INSERT INTO users() SELECT * FROM users u cross join orders o',
])
def test_insert(completer, text):
    position = text.find('(') + 1
    result = result_set(completer, text, position)
    assert result == set(testdata.columns('USERS'))
