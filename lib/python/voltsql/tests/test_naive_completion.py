# This file is part of VoltDB.
# Copyright (C) 2008-2018 VoltDB Inc.
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

from __future__ import unicode_literals

import pytest
from prompt_toolkit.completion import Completion
from prompt_toolkit.document import Document


@pytest.fixture
def completer():
    from voltsql.voltcompleter import VoltCompleter
    return VoltCompleter(smart_completion=False)


@pytest.fixture
def complete_event():
    from mock import Mock
    return Mock()


def test_empty_string_completion(completer, complete_event):
    text = ''
    position = 0
    result = set(completer.get_completions(
        Document(text=text, cursor_position=position),
        complete_event))
    assert result == set(map(Completion, completer.all_completions))


def test_select_keyword_completion(completer, complete_event):
    text = 'SEL'
    position = len('SEL')
    result = completer.get_completions(
        Document(text=text, cursor_position=position),
        complete_event)
    assert result == [Completion(text='SELECT', start_position=-3)]


def test_function_name_completion(completer, complete_event):
    text = 'SELECT MA'
    position = len('SELECT MA')
    result = set(completer.get_completions(
        Document(text=text, cursor_position=position),
        complete_event))
    print(result)
    assert result == {Completion(text=u'MAKEVALIDPOLYGON', start_position=-2),
                      Completion(text=u'MAXEXTENTS', start_position=-2),
                      Completion(text=u'MAX_VALID_TIMESTAMP()', start_position=-2),
                      Completion(text=u'MAX', start_position=-2),
                      Completion(text=u'MATERIALIZED VIEW', start_position=-2)}


def test_column_name_completion(completer, complete_event):
    text = 'SELECT  FROM users'
    position = len('SELECT ')
    result = set(completer.get_completions(
        Document(text=text, cursor_position=position),
        complete_event))
    assert result == set(map(Completion, completer.all_completions))


def test_alter_well_known_keywords_completion(completer, complete_event):
    text = 'ALTER '
    position = len(text)
    result = set(completer.get_completions(
        Document(text=text, cursor_position=position),
        complete_event,
        smart_completion=True))
    assert result == {Completion(text="TABLE", display_meta='keyword')}
    assert Completion(text="CREATE", display_meta="keyword") not in result
