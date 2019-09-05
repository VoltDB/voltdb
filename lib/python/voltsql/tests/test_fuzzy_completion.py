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

import pytest


@pytest.fixture
def completer():
    from voltsql.voltcompleter import VoltCompleter
    return VoltCompleter()


def test_ranking_ignores_identifier_quotes(completer):
    """When calculating result rank, identifier quotes should be ignored.

    This test checks that the fuzzy ranking algorithm correctly ignores
    quotation marks when computing match ranks.

    """

    text = 'user'
    collection = ['user_action', '"user"']
    matches = completer.find_matches(text, collection)
    assert len(matches) == 2


def test_ranking_based_on_shortest_match(completer):
    """Fuzzy result rank should be based on shortest match.

    Result ranking in fuzzy searching is partially based on the length
    of matches: shorter matches are considered more relevant than
    longer ones. When searching for the text 'user', the length
    component of the match 'user_group' could be either 4 ('user') or
    7 ('user_gr').

    This test checks that the fuzzy ranking algorithm uses the shorter
    match when calculating result rank.

    """

    text = 'user'
    collection = ['api_user', 'user_group']
    matches = completer.find_matches(text, collection)

    assert matches[1].priority > matches[0].priority


@pytest.mark.parametrize('collection', [
    ['user_action', 'user'],
    ['user_group', 'user'],
    ['user_group', 'user_action'],
])
def test_should_break_ties_using_lexical_order(completer, collection):
    """Fuzzy result rank should use lexical order to break ties.

    When fuzzy matching, if multiple matches have the same match length and
    start position, present them in lexical (rather than arbitrary) order. For
    example, if we have tables 'user', 'user_action', and 'user_group', a
    search for the text 'user' should present these tables in this order.

    The input collections to this test are out of order; each run checks that
    the search text 'user' results in the input tables being reordered
    lexically.

    """

    text = 'user'
    matches = completer.find_matches(text, collection)

    assert matches[1].priority > matches[0].priority


def test_matching_should_be_case_insensitive(completer):
    """Fuzzy matching should keep matches even if letter casing doesn't match.

    This test checks that variations of the text which have different casing
    are still matched.
    """

    text = 'foo'
    collection = ['Foo', 'FOO', 'fOO']
    matches = completer.find_matches(text, collection)

    assert len(matches) == 3
