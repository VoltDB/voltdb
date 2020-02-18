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

import sqlparse
from sqlparse.sql import Identifier
from sqlparse.tokens import Token, Error

cleanup_regex = {
    # This matches only alphanumerics and underscores.
    'alphanum_underscore': re.compile(r'(\w+)$'),
    # This matches everything except spaces, parens, colon, and comma
    'many_punctuations': re.compile(r'([^():,\s]+)$'),
    # This matches everything except spaces, parens, colon, comma, and period
    'most_punctuations': re.compile(r'([^\.():,\s]+)$'),
    # This matches everything except a space.
    'all_punctuations': re.compile(r'([^\s]+)$'),
}


def last_word(text, include='alphanum_underscore'):
    """
    Find the last word in a sentence.
    """

    if not text:  # Empty string
        return ''

    if text[-1].isspace():
        return ''
    else:
        regex = cleanup_regex[include]
        matches = regex.search(text)
        if matches:
            return matches.group(0)
        else:
            return ''


def find_prev_keyword(sql, n_skip=0):
    """ Find the last sql keyword in an SQL statement

    Returns the value of the last keyword, and the text of the query with
    everything after the last keyword stripped
    """
    if not sql.strip():
        return None, ''

    parsed = sqlparse.parse(sql)[0]
    flattened = list(parsed.flatten())
    flattened = flattened[:len(flattened) - n_skip]

    logical_operators = ('AND', 'OR', 'NOT', 'BETWEEN')

    for t in reversed(flattened):
        if t.value == '(' or (t.is_keyword and (
                t.value.upper() not in logical_operators)):
            # Find the location of token t in the original parsed statement
            # We can't use parsed.token_index(t) because t may be a child token
            # inside a TokenList, in which case token_index thows an error
            # Minimal example:
            #   p = sqlparse.parse('select * from foo where bar')
            #   t = list(p.flatten())[-3]  # The "Where" token
            #   p.token_index(t)  # Throws ValueError: not in list
            idx = flattened.index(t)

            # Combine the string values of all tokens in the original list
            # up to and including the target keyword token t, to produce a
            # query string with everything after the keyword token removed
            text = ''.join(tok.value for tok in flattened[:idx + 1])
            return t, text

    return None, ''


def is_open_quote(sql):
    """Returns true if the query contains an unclosed quote"""

    # parsed can contain one or more semi-colon separated commands
    parsed = sqlparse.parse(sql)
    return any(_parsed_is_open_quote(p) for p in parsed)


def _parsed_is_open_quote(parsed):
    # Look for unmatched single quotes, or unmatched dollar sign quotes
    return any(tok.match(Token.Error, ("'", "$")) for tok in parsed.flatten())


def parse_partial_identifier(word):
    """Attempt to parse a (partially typed) word as an identifier

    word may include a schema qualification, like `schema_name.partial_name`
    or `schema_name.` There may also be unclosed quotation marks, like
    `"schema`, or `schema."partial_name`

    :param word: string representing a (partially complete) identifier
    :return: sqlparse.sql.Identifier, or None
    """

    p = sqlparse.parse(word)[0]
    n_tok = len(p.tokens)
    if n_tok == 1 and isinstance(p.tokens[0], Identifier):
        return p.tokens[0]
    elif p.token_next_by(m=(Error, '"'))[1]:
        # An unmatched double quote, e.g. '"foo', 'foo."', or 'foo."bar'
        # Close the double quote, then reparse
        return parse_partial_identifier(word + '"')
    else:
        return None
