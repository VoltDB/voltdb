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

from __future__ import print_function, unicode_literals

import operator
import re
from collections import namedtuple
from itertools import chain

from prompt_toolkit.completion import Completer, Completion

from parseutils.utils import last_word
from prioritization import PrevalenceCounter
from sqlcompletion import suggest_type, Column, FromClauseItem, JoinCondition, Join, Function, Table, \
    View, Alias, Keyword, Datatype, Procedure
from voltliterals.literals import get_literals

Match = namedtuple('Match', ['completion', 'priority'])

# the set of keywords that are case sensitive
RESERVED_KEYWORDS_SET = {"classes", "procedures", "tables"}


class VoltCompleter(Completer):
    # keywords_tree: A dict mapping keywords to well known following keywords.
    # e.g. 'CREATE': ['TABLE', 'USER', ...],
    keywords_tree = get_literals('keywords', type_=dict)
    keywords = tuple(set(chain(keywords_tree.keys(), *keywords_tree.values())))
    functions = get_literals('functions')
    procedures = get_literals('procedures')
    datatypes = get_literals('datatypes')
    reserved_words = set(get_literals('reserved'))

    def __init__(self, smart_completion=True):
        self.smart_completion = smart_completion
        self.prioritizer = PrevalenceCounter()
        self.keyword_casing = "upper"
        self.name_pattern = re.compile(r"^[_a-z][_a-z0-9\$]*$")
        # metadata should be updated in real-time
        # note that we assume name of tables and views are in upper-case
        self.dbmetadata = {'tables': {}, 'views': {}, 'functions': [],
                           'datatypes': []}
        # TODO: casing is not enabled yet
        # casing should be a dict {lowercasename: PreferredCasingName}
        self.casing = {}

        self.all_completions = set(self.keywords + self.functions)

    def escape_name(self, name):
        """ Quote a string."""
        if name and ((not self.name_pattern.match(name))
                     or (name.upper() in self.reserved_words)
                     or (name.upper() in self.functions)):
            name = '"%s"' % name

        return name

    def unescape_name(self, name):
        """ Unquote a string."""
        if name and name[0] == '"' and name[-1] == '"':
            name = name[1:-1]

        return name

    def escaped_names(self, names):
        return [self.escape_name(name) for name in names]

    def reset_completions(self):
        self.dbmetadata = {'tables': {}, 'views': {}, 'functions': [],
                           'datatypes': []}
        self.all_completions = set(self.keywords + self.functions)

    def case(self, word):
        return self.casing.get(word, word)

    def find_matches(self, text, collection, mode='fuzzy', meta=None):
        """Find completion matches for the given text.

        Given the user's input text and a collection of available
        completions, find completions matching the last word of the
        text.

        `collection` can be either a list of strings or a list of Candidate
        namedtuples.
        `mode` can be either 'fuzzy', or 'strict'
            'fuzzy': fuzzy matching, ties broken by name prevalance
            `keyword`: start only matching, ties broken by keyword prevalance

        yields prompt_toolkit Completion instances for any matches found
        in the collection of available completions.

        """
        if not collection:
            return []
        priority_order = [
            'keyword', 'function', 'procedure', 'view', 'table', 'datatype',
            'column', 'table alias', 'join', 'name join', 'fk join',
            'table format'
        ]
        type_priority = priority_order.index(meta) if meta in priority_order else -1
        text = last_word(text, include='most_punctuations').lower()
        text_len = len(text)

        if text and text[0] == '"':
            # text starts with double quote; user is manually escaping a name
            # Match on everything that follows the double-quote. Note that
            # text_len is calculated before removing the quote, so the
            # Completion.position value is correct
            text = text[1:]

        if mode == 'fuzzy':
            fuzzy = True
            priority_func = self.prioritizer.name_count
        else:
            fuzzy = False
            priority_func = self.prioritizer.keyword_count

        # Construct a `_match` function for either fuzzy or non-fuzzy matching
        # The match function returns a 2-tuple used for sorting the matches,
        # or None if the item doesn't match
        # Note: higher priority values mean more important, so use negative
        # signs to flip the direction of the tuple
        if fuzzy:
            regex = '.*?'.join(map(re.escape, text))
            pat = re.compile('(%s)' % regex)

            def _match(item):
                if item.lower()[:len(text) + 1] in (text, text + ' '):
                    # Exact match of first word in suggestion
                    # This is to get exact alias matches to the top
                    # E.g. for input `e`, 'Entries E' should be on top
                    # (before e.g. `EndUsers EU`)
                    return float('Infinity'), -1
                r = pat.search(self.unescape_name(item.lower()))
                if r:
                    return -len(r.group()), -r.start()
        else:
            match_end_limit = len(text)

            def _match(item):
                match_point = item.lower().find(text, 0, match_end_limit)
                if match_point >= 0:
                    # Use negative infinity to force keywords to sort after all
                    # fuzzy matches
                    return -float('Infinity'), -match_point

        matches = []
        for cand in collection:

            item, display_meta, prio, prio2, display = cand, meta, 0, 0, cand
            sort_key = _match(cand)

            if sort_key:
                if display_meta and len(display_meta) > 50:
                    # Truncate meta-text to 50 characters, if necessary
                    display_meta = display_meta[:47] + u'...'

                # Lexical order of items in the collection, used for
                # tiebreaking items with the same match group length and start
                # position. Since we use *higher* priority to mean "more
                # important," we use -ord(c) to prioritize "aa" > "ab" and end
                # with 1 to prioritize shorter strings (ie "user" > "users").
                # We first do a case-insensitive sort and then a
                # case-sensitive one as a tie breaker.
                # We also use the unescape_name to make sure quoted names have
                # the same priority as unquoted names.
                lexical_priority = (tuple(0 if c in (' _') else -ord(c)
                                          for c in self.unescape_name(item.lower())) + (1,)
                                    + tuple(c for c in item))

                item = self.case(item)
                display = self.case(display)
                priority = (
                    sort_key, type_priority, prio, priority_func(item),
                    prio2, lexical_priority
                )
                matches.append(
                    Match(
                        completion=Completion(
                            text=item,
                            start_position=-text_len,
                            display_meta=display_meta,
                            display=display
                        ),
                        priority=priority
                    )
                )
        return matches

    def get_completions(self, document, complete_event, smart_completion=None):
        word_before_cursor = document.get_word_before_cursor(WORD=True)

        if smart_completion is None:
            smart_completion = self.smart_completion

        # If smart_completion is off then match any word that starts with
        # 'word_before_cursor'.
        if not smart_completion:
            matches = self.find_matches(word_before_cursor, self.all_completions,
                                        mode='strict')
            completions = [m.completion for m in matches]
            return sorted(completions, key=operator.attrgetter('text'))

        matches = []
        suggestions = suggest_type(document.text, document.text_before_cursor)

        for suggestion in suggestions:
            suggestion_type = type(suggestion)
            # Map suggestion type to method
            # e.g. 'table' -> self.get_table_matches
            matcher = self.suggestion_matchers[suggestion_type]
            matches.extend(matcher(self, suggestion, word_before_cursor))

        # Sort matches so highest priorities are first
        matches = sorted(matches, key=operator.attrgetter('priority'),
                         reverse=True)

        return [m.completion for m in matches]

    def get_column_matches(self, suggestion, word_before_cursor):
        tables = suggestion.table_refs

        if not tables or len(tables) == 0:
            return self.find_matches(word_before_cursor,
                                     set([c for column_list in self.dbmetadata['tables'].values() for c in
                                          column_list]), meta='column')
        return self.find_matches(word_before_cursor,
                                 [c for column_list in
                                  [self.dbmetadata['tables'].get(table.name.upper(), []) for table in tables] for c in
                                  column_list],
                                 meta='column')

    def get_join_matches(self, suggestion, word_before_cursor):

        return self.find_matches(word_before_cursor,
                                 self.dbmetadata['tables'].keys(),
                                 meta='join')

    # TODO: this can be improved
    def get_join_condition_matches(self, suggestion, word_before_cursor):
        return self.get_column_matches(suggestion, word_before_cursor)

    def get_function_matches(self, suggestion, word_before_cursor, alias=False):
        return (self.find_matches(word_before_cursor, self.functions, mode='strict',
                                  meta='function')
                + self.find_matches(word_before_cursor, self.dbmetadata['functions'], mode='strict',
                                    meta='function'))

    def get_from_clause_item_matches(self, suggestion, word_before_cursor):
        return (
                self.find_matches(word_before_cursor,
                                  self.dbmetadata['tables'].keys(), meta='table')
                + self.find_matches(word_before_cursor,
                                    self.dbmetadata['views'].keys(), meta='view')
                + self.find_matches(word_before_cursor, self.functions, meta='function')
                + self.find_matches(word_before_cursor, self.dbmetadata['functions'],
                                    meta='function')
        )

    def get_table_matches(self, suggestion, word_before_cursor, alias=False):
        return self.find_matches(word_before_cursor,
                                 self.dbmetadata['tables'].keys(), meta='table')

    def get_view_matches(self, suggestion, word_before_cursor, alias=False):
        return self.find_matches(word_before_cursor,
                                 self.dbmetadata['views'].keys(), meta='view')

    def get_alias_matches(self, suggestion, word_before_cursor):
        aliases = suggestion.aliases
        return self.find_matches(word_before_cursor, aliases,
                                 meta='table alias')

    def get_keyword_matches(self, suggestion, word_before_cursor):
        keywords = self.keywords_tree.keys()
        # Get well known following keywords for the last token. If any, narrow
        # candidates to this list.
        next_keywords = self.keywords_tree.get(suggestion.last_token, [])
        if next_keywords:
            keywords = next_keywords

        casing = self.keyword_casing
        if casing == 'auto':
            if word_before_cursor and word_before_cursor[-1].islower():
                casing = 'lower'
            else:
                casing = 'upper'

        if casing == 'upper':
            keywords = [k.upper() if k not in RESERVED_KEYWORDS_SET else k for k in keywords]
        else:
            keywords = [k.lower() if k not in RESERVED_KEYWORDS_SET else k for k in keywords]

        return self.find_matches(word_before_cursor, keywords,
                                 mode='strict', meta='keyword')

    def get_datatype_matches(self, suggestion, word_before_cursor):
        return self.find_matches(word_before_cursor, self.datatypes,
                                 mode='strict', meta='datatype')

    def get_procedure_matches(self, suggestion, word_before_cursor):
        return (self.find_matches(word_before_cursor, self.procedures,
                                  mode='strict', meta='procedure')
                + self.find_matches(word_before_cursor, self.dbmetadata['procedures'],
                                    meta='procedure')
                )

    suggestion_matchers = {
        FromClauseItem: get_from_clause_item_matches,
        JoinCondition: get_join_condition_matches,
        Join: get_join_matches,
        Column: get_column_matches,
        Function: get_function_matches,
        Table: get_table_matches,
        View: get_view_matches,
        Alias: get_alias_matches,
        Keyword: get_keyword_matches,
        Datatype: get_datatype_matches,
        Procedure: get_procedure_matches
    }

    def update_tables(self, tables):
        self.dbmetadata['tables'] = tables

    def update_views(self, views):
        self.dbmetadata['views'] = views

    def update_functions(self, functions):
        self.dbmetadata['functions'] = functions

    def update_procedures(self, procedures):
        self.dbmetadata['procedures'] = procedures

    def extend_query_history(self, text):
        # Currently only load keyword preferences from history, not names
        self.prioritizer.update_keywords(text)

    def init_prioritization_from_history(self, history, n_recent=100):
        cnt = 0
        # load the most n recent history
        for recent in history.load_history_strings():
            cnt += 1
            if cnt > n_recent:
                break
            self.extend_query_history(recent)

