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

from voltsql.prioritization import PrevalenceCounter


def test_prevalence_counter():
    counter = PrevalenceCounter()
    sql = '''SELECT * FROM foo WHERE bar GROUP BY baz;
             select * from foo;
             SELECT * FROM foo WHERE bar GROUP
             BY baz'''
    counter.update(sql)

    keywords = ['SELECT', 'FROM', 'GROUP BY']
    expected = [3, 3, 2]
    kw_counts = [counter.keyword_count(x) for x in keywords]
    assert kw_counts == expected
    assert counter.keyword_count('NOSUCHKEYWORD') == 0

    names = ['foo', 'bar', 'baz']
    name_counts = [counter.name_count(x) for x in names]
    assert name_counts == [3, 2, 2]
