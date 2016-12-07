/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "setoperator.h"

namespace voltdb {

struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const
    {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};


SetOperator::SetOperator(const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool is_all,
                bool need_children_result)
    : m_input_tablerefs(input_tablerefs),
      m_output_table(output_table),
      m_is_all(is_all),
      m_need_children_result(need_children_result)
{ }
SetOperator::~SetOperator() {}


UnionSetOperator::UnionSetOperator(const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                 bool is_all)
    : SetOperator(input_tablerefs, output_table, is_all, false)
{ }

bool UnionSetOperator::processTuples()
{
    // Set to keep candidate tuples.
    TupleSet tuples;

    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table. Only distinct tuples are retained.
    //
    for (size_t ctr = 0, cnt = m_input_tablerefs.size(); ctr < cnt; ctr++) {
        Table* input_table = m_input_tablerefs[ctr].getTable();
        assert(input_table);
        TableIterator iterator = input_table->iterator();
        TableTuple tuple(input_table->schema());
        while (iterator.next(tuple)) {
            if (m_is_all || needToInsert(tuple, tuples)) {
                // we got tuple to insert
                m_output_table->insertTempTuple(tuple);
            }
        }
    }
    return true;
}

ExceptIntersectSetOperator::ExceptIntersectSetOperator(const std::vector<TableReference>& input_tablerefs,
                               TempTable* output_table,
                               bool is_all,
                               bool is_except,
                               bool need_children_result)
    : SetOperator(input_tablerefs, output_table, is_all, need_children_result),
      m_is_except(is_except),
      m_input_tables(input_tablerefs.size())
{ }

// for debugging - may be unused
void SetOperator::printTupleMap(const char* nonce, TupleMap &tuples)
{
    printf("Printing TupleMap (%s): ", nonce);
    for (TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        printf("%s, ", tuple.debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

// for debugging - may be unused
void SetOperator::printTupleSet(const char* nonce, TupleSet &tuples)
{
    printf("Printing TupleSet (%s): ", nonce);
    for (TupleSet::const_iterator setIt = tuples.begin(); setIt != tuples.end(); ++setIt) {
        printf("%s, ", setIt->debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

bool ExceptIntersectSetOperator::processTuples()
{
    // Map to keep candidate tuples. The key is the tuple itself
    // The value - tuple's repeat count in the final table.
    TupleMap tuples;

    assert( ! m_input_tables.empty());

    size_t ii = m_input_tablerefs.size();
    while (ii--) {
        m_input_tables[ii] = m_input_tablerefs[ii].getTable();
    }

    if ( ! m_is_except) {
        // For intersect we want to start with the smallest table
        std::vector<Table*>::iterator minTableIt =
            std::min_element(m_input_tables.begin(), m_input_tables.end(), TableSizeLess());
        std::swap(m_input_tables[0], *minTableIt);
    }
    // Collect all tuples from the first set
    Table* input_table = m_input_tables[0];
    collectTuples(*input_table, tuples);

    //
    // For each remaining input table, collect its tuple into a separate map
    // and substract/intersect it from/with the first one
    //
    TupleMap next_tuples;
    for (size_t ctr = 1, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
        next_tuples.clear();
        input_table = m_input_tables[ctr];
        assert(input_table);
        collectTuples(*input_table, next_tuples);
        if (m_is_except) {
            exceptTupleMaps(tuples, next_tuples);
        } else {
            intersectTupleMaps(tuples, next_tuples);
        }
    }

    // Insert remaining tuples to the output table
    for (TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        for (size_t i = 0; i < mapIt->second; ++i) {
            m_output_table->insertTempTuple(tuple);
        }
    }
    return true;
}

void ExceptIntersectSetOperator::collectTuples(Table& input_table, TupleMap& tuple_map)
{
    TableIterator iterator = input_table.iterator();
    TableTuple tuple(input_table.schema());
    while (iterator.next(tuple)) {
        TupleMap::iterator mapIt = tuple_map.find(tuple);
        if (mapIt == tuple_map.end()) {
            tuple_map.insert(std::make_pair(tuple, 1));
        } else if (m_is_all) {
            ++(mapIt->second);
        }
    }
}

void ExceptIntersectSetOperator::exceptTupleMaps(TupleMap& map_a, TupleMap& map_b)
{
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b != map_b.end()) {
            if (it_a->second > it_b->second) {
                it_a->second -= it_b->second;
            }
            else {
                it_a = map_a.erase(it_a);
                continue;
            }
        }
        ++it_a;
    }
}

void ExceptIntersectSetOperator::intersectTupleMaps(TupleMap& map_a, TupleMap& map_b)
{
    TupleMap::iterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMap::iterator it_b = map_b.find(it_a->first);
        if (it_b == map_b.end()) {
            it_a = map_a.erase(it_a);
        } else {
            it_a->second = std::min(it_a->second, it_b->second);
            ++it_a;
        }
    }
}

SetOperator* SetOperator::getSetOperator(SetOpType setopType,
                const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool need_children_result)
{
    switch (setopType) {
        case SETOP_TYPE_UNION_ALL:
            return new UnionSetOperator(input_tablerefs, output_table, true);
        case SETOP_TYPE_UNION:
            return new UnionSetOperator(input_tablerefs, output_table, false);
        case SETOP_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator(input_tablerefs, output_table,
                true, true, need_children_result);
        case SETOP_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator(input_tablerefs, output_table,
                false, true, need_children_result);
        case SETOP_TYPE_INTERSECT_ALL:
            return new ExceptIntersectSetOperator(input_tablerefs, output_table,
                true, false, need_children_result);
        case SETOP_TYPE_INTERSECT:
            return new ExceptIntersectSetOperator(input_tablerefs, output_table,
                false, false, need_children_result);
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", setopType);
            return NULL;
    }
}

}
