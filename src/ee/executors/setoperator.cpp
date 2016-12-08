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

#include "common/ValueFactory.hpp"

namespace voltdb {

struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const
    {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};

SetOperator::SetOperator(const std::vector<Table*>& input_tables,
                TempTable* output_table,
                bool is_all)
    : m_input_tables(input_tables),
      m_output_table(output_table),
      m_is_all(is_all)
{ }

SetOperator::~SetOperator() {}

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

SetOperator* SetOperator::getSetOperator(SetOpType setopType,
                const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool need_children_result)
{
    std::vector<Table*> input_vectors;
    input_vectors.reserve(input_tablerefs.size());
    for (int i = 0; i < input_tablerefs.size(); ++i)
    {
        input_vectors.push_back(input_tablerefs[i].getTable());
    }
    return SetOperator::getSetOperator(setopType, input_vectors, output_table, need_children_result);
}

SetOperator* SetOperator::getSetOperator(SetOpType setopType,
                const std::vector<Table*>& input_tables,
                TempTable* output_table,
                bool need_children_result)
{
    switch (setopType) {
        case SETOP_TYPE_UNION_ALL:
            return new UnionSetOperator(input_tables, output_table, true);
        case SETOP_TYPE_UNION:
            return new UnionSetOperator(input_tables, output_table, false);
        case SETOP_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator(input_tables, output_table,
                true, true, need_children_result);
        case SETOP_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator(input_tables, output_table,
                false, true, need_children_result);
        case SETOP_TYPE_INTERSECT_ALL:
            if (need_children_result) {
                return new PassThroughSetOperator(input_tables, output_table);
            } else {
                return new ExceptIntersectSetOperator(input_tables, output_table,
                    true, false, false);
            }
        case SETOP_TYPE_INTERSECT:
            if (need_children_result) {
                return new PassThroughSetOperator(input_tables, output_table);
            } else {
                return new ExceptIntersectSetOperator(input_tables, output_table,
                    false, false, false);
            }
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", setopType);
            return NULL;
    }
}

UnionSetOperator::UnionSetOperator(const std::vector<Table*>& input_tables,
                TempTable* output_table,
                 bool is_all)
    : SetOperator(input_tables, output_table, is_all)
{ }

bool UnionSetOperator::processTuples()
{
    // Set to keep candidate tuples.
    TupleSet tuples;

    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table. Only distinct tuples are retained.
    //
    for (size_t ctr = 0, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
        Table* input_table = m_input_tables[ctr];
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

ExceptIntersectSetOperator::ExceptIntersectSetOperator(const std::vector<Table*>& input_tables,
                               TempTable* output_table,
                               bool is_all,
                               bool is_except,
                               bool need_children_result)
    : SetOperator(input_tables, output_table, is_all),
      m_is_except(is_except),
      m_need_children_result(need_children_result)
{ }

bool ExceptIntersectSetOperator::processTuples()
{
    // Map to keep candidate tuples. The key is the tuple itself
    // The value - tuple's repeat count in the final table.
    TupleMap tuples;

    assert( ! m_input_tables.empty());

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
            if (m_need_children_result) {
                send_child_rows_up(next_tuples, ctr);
            }
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
                // Remove rows that already was accounted for from the child
                if (m_need_children_result) {
                    map_b.erase(it_b);
                }
            }
            else {
                if (m_need_children_result) {
                    if (it_a->second == it_b->second) {
                        map_b.erase(it_b);
                    } else {
                        it_b->second -= it_a->second;
                    }
                }
                it_a = map_a.erase(it_a);
                continue;
            }
        }
        ++it_a;
    }
}

void ExceptIntersectSetOperator::send_child_rows_up(TupleMap& child_tuples, int child_id) {
    TableTuple out_tuple = m_output_table->tempTuple();
    TupleMap::iterator child_it = child_tuples.begin();
    while(child_it != child_tuples.end()) {
        TableTuple tuple = child_it->first;
        int count = child_it->second;
        int child_column_cnt = tuple.sizeInValues();
        // Output current tuple multiple times according to a tuple count
        for (int i = 0; i < count; ++i) {
            out_tuple.setNValues(0, tuple, 0, child_column_cnt);
            out_tuple.setNValue(child_column_cnt, ValueFactory::getBigIntValue(child_id));
            m_output_table->insertTempTuple(out_tuple);
        }
        // Move to the next tuple;
        ++child_it;
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

PassThroughSetOperator::PassThroughSetOperator(const std::vector<Table*>& input_tables,
                TempTable* output_table)
    : SetOperator(input_tables, output_table, true)
{ }

bool PassThroughSetOperator::processTuples()
{
    assert(m_output_table);

    TableTuple out_tuple = m_output_table->tempTuple();
    for (size_t ctr = 0, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
        Table* input_table = m_input_tables[ctr];
        assert(input_table);
        assert(m_output_table->schema()->columnCount() == input_table->schema()->columnCount() + 1);
        int input_columns = input_table->schema()->columnCount();
        TableIterator iterator = input_table->iterator();
        TableTuple tuple(input_table->schema());
        while (iterator.next(tuple)) {
            out_tuple.setNValues(0, tuple, 0, input_columns);
            out_tuple.setNValue(input_columns, ValueFactory::getBigIntValue(ctr));
            m_output_table->insertTempTuple(out_tuple);
        }
    }
    return true;
}

}
