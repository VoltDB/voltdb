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

#ifndef HSTORESETOPERATOR_H
#define HSTORESETOPERATOR_H

#include "common/common.h"
#include "common/tabletuple.h"
#include "plannodes/abstractplannode.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"

#include <vector>

namespace voltdb {

// Base SetOP class to define the interface
struct SetOperator {
    typedef AbstractPlanNode::TableReference TableReference;

    virtual bool processTuples() = 0;

    virtual SetOpType getSetOpType() const = 0;

    // Factory method to create a set op to work with a single partition
    static SetOperator* getSetOperator(SetOpType setopType,
                const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool need_send_children_result);

    // Factory method to create a set op to aggregate cross-partition results
    static SetOperator* getReceiveSetOperator(SetOpType setopType,
                const std::vector<Table*>& input_tables,
                TempTable* output_table);

    virtual ~SetOperator();

protected:

    SetOperator(const std::vector<Table*>& input_tables,
                TempTable* output_table,
                bool is_all);

    std::vector<Table*> m_input_tables;
    TempTable* const m_output_table;
    bool const m_is_all;
};

//
template<typename Hasher, typename EqualityChecker>
struct SetOperatorImpl : public SetOperator {
    typedef boost::unordered_set<TableTuple, Hasher, EqualityChecker>
        TupleSet;
    typedef boost::unordered_map<TableTuple, size_t, Hasher, EqualityChecker>
        TupleMap;

    // for debugging - may be unused
    static void printTupleMap(const char* nonce, TupleMap &tuples);
    static void printTupleSet(const char* nonce, TupleSet &tuples);

protected:

    Hasher getHasher(int size = 0) const;
    EqualityChecker getEqualityChecker(int size = 0) const;

    SetOperatorImpl(const std::vector<Table*>& input_tables,
                TempTable* output_table,
                bool is_all);

};

// UNION(ALL) Set Op
struct UnionSetOperator : public SetOperatorImpl<TableTupleHasher, TableTupleEqualityChecker> {
    UnionSetOperator(const std::vector<Table*>& input_tables,
                     TempTable* output_table,
                     bool is_all);

    SetOpType getSetOpType() const
    {
        return (m_is_all) ? SETOP_TYPE_UNION_ALL : SETOP_TYPE_UNION;
    }

private:
    typedef SetOperatorImpl<TableTupleHasher, TableTupleEqualityChecker>::TupleSet TupleSet;

    bool processTuples();

    bool needToInsert(const TableTuple& tuple, TupleSet& tuples)
    {
        bool result = tuples.find(tuple) == tuples.end();
        if (result) {
            tuples.insert(tuple);
        }
        return result;
    }
};

// EXCEPT/INTERSECT (ALL) Set Op. The Hasher and the EqualityChecker types can be either
// regular TableTupleHasher, TableTupleEqualityChecker to do the equality check using all tuple columns
// or TableTuplePartialHasher, TableTuplePartialEqualityChecker to compare tuples using
// a subset of the columns (to be used at the coordinator fragment to ignore the temp TAG column)
template<typename Hasher, typename EqualityChecker>
struct ExceptIntersectSetOperator : public SetOperatorImpl<Hasher, EqualityChecker> {
    ExceptIntersectSetOperator(const std::vector<Table*>& input_tables,
                               TempTable* output_table,
                               bool is_all,
                               bool is_except,
                               bool need_send_children_result = false);

    SetOpType getSetOpType() const;

private:
    typedef typename SetOperatorImpl<Hasher, EqualityChecker>::TupleMap TupleMap;
    typedef typename TupleMap::iterator TupleMapIterator;
    typedef typename TupleMap::const_iterator TupleMapConstIterator;

    bool processTuples();
    void collectTuples(Table& input_table, TupleMap& tuple_map);
    void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
    void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
    void send_child_rows_up(TupleMap& child_tuples, int child_id);

    bool const m_is_except;
    bool const m_need_send_children_result;

};

// Dummy Set Op to send children results to the coordinator
struct PassThroughSetOperator : public SetOperator {
    PassThroughSetOperator(const std::vector<Table*>& input_tables,
                     TempTable* output_table);

    SetOpType getSetOpType() const {
        return SETOP_TYPE_NONE;
    }

private:
    bool processTuples();
};

// Table size comparator
struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const
    {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};

// Tuple Hasher that operates on a subset of the tuple's columns
struct TableTuplePartialHasher : std::unary_function<TableTuple, std::size_t>
{
    template<typename Iter>
    TableTuplePartialHasher(Iter begin, Iter end) :
        m_columns(begin, end)
    {}
    TableTuplePartialHasher(int begin, int end) :
        m_columns(end - begin)
    {
        while(begin != end) {
           m_columns.push_back(begin++);
        }
    }
    /** Generate a 64-bit number for the key value */
    inline size_t operator()(TableTuple tuple) const
    {
        return tuple.hashCode(m_columns.begin(), m_columns.end());
    }
private:
    std::vector<int> m_columns;
};

// Tuple Equality Checker that that operates on a subset of the tuple's columns
class TableTuplePartialEqualityChecker {
public:
    template<typename Iter>
    TableTuplePartialEqualityChecker(Iter begin, Iter end) :
        m_columns(begin, end)
    {}

    TableTuplePartialEqualityChecker(int begin, int end) :
        m_columns(end - begin)
    {
        while(begin != end) {
           m_columns.push_back(begin++);
        }
    }

    inline bool operator()(const TableTuple lhs, const TableTuple rhs) const {
        return lhs.equalsNoSchemaCheck(rhs, m_columns.begin(), m_columns.end());
    }
private:
    std::vector<int> m_columns;
};

template<>
inline
TableTupleHasher SetOperatorImpl<TableTupleHasher, TableTupleEqualityChecker>::getHasher(int) const {
    return TableTupleHasher();
}
template<>
inline
TableTupleEqualityChecker SetOperatorImpl<TableTupleHasher, TableTupleEqualityChecker>::getEqualityChecker(int) const {
    return TableTupleEqualityChecker();
}

template<>
inline
TableTuplePartialHasher SetOperatorImpl<TableTuplePartialHasher, TableTuplePartialEqualityChecker>::getHasher(int size) const {
    assert (size > 1 );
    return TableTuplePartialHasher(0, size - 1);
}
template<>
inline
TableTuplePartialEqualityChecker SetOperatorImpl<TableTuplePartialHasher, TableTuplePartialEqualityChecker>::getEqualityChecker(int size) const {
    assert (size > 1 );
    return TableTuplePartialEqualityChecker(0, size - 1);
}

template<typename Hasher, typename EqualityChecker>
SetOperatorImpl<Hasher, EqualityChecker>::SetOperatorImpl(const std::vector<Table*>& input_tables,
                TempTable* output_table,
                bool is_all)
    : SetOperator(input_tables, output_table, is_all)
{ }

// for debugging - may be unused
template<typename Hasher, typename EqualityChecker>
void SetOperatorImpl<Hasher, EqualityChecker>::printTupleMap(const char* nonce, TupleMap &tuples)
{
    printf("Printing TupleMap (%s): ", nonce);
    for (typename TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        printf("%s, ", tuple.debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

// for debugging - may be unused
template<typename Hasher, typename EqualityChecker>
void SetOperatorImpl<Hasher, EqualityChecker>::printTupleSet(const char* nonce, TupleSet &tuples)
{
    printf("Printing TupleSet (%s): ", nonce);
    for (typename TupleSet::const_iterator setIt = tuples.begin(); setIt != tuples.end(); ++setIt) {
        printf("%s, ", setIt->debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

template<typename Hasher, typename EqualityChecker>
ExceptIntersectSetOperator<Hasher, EqualityChecker>::ExceptIntersectSetOperator(
                               const std::vector<Table*>& input_tables,
                               TempTable* output_table,
                               bool is_all,
                               bool is_except,
                               bool need_send_children_result)
    : SetOperatorImpl<Hasher, EqualityChecker>(input_tables, output_table, is_all),
      m_is_except(is_except),
      m_need_send_children_result(need_send_children_result)
{
    // need_send_children_result can be TRUE only for EXCEPT(ALL) Set ops
    assert((is_except && need_send_children_result) || !need_send_children_result);
}

template<typename Hasher, typename EqualityChecker>
inline
SetOpType ExceptIntersectSetOperator<Hasher, EqualityChecker>::getSetOpType() const {
    if (this->m_is_all) {
        return (m_is_except) ? SETOP_TYPE_EXCEPT_ALL : SETOP_TYPE_INTERSECT_ALL;
    }
    return (m_is_except) ? SETOP_TYPE_EXCEPT : SETOP_TYPE_INTERSECT;
}

template<typename Hasher, typename EqualityChecker>
bool ExceptIntersectSetOperator<Hasher, EqualityChecker>::processTuples()
{
    assert( ! this->m_input_tables.empty());

    if ( ! this->m_is_except) {
        // For intersect we want to start with the smallest table
        std::vector<Table*>::iterator minTableIt =
            std::min_element(this->m_input_tables.begin(), this->m_input_tables.end(), TableSizeLess());
        std::swap(this->m_input_tables[0], *minTableIt);
    }
    // Collect all tuples from the first set
    Table* input_table = this->m_input_tables[0];

    int column_cnt = input_table->columnCount();
    // Map to keep candidate tuples. The key is the tuple itself
    // The value - tuple's repeat count in the final table.
    TupleMap tuples(0, this->getHasher(column_cnt), this->getEqualityChecker(column_cnt));
    collectTuples(*input_table, tuples);

    //
    // For each remaining input table, collect its tuple into a separate map
    // and substract/intersect it from/with the first one
    //
    TupleMap next_tuples(0, this->getHasher(column_cnt), this->getEqualityChecker(column_cnt));
    for (size_t ctr = 1, cnt = this->m_input_tables.size(); ctr < cnt; ctr++) {
        next_tuples.clear();
        input_table = this->m_input_tables[ctr];
        assert(input_table);
        collectTuples(*input_table, next_tuples);
        if (m_is_except) {
            exceptTupleMaps(tuples, next_tuples);
            if (m_need_send_children_result) {
                send_child_rows_up(next_tuples, ctr);
            }
        } else {
            intersectTupleMaps(tuples, next_tuples);
        }
    }

    // Insert remaining tuples to the output table
    TableTuple out_tuple = this->m_output_table->tempTuple();
    for (TupleMapConstIterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        if (m_need_send_children_result) {
            out_tuple.setNValues(0, tuple, 0, column_cnt);
            // The first table from EXCPECT
            out_tuple.setNValue(column_cnt, ValueFactory::getBigIntValue(0));
        }

        for (size_t i = 0; i < mapIt->second; ++i) {
            if (m_need_send_children_result) {
                out_tuple.setNValues(0, tuple, 0, column_cnt);
                this->m_output_table->insertTempTuple(out_tuple);
            } else {
                this->m_output_table->insertTempTuple(tuple);
            }
        }
    }
    return true;
}

template<typename Hasher, typename EqualityChecker>
void ExceptIntersectSetOperator<Hasher, EqualityChecker>::send_child_rows_up(TupleMap& child_tuples, int child_id) {
    TableTuple out_tuple = this->m_output_table->tempTuple();
    TupleMapIterator child_it = child_tuples.begin();
    while(child_it != child_tuples.end()) {
        TableTuple tuple = child_it->first;
        int count = child_it->second;
        int child_column_cnt = tuple.sizeInValues();
        assert(child_column_cnt + 1 == out_tuple.sizeInValues());
        // Output current tuple multiple times according to a tuple count
        for (int i = 0; i < count; ++i) {
            out_tuple.setNValues(0, tuple, 0, child_column_cnt);
            out_tuple.setNValue(child_column_cnt, ValueFactory::getBigIntValue(child_id));
            this->m_output_table->insertTempTuple(out_tuple);
        }
        // Move to the next tuple;
        ++child_it;
    }
}

template<typename Hasher, typename EqualityChecker>
void ExceptIntersectSetOperator<Hasher, EqualityChecker>::collectTuples(Table& input_table, TupleMap& tuple_map)
{
    TableIterator iterator = input_table.iterator();
    TableTuple tuple(input_table.schema());
    while (iterator.next(tuple)) {
        TupleMapIterator mapIt = tuple_map.find(tuple);
        if (mapIt == tuple_map.end()) {
            tuple_map.insert(std::make_pair(tuple, 1));
        } else if (this->m_is_all) {
            ++(mapIt->second);
        }
    }
}

template<typename Hasher, typename EqualityChecker>
void ExceptIntersectSetOperator<Hasher, EqualityChecker>::exceptTupleMaps(TupleMap& map_a, TupleMap& map_b)
{
    TupleMapIterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMapIterator it_b = map_b.find(it_a->first);
        if (it_b != map_b.end()) {
            if (it_a->second > it_b->second) {
                it_a->second -= it_b->second;
                if (m_need_send_children_result) {
                    if (this->m_is_all) {
                        // All matching B rows are already accounted for and need to be removed so
                        // they won't contribute to the cross-partition EXCEPT ALL.
                        // Still need them for a simple EXCEPT
                        map_b.erase(it_b);
                    }
                }
            }
            else {
                if (m_need_send_children_result) {
                    // See the comment above
                    if (this->m_is_all) {
                        if (it_a->second == it_b->second) {
                            map_b.erase(it_b);
                        } else {
                            it_b->second -= it_a->second;
                        }
                    }
                }
                it_a = map_a.erase(it_a);
                continue;
            }
        }
        ++it_a;
    }
}

template<typename Hasher, typename EqualityChecker>
void ExceptIntersectSetOperator<Hasher, EqualityChecker>::intersectTupleMaps(TupleMap& map_a, TupleMap& map_b)
{
    TupleMapIterator it_a = map_a.begin();
    while(it_a != map_a.end()) {
        TupleMapIterator it_b = map_b.find(it_a->first);
        if (it_b == map_b.end()) {
            it_a = map_a.erase(it_a);
        } else {
            it_a->second = std::min(it_a->second, it_b->second);
            ++it_a;
        }
    }
}

}

#endif
