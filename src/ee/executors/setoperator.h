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

struct SetOperator {
    typedef boost::unordered_set<TableTuple, TableTupleHasher, TableTupleEqualityChecker>
        TupleSet;
    typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
        TupleMap;
    typedef AbstractPlanNode::TableReference TableReference;

    SetOperator(const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool is_all,
                bool need_children_result);
    virtual ~SetOperator();

    virtual bool processTuples() = 0;

    static SetOperator* getSetOperator(SetOpType setopType,
                const std::vector<TableReference>& input_tablerefs,
                TempTable* output_table,
                bool need_children_result);

    // for debugging - may be unused
    static void printTupleMap(const char* nonce, TupleMap &tuples);
    static void printTupleSet(const char* nonce, TupleSet &tuples);

protected:
    const std::vector<TableReference>& m_input_tablerefs;
    TempTable* const m_output_table;
    bool const m_is_all;
    bool const m_need_children_result;
};

struct UnionSetOperator : public SetOperator {
    UnionSetOperator(const std::vector<TableReference>& input_tablerefs,
                     TempTable* output_table,
                     bool is_all);
private:
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

struct ExceptIntersectSetOperator : public SetOperator {
    ExceptIntersectSetOperator(const std::vector<TableReference>& input_tablerefs,
                               TempTable* output_table,
                               bool is_all,
                               bool is_except,
                               bool need_children_result);

private:
    bool processTuples();
    void collectTuples(Table& input_table, TupleMap& tuple_map);
    void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
    void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);

    bool const m_is_except;
    std::vector<Table*> m_input_tables;
};

}

#endif
