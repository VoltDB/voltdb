/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

SetOperator::SetOperator(const std::vector<Table*>& input_tables,
    TempTable* output_table,
    bool is_all)
    : m_input_tables(input_tables),
      m_output_table(output_table),
      m_is_all(is_all)
{ }

SetOperator::~SetOperator() {}

SetOperator* SetOperator::getSetOperator(SetOpType setopType,
    const std::vector<TableReference>& input_tablerefs,
    TempTable* output_table,
    bool need_send_children_result)
{
    std::vector<Table*> input_tables;
    input_tables.reserve(input_tablerefs.size());
    for (int i = 0; i < input_tablerefs.size(); ++i)
    {
        input_tables.push_back(input_tablerefs[i].getTable());
    }

    switch (setopType) {
        case SETOP_TYPE_UNION_ALL:
            // UNION_ALL and UNION don't need to send individual children results up to coordinator at all.
            return new UnionSetOperator(input_tables, output_table, true);
        case SETOP_TYPE_UNION:
            return new UnionSetOperator(input_tables, output_table, false);
        case SETOP_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>(
                input_tables, output_table, true, true, need_send_children_result);
        case SETOP_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>(
                input_tables, output_table, false, true, need_send_children_result);
        case SETOP_TYPE_INTERSECT_ALL:
            // It doesn't make much sense to perform the INTERSECT_ALL and INTERSECT at the partition and the coordinator
            // levels for a MP query. It needs to be done in one place at the coordinator. The partition just need
            // to channel the children results through as is.
            if (need_send_children_result) {
                return new PassThroughSetOperator(input_tables, output_table);
            } else {
                return new ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>(
                    input_tables, output_table, true, false);
            }
        case SETOP_TYPE_INTERSECT:
            if (need_send_children_result) {
                return new PassThroughSetOperator(input_tables, output_table);
            } else {
                return new ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>(
                    input_tables, output_table, false, false);
            }
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", setopType);
            return NULL;
    }
}

SetOperator* SetOperator::getReceiveSetOperator(SetOpType setopType,
    const std::vector<Table*>& input_tables,
    TempTable* output_table)
{
    switch (setopType) {
        case SETOP_TYPE_UNION:
            // UNION_ALL does not require the coordinator SetOp node at all.
            return new UnionSetOperator(
                input_tables, output_table, false);
        case SETOP_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator<TableTuplePartialHasher, TableTuplePartialEqualityChecker>(
                input_tables, output_table, true, true);
        case SETOP_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator<TableTuplePartialHasher, TableTuplePartialEqualityChecker>(
                input_tables, output_table, false, true);
        case SETOP_TYPE_INTERSECT_ALL:
            return new ExceptIntersectSetOperator<TableTuplePartialHasher, TableTuplePartialEqualityChecker>(
                    input_tables, output_table, true, false);
        case SETOP_TYPE_INTERSECT:
            return new ExceptIntersectSetOperator<TableTuplePartialHasher, TableTuplePartialEqualityChecker>(
                    input_tables, output_table, false, false);
        default:
            VOLT_ERROR("Unsupported tuple receive set operation '%d'.", setopType);
            return NULL;
    }
}

UnionSetOperator::UnionSetOperator(const std::vector<Table*>& input_tables,
    TempTable* output_table,
    bool is_all)
    : SetOperatorImpl<TableTupleHasher, TableTupleEqualityChecker>(input_tables, output_table, is_all)
{ }

bool UnionSetOperator::processTuples()
{
    // Set to keep candidate tuples.
    TupleSet tuples(0, getHasher(), getEqualityChecker());

    //
    // For each input table, grab their TableIterator and then append all of its tuples
    // to our ouput table. Only distinct tuples are retained.
    //
    for (size_t ctr = 0, cnt = this->m_input_tables.size(); ctr < cnt; ctr++) {
        Table* input_table = this->m_input_tables[ctr];
        assert(input_table);
        TableIterator iterator = input_table->iterator();
        TableTuple tuple(input_table->schema());
        while (iterator.next(tuple)) {
            if (this->m_is_all || needToInsert(tuple, tuples)) {
                // we got tuple to insert
                this->m_output_table->insertTempTuple(tuple);
            }
        }
    }
    return true;
}

PassThroughSetOperator::PassThroughSetOperator(const std::vector<Table*>& input_tables,
    TempTable* output_table)
    : SetOperator(input_tables, output_table, true)
{ }

bool PassThroughSetOperator::processTuples()
{
    assert(m_output_table);

    // Simply iterate over the children's output, tag each row by adding an extra column,
    // and write the updated tuple to the output
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
