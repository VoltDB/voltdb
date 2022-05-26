/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include "unionexecutor.h"

#include "plannodes/unionnode.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"

namespace voltdb {

namespace detail {

struct SetOperator {
    typedef boost::unordered_set<TableTuple, TableTupleHasher, TableTupleEqualityChecker>
        TupleSet;
    typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
        TupleMap;
    typedef AbstractPlanNode::TableReference TableReference;

    SetOperator(const std::vector<TableReference>& input_tablerefs,
                AbstractTempTable* output_table,
                bool is_all)
        : m_input_tablerefs(input_tablerefs), m_output_table(output_table), m_is_all(is_all)
    { }
    virtual ~SetOperator() {}

    virtual bool processTuples() = 0;

    static SetOperator* getSetOperator(UnionPlanNode* node);

    // for debugging - may be unused
    static void printTupleMap(const char* nonce, TupleMap &tuples);
    static void printTupleSet(const char* nonce, TupleSet &tuples);

protected:
    const std::vector<TableReference>& m_input_tablerefs;
    AbstractTempTable* const m_output_table;
    bool const m_is_all;
};

struct UnionSetOperator : public SetOperator {
    UnionSetOperator(const std::vector<TableReference>& input_tablerefs,
                     AbstractTempTable* output_table,
                     bool is_all)
        : SetOperator(input_tablerefs, output_table, is_all)
    { }
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
        vassert(input_table);
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

struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const
    {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};

struct ExceptIntersectSetOperator : public SetOperator {
    ExceptIntersectSetOperator(const std::vector<TableReference>& input_tablerefs,
                               AbstractTempTable* output_table,
                               bool is_all,
                               bool is_except)
        : SetOperator(input_tablerefs, output_table, is_all)
        , m_is_except(is_except)
        , m_input_tables(input_tablerefs.size())
    { }

private:
    bool processTuples();
    void collectTuples(Table& input_table, TupleMap& tuple_map);
    void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
    void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);

    bool const m_is_except;
    std::vector<Table*> m_input_tables;
};

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

    vassert( ! m_input_tables.empty());

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
        vassert(input_table);
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

SetOperator* SetOperator::getSetOperator(UnionPlanNode* node)
{
    UnionType unionType = node->getUnionType();
    switch (unionType) {
        case UNION_TYPE_UNION_ALL:
            return new UnionSetOperator(node->getInputTableRefs(), node->getTempOutputTable(), true);
        case UNION_TYPE_UNION:
            return new UnionSetOperator(node->getInputTableRefs(), node->getTempOutputTable(), false);
        case UNION_TYPE_EXCEPT_ALL:
            return new ExceptIntersectSetOperator(node->getInputTableRefs(), node->getTempOutputTable(),
                true, true);
        case UNION_TYPE_EXCEPT:
            return new ExceptIntersectSetOperator(node->getInputTableRefs(), node->getTempOutputTable(),
                false, true);
        case UNION_TYPE_INTERSECT_ALL:
            return new ExceptIntersectSetOperator(node->getInputTableRefs(), node->getTempOutputTable(),
                true, false);
        case UNION_TYPE_INTERSECT:
            return new ExceptIntersectSetOperator(node->getInputTableRefs(), node->getTempOutputTable(),
                false, false);
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", unionType);
            return NULL;
    }
}

} // namespace detail

UnionExecutor::UnionExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node)
    : AbstractExecutor(engine, abstract_node)
{ }

bool UnionExecutor::p_init(AbstractPlanNode* abstract_node,
                           const ExecutorVector& executorVector)
{
    VOLT_TRACE("init Union Executor");
    vassert(! executorVector.isLargeQuery());

    UnionPlanNode* node = dynamic_cast<UnionPlanNode*>(abstract_node);
    vassert(node);

    //
    // First check to make sure they have the same number of columns
    //
    vassert(node->getInputTableCount() > 0);

    Table* input_table_0 = node->getInputTable(0);
    const TupleSchema *table_0_schema = input_table_0->schema();

    for (int table_ctr = 1, table_cnt = (int)node->getInputTableCount();
         table_ctr < table_cnt;
         ++table_ctr) {
        Table* input_table_n = node->getInputTable(table_ctr);
        if (input_table_0->columnCount() != input_table_n->columnCount()) {
            VOLT_ERROR("Table '%s' has %d columns, but table '%s' has %d"
                       " columns",
                       input_table_0->name().c_str(),
                       input_table_0->columnCount(),
                       input_table_n->name().c_str(),
                       input_table_n->columnCount());
            return false;
        }

        //
        // Then check that they have the same types
        //

        // iterate over all columns in the first table
        for (int col_ctr = 0, col_cnt = table_0_schema->columnCount();
             col_ctr < col_cnt;
             col_ctr++) {
            // get the type for the current column
            ValueType type_0 = table_0_schema->columnType(col_ctr);

            const TupleSchema *table_n_schema = input_table_n->schema();
            ValueType type_n = table_n_schema->columnType(col_ctr);
            if (type_0 != type_n) {
                VOLT_ERROR("Table '%s' has value type '%s' for column '%d',"
                           " table '%s' has value type '%s' for column '%d'",
                           input_table_0->name().c_str(), getTypeName(type_0).c_str(), col_ctr,
                           input_table_n->name().c_str(), getTypeName(type_n).c_str(), col_ctr);
                return false;
            }
        }
    }
    //
    // Create our output table that will hold all the tuples that we are appending into.
    // Since we're are assuming that all of the tables have the same number of columns with
    // the same format. Therefore, we will just grab the first table in the list
    //
    node->setOutputTable(TableFactory::buildCopiedTempTable(node->getInputTable(0)->name(),
                                                            node->getInputTable(0),
                                                            executorVector));

    m_setOperator.reset(detail::SetOperator::getSetOperator(node));
    return true;
}

bool UnionExecutor::p_execute(const NValueArray &params) {
    return m_setOperator->processTuples();
}

}
