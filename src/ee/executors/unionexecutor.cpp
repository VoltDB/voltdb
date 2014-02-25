/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"

#include "unionexecutor.h"
#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "plannodes/unionnode.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tableiterator.h"
#include "storage/tablefactory.h"

namespace voltdb {

namespace detail {

struct SetOperator {
    typedef boost::unordered_set<TableTuple, TableTupleHasher, TableTupleEqualityChecker>
        TupleSet;
    typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
        TupleMap;

    SetOperator(std::vector<Table*>& input_tables, Table* output_table, bool is_all) :
        m_input_tables(input_tables), m_output_table(output_table), m_is_all(is_all)
        {}

    virtual ~SetOperator() {}

    bool processTuples() {
        return processTuplesDo();
    }

    static boost::shared_ptr<SetOperator> getSetOperator(UnionPlanNode* node);

    std::vector<Table*>& m_input_tables;

    // for debugging - may be unused
    void printTupleMap(const char* nonce, TupleMap &tuples);
    void printTupleSet(const char* nonce, TupleSet &tuples);

    protected:
        virtual bool processTuplesDo() = 0;

        Table* m_output_table;
        bool m_is_all;
};

struct UnionSetOperator : public SetOperator {
    UnionSetOperator(std::vector<Table*>& input_tables, Table* output_table, bool is_all) :
       SetOperator(input_tables, output_table, is_all)
       {}

    protected:
        bool processTuplesDo();

    private:
        bool needToInsert(const TableTuple& tuple, TupleSet& tuples);

};

bool UnionSetOperator::processTuplesDo() {

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
                if (!m_output_table->insertTuple(tuple)) {
                    VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                               " output table '%s'",
                               input_table->name().c_str(),
                               m_output_table->name().c_str());
                    return false;
                }
            }
        }
    }
    return true;
}

inline
bool UnionSetOperator::needToInsert(const TableTuple& tuple, TupleSet& tuples) {
    bool result = tuples.find(tuple) == tuples.end();
    if (result) {
        tuples.insert(tuple);
    }
    return result;
}

struct TableSizeLess {
    bool operator()(const Table* t1, const Table* t2) const {
        return t1->activeTupleCount() < t2->activeTupleCount();
    }
};

struct ExceptIntersectSetOperator : public SetOperator {
    ExceptIntersectSetOperator(std::vector<Table*>& input_tables, Table* output_table,
        bool is_all, bool is_except);

    protected:
        bool processTuplesDo();

    private:
        void collectTuples(Table& input_table, TupleMap& tuple_map);
        void exceptTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);
        void intersectTupleMaps(TupleMap& tuple_a, TupleMap& tuple_b);

        bool m_is_except;
};

ExceptIntersectSetOperator::ExceptIntersectSetOperator(
    std::vector<Table*>& input_tables, Table* output_table, bool is_all, bool is_except) :
        SetOperator(input_tables, output_table, is_all), m_is_except(is_except) {
    if (!is_except) {
        // For intersect we want to start with the smalest table
        std::vector<Table*>::iterator minTableIt =
            std::min_element(m_input_tables.begin(), m_input_tables.end(), TableSizeLess());
        std::swap( m_input_tables[0], *minTableIt);
    }

}

// for debugging - may be unused
void SetOperator::printTupleMap(const char* nonce, TupleMap &tuples) {
    printf("Printing TupleMap (%s): ", nonce);
    for (TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        printf("%s, ", tuple.debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

// for debugging - may be unused
void SetOperator::printTupleSet(const char* nonce, TupleSet &tuples) {
    printf("Printing TupleSet (%s): ", nonce);
    for (TupleSet::const_iterator setIt = tuples.begin(); setIt != tuples.end(); ++setIt) {
        printf("%s, ", setIt->debugNoHeader().c_str());
    }
    printf("\n");
    fflush(stdout);
}

bool ExceptIntersectSetOperator::processTuplesDo() {
    // Map to keep candidate tuples. The key is the tuple itself
    // The value - tuple's repeat count in the final table.
    TupleMap tuples;

    // Collect all tuples from the first set
    assert(!m_input_tables.empty());
    Table* input_table = m_input_tables[0];
    collectTuples(*input_table, tuples);

    //
    // For each remaining input table, collect its tuple into a separate map
    // and substract/intersect it from/with the first one
    //
    TupleMap next_tuples;
    for (size_t ctr = 1, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
        next_tuples.clear();
        Table* input_table = m_input_tables[ctr];
        assert(input_table);
        collectTuples(*input_table, next_tuples);
        if (m_is_except) {
            exceptTupleMaps(tuples, next_tuples);
        } else {
            intersectTupleMaps(tuples, next_tuples);
        }
    }

    // Insert remaining tuples to our ouput table
    for (TupleMap::const_iterator mapIt = tuples.begin(); mapIt != tuples.end(); ++mapIt) {
        TableTuple tuple = mapIt->first;
        for (size_t i = 0; i < mapIt->second; ++i) {
            if (!m_output_table->insertTuple(tuple)) {
                VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                           " output table '%s'",
                           m_input_tables[0]->name().c_str(),
                           m_output_table->name().c_str());
                return false;
            }
        }
    }
    return true;
}

void ExceptIntersectSetOperator::collectTuples(Table& input_table, TupleMap& tuple_map) {
    TableIterator iterator = input_table.iterator();
    TableTuple tuple(input_table.schema());
    while (iterator.next(tuple)) {
        TupleMap::iterator mapIt = tuple_map.find(tuple);
        if (mapIt == tuple_map.end()) {
            tuple_map.insert(std::make_pair(tuple, 1));
        } else if (m_is_all) {
            ++mapIt->second;
        }
    }
}

void ExceptIntersectSetOperator::exceptTupleMaps(TupleMap& map_a, TupleMap& map_b) {
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

void ExceptIntersectSetOperator::intersectTupleMaps(TupleMap& map_a, TupleMap& map_b) {
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

boost::shared_ptr<SetOperator> SetOperator::getSetOperator(UnionPlanNode* node) {
    UnionType unionType = node->getUnionType();
    switch (unionType) {
        case UNION_TYPE_UNION_ALL:
            return boost::shared_ptr<SetOperator>(
            new UnionSetOperator(node->getInputTables(), node->getOutputTable(), true));
        case UNION_TYPE_UNION:
            return boost::shared_ptr<SetOperator>(
            new UnionSetOperator(node->getInputTables(), node->getOutputTable(), false));
        case UNION_TYPE_EXCEPT_ALL:
            return boost::shared_ptr<SetOperator>(
            new ExceptIntersectSetOperator(node->getInputTables(), node->getOutputTable(), true, true));
        case UNION_TYPE_EXCEPT:
            return boost::shared_ptr<SetOperator>(
            new ExceptIntersectSetOperator(node->getInputTables(), node->getOutputTable(), false, true));
        case UNION_TYPE_INTERSECT_ALL:
            return boost::shared_ptr<SetOperator>(
            new ExceptIntersectSetOperator(node->getInputTables(), node->getOutputTable(), true, false));
        case UNION_TYPE_INTERSECT:
            return boost::shared_ptr<SetOperator>(
            new ExceptIntersectSetOperator(node->getInputTables(), node->getOutputTable(), false, false));
        default:
            VOLT_ERROR("Unsupported tuple set operation '%d'.", unionType);
            return boost::shared_ptr<SetOperator>();
    }
}

} // namespace detail

UnionExecutor::UnionExecutor(VoltDBEngine *engine, AbstractPlanNode* abstract_node) :
    AbstractExecutor(engine, abstract_node), m_setOperator()
{}

bool UnionExecutor::p_init(AbstractPlanNode* abstract_node,
                           TempTableLimits* limits)
{
    VOLT_TRACE("init Union Executor");

    UnionPlanNode* node = dynamic_cast<UnionPlanNode*>(abstract_node);
    assert(node);

    //
    // First check to make sure they have the same number of columns
    //
    assert(node->getInputTables().size() > 0);
    for (int table_ctr = 1, table_cnt = (int)node->getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
        if (node->getInputTables()[0]->columnCount() != node->getInputTables()[table_ctr]->columnCount()) {
            VOLT_ERROR("Table '%s' has %d columns, but table '%s' has %d"
                       " columns",
                       node->getInputTables()[0]->name().c_str(),
                       node->getInputTables()[0]->columnCount(),
                       node->getInputTables()[table_ctr]->name().c_str(),
                       node->getInputTables()[table_ctr]->columnCount());
            return false;
        }
    }

    //
    // Then check that they have the same types
    // The two loops here are broken out so that we don't have to keep grabbing the same column for input_table[0]
    //

    // get the first table
    const TupleSchema *table0Schema = node->getInputTables()[0]->schema();
    // iterate over all columns in the first table
    for (int col_ctr = 0, col_cnt = table0Schema->columnCount(); col_ctr < col_cnt; col_ctr++) {
        // get the type for the current column
        ValueType type0 = table0Schema->columnType(col_ctr);

        // iterate through all the other tables, comparing one column at a time
        for (int table_ctr = 1, table_cnt = (int)node->getInputTables().size(); table_ctr < table_cnt; table_ctr++) {
            // get another table
            const TupleSchema *table1Schema = node->getInputTables()[table_ctr]->schema();
            ValueType type1 = table1Schema->columnType(col_ctr);
            if (type0 != type1) {
                // TODO: DEBUG
                VOLT_ERROR("Table '%s' has value type '%s' for column '%d',"
                           " table '%s' has value type '%s' for column '%d'",
                           node->getInputTables()[0]->name().c_str(),
                           getTypeName(type0).c_str(),
                           col_ctr,
                           node->getInputTables()[table_ctr]->name().c_str(),
                           getTypeName(type1).c_str(), col_ctr);
                return false;
            }
        }
    }
    //
    // Create our output table that will hold all the tuples that we are appending into.
    // Since we're are assuming that all of the tables have the same number of columns with
    // the same format. Therefore, we will just grab the first table in the list
    //
    node->setOutputTable(TableFactory::getCopiedTempTable(node->databaseId(),
                                                          node->getInputTables()[0]->name(),
                                                          node->getInputTables()[0],
                                                          limits));

    m_setOperator = detail::SetOperator::getSetOperator(node);
    return true;
}

bool UnionExecutor::p_execute(const NValueArray &params) {
    return m_setOperator->processTuples();
}

}
