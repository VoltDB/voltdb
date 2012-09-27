/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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
        typedef boost::unordered_multiset<TableTuple, TableTupleHasher, TableTupleEqualityChecker>
            TupleMultiSet;
        typedef boost::unordered_map<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
            TupleMap;
        typedef boost::unordered_multimap<TableTuple, size_t, TableTupleHasher, TableTupleEqualityChecker>
            TupleMultiMap;

        SetOperator(std::vector<Table*>& input_tables, Table* output_table) :
            m_input_tables(input_tables), m_output_table(output_table)
            {}

        virtual bool processTuples() = 0;

        static boost::shared_ptr<SetOperator> getSetOperator(UnionPlanNode* node);

        std::vector<Table*>& m_input_tables;
        Table* m_output_table;
    };

    template <typename SetType>
    struct UnionSetOperator : public SetOperator {
        UnionSetOperator(std::vector<Table*>& input_tables, Table* output_table) :
           SetOperator(input_tables, output_table), m_tuples()
           {}

        bool processTuples() {
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
                    if (this->needToInsert(tuple)) {
                        // we got tuple to insert
                        m_tuples.insert(tuple);
                        if (!m_output_table->insertTuple(tuple)) {
                            VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                                       " output table '%s'",
                                       input_table->name().c_str(),
                                       m_output_table->name().c_str());
                            return false;
                        }
                    }
                }
            // FIXME: node->tables[ctr]->onTableRead(undo);
            }
            return true;
        }

        SetType m_tuples;

        private:
            bool needToInsert(const TableTuple& tuple);
    };

    // UNION Specialization
    template<>
    inline
    bool UnionSetOperator<SetOperator::TupleSet>::needToInsert(const TableTuple& tuple) {
        return this->m_tuples.find(tuple) == this->m_tuples.end();
    }

    // UNION ALL Specialization
    template<>
    inline
    bool UnionSetOperator<SetOperator::TupleMultiSet>::needToInsert(const TableTuple& tuple) {
        return true;
    }

    template <typename SetType>
    struct ExceptSetOperator : public SetOperator {
        ExceptSetOperator(std::vector<Table*>& input_tables, Table* output_table) :
           SetOperator(input_tables, output_table)
           {}

        bool processTuples() {
            // Collect all tuples from the first set
            assert(!m_input_tables.empty());
            Table* input_table = m_input_tables[0];
            TableIterator iterator = input_table->iterator();
            TableTuple tuple(input_table->schema());
            while (iterator.next(tuple)) {
                m_tuples.insert(tuple);
            }

            //
            // For each remaining input table, grab their TableIterator and
            // then substract all of its tuples from the initial table
            //
            for (size_t ctr = 1, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
                Table* input_table = m_input_tables[ctr];
                assert(input_table);
                TableIterator iterator = input_table->iterator();
                TableTuple tuple(input_table->schema());
                while (iterator.next(tuple)) {
                    this->erase(tuple);
                }
            }

            // Insert remaining tuples to our ouput table
            for (typename SetType::iterator tupleIt = m_tuples.begin(); tupleIt != m_tuples.end(); ++tupleIt) {
                // Unfortunately, need to make an extra copy.
                // insertTuple expects TableTuple&, but set iterator produces a const verrsion.
                TableTuple tuple = *tupleIt;
                if (!m_output_table->insertTuple(tuple)) {
                    VOLT_ERROR("Failed to insert tuple from input table '%s' into"
                               " output table '%s'",
                               m_input_tables[0]->name().c_str(),
                               m_output_table->name().c_str());
                    return false;
                }
            }
            return true;
        }

        SetType m_tuples;

        private:
            void erase(const TableTuple& tuple);
    };

    // EXCEPT Specialization
    template<>
    inline
    void ExceptSetOperator<SetOperator::TupleSet>::erase(const TableTuple& tuple) {
        SetOperator::TupleSet::iterator tupleIt = m_tuples.find(tuple);
        if (tupleIt != m_tuples.end()) {
            this->m_tuples.erase(tupleIt);
        }
    }
    // EXCEPT ALL Specialization
    template<>
    inline
    void ExceptSetOperator<SetOperator::TupleMultiSet>::erase(const TableTuple& tuple) {
        std::pair<SetOperator::TupleMultiSet::iterator, SetOperator::TupleMultiSet::iterator>
            range = m_tuples.equal_range(tuple);
        if (range.first != range.second) {
            this->m_tuples.erase(range.first, range.second);
        }
    }

    struct TableSizeLess {
        bool operator()(const Table* t1, const Table* t2) const {
            return t1->activeTupleCount() < t2->activeTupleCount();
        }
    };

    template <typename MapType>
    struct IntersectSetOperator : public SetOperator {
        IntersectSetOperator(std::vector<Table*>& input_tables, Table* output_table) :
            SetOperator(input_tables, output_table) {
                // Find the smallest input table
                std::vector<Table*>::iterator minTableIt =
                    std::min_element(m_input_tables.begin(), m_input_tables.end(), TableSizeLess());
                std::swap( m_input_tables[0], *minTableIt);
            }

        bool processTuples() {
            // Collect all tuples from the smallest set
            assert(!m_input_tables.empty());
            Table* input_table = m_input_tables[0];
            TableIterator iterator = input_table->iterator();
            TableTuple tuple(input_table->schema());
            while (iterator.next(tuple)) {
                m_tuples.insert(typename MapType::value_type(tuple, 1));
            }

            //
            // For each remaining input table, grab their TableIterator and
            // then increment each tuple's count in the map
            //
            for (size_t ctr = 1, cnt = m_input_tables.size(); ctr < cnt; ctr++) {
                Table* input_table = m_input_tables[ctr];
                assert(input_table);
                TableIterator iterator = input_table->iterator();
                TableTuple tuple(input_table->schema());
                while (iterator.next(tuple)) {
                    this->increment(tuple, ctr);
                }
            }

            // Iterate over the collected tuples and insert only ones that
            // have associated count equal to the number of the input tables
            size_t count = m_input_tables.size();
            for (typename MapType::iterator tupleIt = m_tuples.begin(); tupleIt != m_tuples.end(); ++tupleIt) {
                if (tupleIt->second == count) {
                    TableTuple tuple = tupleIt->first;
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

        // Map to keep candidate tuples. The key is the tuple itself
        // The value - number of input tables this tuple is in.
        MapType m_tuples;

        private:
            void increment(const TableTuple& tuple, size_t current_cnt);

    };

    // INTERSECT Specialization
    template<>
    inline
    void IntersectSetOperator<SetOperator::TupleMap>::increment(const TableTuple& tuple, size_t) {
        // Find the tuple
        SetOperator::TupleMap::iterator tupleIt = m_tuples.find(tuple);
        // If exist simply increment count
        if (tupleIt != m_tuples.end()) {
            ++tupleIt->second;
        }
    }
    // INTERSECT ALL Specialization
    template<>
    inline
    void IntersectSetOperator<SetOperator::TupleMultiMap>::increment(const TableTuple& tuple, size_t current_cnt) {
        // Find all tuples
        std::pair<SetOperator::TupleMultiMap::iterator, SetOperator::TupleMultiMap::iterator>
            range = m_tuples.equal_range(tuple);
        if (range.first != range.second){
            // For a given table we need to increment the count only once even
            // the table contains multiple identical tuples
            SetOperator::TupleMultiMap::iterator insertIt = range.first;
            if (range.first->second != current_cnt + 1) {
                // this is the first time we see this tuple
                // increment count for all identical tuples;
                while (range.first != range.second) {
                    ++range.first->second;
                    ++range.first;
                }
            }
            // Insert new tuple with the appropriate count
            m_tuples.insert(insertIt, SetOperator::TupleMultiMap::value_type(tuple, current_cnt + 1));
        }
    }

    boost::shared_ptr<SetOperator> SetOperator::getSetOperator(UnionPlanNode* node) {
        UnionType unionType = node->getUnionType();
        switch (unionType) {
            case UNION_TYPE_UNION_ALL:
                return boost::shared_ptr<SetOperator>(
                new UnionSetOperator<TupleMultiSet>(node->getInputTables(), node->getOutputTable()));
            case UNION_TYPE_UNION:
                return boost::shared_ptr<SetOperator>(
                new UnionSetOperator<TupleSet>(node->getInputTables(), node->getOutputTable()));
            case UNION_TYPE_EXCEPT_ALL:
                return boost::shared_ptr<SetOperator>(
                new ExceptSetOperator<TupleMultiSet>(node->getInputTables(), node->getOutputTable()));
            case UNION_TYPE_EXCEPT:
                return boost::shared_ptr<SetOperator>(
                new ExceptSetOperator<TupleSet>(node->getInputTables(), node->getOutputTable()));
            case UNION_TYPE_INTERSECT_ALL:
                return boost::shared_ptr<SetOperator>(
                new IntersectSetOperator<TupleMultiMap>(node->getInputTables(), node->getOutputTable()));
            case UNION_TYPE_INTERSECT:
                return boost::shared_ptr<SetOperator>(
                new IntersectSetOperator<TupleMap>(node->getInputTables(), node->getOutputTable()));
            default:
                VOLT_ERROR("Unsupported tuple set operation '%d'.", unionType);
                return boost::shared_ptr<SetOperator>();
        }
    }
}

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
