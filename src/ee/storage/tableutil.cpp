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

#include <stdint.h>
#include <cstdlib>
#include <ctime>
#include "storage/tableutil.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"

namespace voltdb {

/**
 * A special iterator class used only for testing.  This class allows
 * utility functions getRandomTuple and the like to work.
 */
class JumpingTableIterator : public TableIterator {
    TBMapI m_end;        // Use here for easy access to end()
public:
    JumpingTableIterator(PersistentTable* table);
    int getTuplesInNextBlock();
    bool hasNextBlock();
    void nextBlock();
};

inline JumpingTableIterator::JumpingTableIterator(PersistentTable* parent)
    : TableIterator((Table*)parent, parent->m_data.begin()) , m_end(parent->m_data.end()) {
}

inline int JumpingTableIterator::getTuplesInNextBlock() {
    vassert(getBlockIterator() != m_end);
    return getBlockIterator().data()->activeTuples();
}

inline bool JumpingTableIterator::hasNextBlock() {
    return getBlockIterator() != m_end;
}

inline void JumpingTableIterator::nextBlock() {
    vassert(getBlockIterator() != m_end);
    TBPtr currentBlock = getBlockIterator().data();
    auto blockIt = getBlockIterator();
    ++blockIt;
    setBlockIterator(blockIt);
    setFoundTuples(getFoundTuples() + currentBlock->activeTuples());
}

bool tableutil::getRandomTuple(const voltdb::PersistentTable* table, voltdb::TableTuple &out) {
    voltdb::PersistentTable* table2 = const_cast<voltdb::PersistentTable*>(table);
    int cnt = (int)table->visibleTupleCount();
    if (cnt > 0) {
        int idx = rand() % cnt;
        JumpingTableIterator it(table2);
        while (it.hasNextBlock() && it.getTuplesInNextBlock() <= idx) {
            idx -= it.getTuplesInNextBlock();
            it.nextBlock();
        }
        while (it.next(out)) {
            if (idx-- == 0) {
                return true;
            }
        }
        throwFatalException("Unable to retrieve a random tuple."
                "Iterated entire table below active tuple count but ran out of tuples");
    }
    return false;
}

bool tableutil::getLastTuple(const voltdb::PersistentTable* table, voltdb::TableTuple &out) {
    voltdb::PersistentTable* table2 = const_cast<voltdb::PersistentTable*>(table);
    int cnt = (int)table->visibleTupleCount();
    if (cnt > 0) {
        int idx = cnt-1;
        JumpingTableIterator it(table2);
        while (it.hasNextBlock() && it.getTuplesInNextBlock() <= idx) {
            idx -= it.getTuplesInNextBlock();
            it.nextBlock();
        }
        while (it.next(out)) {
            if (idx-- == 0) {
                __attribute__((unused)) voltdb::TableTuple tmp;
                vassert(! it.next(tmp));
                return true;
            }
        }
        throwFatalException("Unable to retrieve a random tuple."
                "Iterated entire table below active tuple count but ran out of tuples");
    }
    return false;
}

void tableutil::setRandomTupleValues(Table* table, TableTuple *tuple) {
    vassert(table);
    vassert(tuple);
    for (int col_ctr = 0, col_cnt = table->columnCount(); col_ctr < col_cnt; col_ctr++) {
        const TupleSchema::ColumnInfo *columnInfo = table->schema()->getColumnInfo(col_ctr);
        NValue value = ValueFactory::getRandomValue(columnInfo->getVoltType(), columnInfo->length);

        tuple->setNValue(col_ctr, value);

        /*
         * getRandomValue() does an allocation for all strings it generates and those need to be freed
         * if the pointer wasn't transferred into the tuple.
         * The pointer won't be transferred into the tuple if the schema has that column inlined.
         */
        const TupleSchema::ColumnInfo *tupleColumnInfo = tuple->getSchema()->getColumnInfo(col_ctr);

        const ValueType t = tupleColumnInfo->getVoltType();
        if ((t == ValueType::tVARCHAR || t == ValueType::tVARBINARY) &&
                tupleColumnInfo->inlined) {
            value.free();
        }
    }
}

bool tableutil::addRandomTuples(Table* table, int num_of_tuples) {
    vassert(num_of_tuples >= 0);
    for (int ctr = 0; ctr < num_of_tuples; ctr++) {
        TableTuple &tuple = table->tempTuple();
        setRandomTupleValues(table, &tuple);
        // std::cout << std::endl << "Creating tuple" << std::endl
        //           << tuple.debug(table->name()) << std::endl;
        // VOLT_DEBUG("  Created random tuple: %s\n", tuple.debug(table->name()).c_str());
        if ( ! table->insertTuple(tuple)) {
            return false;
        }

        /*
         * The insert into the table (assuming a persistent table) will make a copy of the strings
         * so the string allocations for uninlined columns need to be freed here.
         */
        tuple.freeObjectColumns();
    }
    return true;
}

bool tableutil::addDuplicateRandomTuples(Table* table, int num_of_tuples) {
    vassert(num_of_tuples > 1);
    TableTuple &tuple = table->tempTuple();
    setRandomTupleValues(table, &tuple);
    for (int ctr = 0; ctr < num_of_tuples; ctr++) {
        //std::cout << std::endl << "Creating tuple " << std::endl << tuple.debug(table->name()) << std::endl;
        //VOLT_DEBUG("Created random tuple: %s", tuple.debug(table->name()).c_str());
        if ( ! table->insertTuple(tuple)) {
            return false;
        }
    }

    /*
     * The insert into the table (assuming a persistent table) will make a copy of the strings
     * so the string allocations for uninlined columns need to be freed here.
     */
    tuple.freeObjectColumns();
    return true;
}

}
