/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

#include <stdint.h>
#include <cstdlib>
#include <ctime>
#include "storage/tableutil.h"
#include "common/common.h"
#include "common/ValueFactory.hpp"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "storage/table.h"
#include "storage/tableiterator.h"

namespace tableutil {

bool getRandomTuple(const voltdb::Table* table, voltdb::TableTuple &out) {
    voltdb::Table* table2 = const_cast<voltdb::Table*>(table);
    int cnt = (int)table->usedTupleCount();
    if (cnt > 0) {
        int idx = (rand() % cnt);
        voltdb::TableIterator it = table2->iterator();
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

bool setRandomTupleValues(voltdb::Table* table, voltdb::TableTuple *tuple) {
    assert(table);
    assert(tuple);
    for (int col_ctr = 0, col_cnt = table->columnCount(); col_ctr < col_cnt; col_ctr++) {
        voltdb::NValue value = voltdb::getRandomValue(table->schema()->columnType(col_ctr));
        tuple->setNValue(col_ctr, value);

        /*
         * getRandomValue() does an allocation for all strings it generates and those need to be freed
         * if the pointer wasn't transferred into the tuple by setSlimValue(). The pointer won't be transferred into
         * the tuple if the schema has that column inlined.
         */
        voltdb::ValueType t = tuple->getSchema()->columnType(col_ctr);
        if (((t == voltdb::VALUE_TYPE_VARCHAR) || (t == voltdb::VALUE_TYPE_VARBINARY)) &&
                tuple->getSchema()->columnIsInlined(col_ctr)) {
            value.free();
        }
    }
    return (true);
}

bool addRandomTuples(voltdb::Table* table, int num_of_tuples) {
    assert(num_of_tuples >= 0);
    for (int ctr = 0; ctr < num_of_tuples; ctr++) {
        voltdb::TableTuple &tuple = table->tempTuple();
        if (!tableutil::setRandomTupleValues(table, &tuple)) {
            return (false);
        }
        //std::cout << std::endl << "Creating tuple " << std::endl << tuple.debugNoHeader() << std::endl;
        //VOLT_DEBUG("Created random tuple: %s", tuple.debug().c_str());
        if (!table->insertTuple(tuple)) {
            return (false);
        }

        /*
         * The insert into the table (assuming a persistent table) will make a copy of the strings so the string allocations
         * for unlined columns need to be freed here.
         */
        for (int ii = 0; ii < tuple.getSchema()->getUninlinedObjectColumnCount(); ii++) {
            tuple.getNValue(tuple.getSchema()->getUninlinedObjectColumnInfoIndex(ii)).free();
        }
    }
    return (true);
}

bool equals(const voltdb::Table* table, voltdb::TableTuple *tuple0, voltdb::TableTuple *tuple1) {
    assert(table);
    assert(tuple0);
    assert(tuple1);
    return tuple0->equals(*tuple1);
}


bool copy(const voltdb::Table *from_table, voltdb::Table* to_table) {
    voltdb::Table* fromtable2 = const_cast<voltdb::Table*>(from_table);
    assert(from_table->columnCount() == to_table->columnCount());
    voltdb::TableIterator iterator = fromtable2->iterator();
    voltdb::TableTuple tuple(fromtable2->schema());
    while (iterator.next(tuple)) {
        if (!to_table->insertTuple(tuple)) {
            return (false);
        }
    }
    return (true);
}

bool getTupleAt(const voltdb::Table* table, int64_t position, voltdb::TableTuple &out) {
    assert(table);
    voltdb::Table* table2 = const_cast<voltdb::Table*>(table);
    voltdb::TableIterator iterator = table2->iterator();
    int64_t ctr = 0;
    while (iterator.next(out)) {
        if (ctr++ == position) {
            return true;
        }
    }
    return false;
}

}
