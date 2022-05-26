/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"

#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValuePeeker.hpp"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"

#include <stdio.h>
#include <string>
#include <set>

namespace voltdb {

class TableTupleFilterTest : public Test
{
public:
    static const int NUM_OF_TUPLES = 50000;

    TableTupleFilterTest()
        : m_tempTable(createTempTable())
    {
        init();
    }

    TempTable* getTempTable() {
        return m_tempTable.get();
    }


private:

    TupleSchema* createTupleSchema(int columns) {
        std::vector<ValueType> all_types;
        std::vector<bool> column_allow_null(columns, true);
        std::vector<int32_t> all_inline_lengths;

        for (int i = 0 ; i < columns; ++i) {
            all_types.push_back(ValueType::tBIGINT);
            all_inline_lengths.push_back(NValue::getTupleStorageSize(ValueType::tBIGINT));
        }
        return TupleSchema::createTupleSchemaForTest(all_types,
                                       all_inline_lengths,
                                       column_allow_null);
    }

    TempTable* createTempTable() {

        std::string tableName = "a_table";
        int columns = 5;
        TupleSchema*  schema = createTupleSchema(columns);

        std::vector<std::string> names;
        for (int i = 0 ; i < columns; ++i) {
            char buffer[5];
            snprintf(buffer, 4, "C%02d", i);
            std::string name(buffer);
            names.push_back(name);
        }
        return TableFactory::buildTempTable(tableName, schema, names, NULL);
    }

    void init() {
        bool addTuples = tableutil::addRandomTuples(m_tempTable.get(), NUM_OF_TUPLES);
        if(!addTuples) {
            assert(!"Failed adding random tuples");
        }
    }

    boost::scoped_ptr<TempTable> m_tempTable;
};

TEST_F(TableTupleFilterTest, tableTupleFilterTest)
{
    static const int MARKER = 33;

    TempTable* table = getTempTable();
    TableTupleFilter tableFilter;
    tableFilter.init(table);

    int tuplePerBlock = table->getTuplesPerBlock();
    // make sure table spans more than one block
    ASSERT_TRUE(NUM_OF_TUPLES / tuplePerBlock > 1);

    TableTuple tuple = table->tempTuple();
    TableIterator iterator = table->iterator();

    // iterator over and mark every 5th tuple
    int counter = 0;
    std::multiset<int64_t> control_values;

    while(iterator.next(tuple)) {
        if (++counter % 5 == 0) {
            NValue nvalue = tuple.getNValue(1);
            int64_t value = ValuePeeker::peekBigInt(nvalue);
            control_values.insert(value);
            tableFilter.updateTuple(tuple, MARKER);
        }
    }

    TableTupleFilter_iter<MARKER> endItr = tableFilter.end<MARKER>();
    for (TableTupleFilter_iter<MARKER> itr = tableFilter.begin<MARKER>(); itr != endItr; ++itr) {
            uint64_t tupleAddr = tableFilter.getTupleAddress(*itr);
            tuple.move((char *)tupleAddr);
            ASSERT_TRUE(tuple.isActive());
            NValue nvalue = tuple.getNValue(1);
            int64_t value = ValuePeeker::peekBigInt(nvalue);

            ASSERT_FALSE(control_values.empty());
            auto it = control_values.find(value);
            ASSERT_NE(it, control_values.end());
            control_values.erase(it);
    }
    ASSERT_TRUE(control_values.empty());

}


} // end namespace

int main()
{
    return TestSuite::globalInstance()->runAll();
}
