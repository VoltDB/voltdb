/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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

#include "harness.h"

#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/ThreadLocalPool.h"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"
#include "storage/temptable.h"

#include <cstdlib>
#include <ctime>
#include <string>

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 9
#define NUM_OF_TUPLES 5000

ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = {
    ValueType::tTINYINT,
    ValueType::tSMALLINT,
    ValueType::tINTEGER,
    ValueType::tBIGINT,
    ValueType::tDECIMAL,
    ValueType::tDOUBLE,
    ValueType::tTIMESTAMP,
    ValueType::tVARCHAR,
    ValueType::tVARBINARY
};

int32_t COLUMN_SIZES[NUM_OF_COLUMNS] =
    {
        NValue::getTupleStorageSize(ValueType::tTINYINT),    // 1
        NValue::getTupleStorageSize(ValueType::tSMALLINT),   // 2
        NValue::getTupleStorageSize(ValueType::tINTEGER),    // 4
        NValue::getTupleStorageSize(ValueType::tBIGINT),     // 8
        NValue::getTupleStorageSize(ValueType::tDECIMAL),    // 16
        NValue::getTupleStorageSize(ValueType::tDOUBLE),     // 8
        NValue::getTupleStorageSize(ValueType::tTIMESTAMP),  // 8
        10,    /* The test uses getRandomValue() to generate random value,
                  make sure the column size not conflict with the value it generates. */
        16     /* same as above */
    };
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS] = { true, true, true, true, true, true, true, true, true };

class TableTest : public Test {
public:
    TableTest() : m_table(NULL), temp_table(NULL), persistent_table(NULL), limits(1024 * 1024)
    {
        srand(0);
        init(false); // default is temp_table. call init(true) to make it transactional
    }
    ~TableTest()
    {
        delete m_table;
    }

protected:
    void init(bool xact) {
        CatalogId database_id = 1000;
        char buffer[32];

        vector<string> columnNames(NUM_OF_COLUMNS);
        vector<ValueType> columnTypes;
        vector<int32_t> columnLengths;
        vector<bool> columnAllowNull;
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            snprintf(buffer, 32, "column%02d", ctr);
            columnNames[ctr] = buffer;
            columnTypes.push_back(COLUMN_TYPES[ctr]);
            columnLengths.push_back(COLUMN_SIZES[ctr]);
            columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
        }
        TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes, columnLengths, columnAllowNull);
        if (xact) {
            persistent_table = TableFactory::getPersistentTable(database_id, "test_table", schema, columnNames, signature);
            m_table = persistent_table;
        } else {
            temp_table = TableFactory::buildTempTable("test_temp_table", schema, columnNames, &limits);
            m_table = temp_table;
        }

        bool addTuples = tableutil::addRandomTuples(m_table, NUM_OF_TUPLES);
        if(!addTuples) {
            assert(!"Failed adding random tuples");
        }
    }

    Table* m_table;
    Table* temp_table;
    Table* persistent_table;
    TempTableLimits limits;
    char signature[20];
};

TEST_F(TableTest, ValueTypes) {
    //
    // Make sure that our table has the right types and that when
    // we pull out values from a tuple that it has the right type too
    //
    TableIterator iterator = m_table->iterator();
    TableTuple tuple(m_table->schema());
    while (iterator.next(tuple)) {
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            EXPECT_EQ(COLUMN_TYPES[ctr], m_table->schema()->columnType(ctr));

            const TupleSchema::ColumnInfo *columnInfo = tuple.getSchema()->getColumnInfo(ctr);
            EXPECT_EQ(COLUMN_TYPES[ctr], columnInfo->getVoltType());
        }
    }
}

TEST_F(TableTest, TableSerialize) {
    // Add sizeof(int32_t) to cover table size prefix
    size_t serializeSize = m_table->getAccurateSizeToSerialize() + sizeof(int32_t);
    char* backingCharArray = new char[serializeSize];
    ReferenceSerializeOutput conflictSerializeOutput(backingCharArray, serializeSize);
    m_table->serializeTo(conflictSerializeOutput);

    EXPECT_EQ(serializeSize, conflictSerializeOutput.size());

    delete[] backingCharArray;
}

TEST_F(TableTest, TableSerializeWithoutTotalSize) {
    size_t serializeSize = m_table->getAccurateSizeToSerialize();
    char* backingCharArray = new char[serializeSize];
    ReferenceSerializeOutput conflictSerializeOutput(backingCharArray, serializeSize);
    m_table->serializeToWithoutTotalSize(conflictSerializeOutput);

    EXPECT_EQ(serializeSize, conflictSerializeOutput.size());

    delete[] backingCharArray;
}

TEST_F(TableTest, TupleInsert) {
    //
    // All of the values have already been inserted, we just
    // need to make sure that the data makes sense
    //
    TableIterator iterator = m_table->iterator();
    TableTuple tuple(m_table->schema());
    while (iterator.next(tuple)) {
        //printf("%s\n", tuple->debug(m_table).c_str());
        //
        // Make sure it is not deleted
        //
        EXPECT_TRUE(tuple.isActive());
    }

    //
    // Make sure that if we insert one tuple, we only get one tuple
    //
    TableTuple &temp_tuple = m_table->tempTuple();
    tableutil::setRandomTupleValues(m_table, &temp_tuple);
    m_table->deleteAllTuples();
    ASSERT_EQ(0, m_table->activeTupleCount());
    ASSERT_TRUE(m_table->insertTuple(temp_tuple));
    ASSERT_EQ(1, m_table->activeTupleCount());

    //
    // Then check to make sure that it has the same value and type
    //
    iterator = m_table->iterator();
    ASSERT_TRUE(iterator.next(tuple));
    for (int col_ctr = 0, col_cnt = NUM_OF_COLUMNS; col_ctr < col_cnt; col_ctr++) {
        const TupleSchema::ColumnInfo *columnInfo = tuple.getSchema()->getColumnInfo(col_ctr);
        EXPECT_EQ(COLUMN_TYPES[col_ctr], columnInfo->getVoltType());
        EXPECT_TRUE(temp_tuple.getNValue(col_ctr).op_equals(tuple.getNValue(col_ctr)).isTrue());
    }
}

/* updateTuple in TempTable is not supported because it is not required in the product.
TEST_F(TableTest, TupleUpdate) {
    //
    // Loop through and randomly update values
    // We will keep track of multiple columns to make sure our updates
    // are properly applied to the tuples. We will test two things:
    //
    //      (1) Updating a tuple sets the values correctly
    //      (2) Updating a tuple without changing the values doesn't do anything
    //

    vector<int64_t> totals(NUM_OF_COLUMNS, 0);
    vector<int64_t> totalsNotSlim(NUM_OF_COLUMNS, 0);

    TableIterator iterator = m_table->iterator();
    TableTuple tuple(m_table->schema());
    while (iterator.next(tuple)) {
        bool update = (rand() % 2 == 0);
        TableTuple &temp_tuple = m_table->tempTuple();
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    NValue new_value = getRandomValue(COLUMN_TYPES[col_ctr]);
                    temp_tuple.setNValue(col_ctr, new_value);
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                } else {
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                }
            }
        }
        if (update) {
            EXPECT_TRUE(temp_table->updateTuple(tuple, temp_tuple));
        }
    }

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = m_table->iterator();
            while (iterator.next(tuple)) {
                new_total += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", m_table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
            EXPECT_EQ(totals[col_ctr], new_total);
            EXPECT_EQ(totalsNotSlim[col_ctr], new_total);
        }
    }
}
*/

// I can't for the life of me make this pass using Valgrind.  I
// suspect that there's an extra reference to the ThreadLocalPool
// which isn't getting deleted, but I can't find it.  Leaving this
// here for now, feel free to fix or delete if you're offended.
// --izzy 7/8/2011
//
// TEST_F(TableTest, TempTableBoom)
// {
//     init(false);
//     bool threw = false;
//     try
//     {
//         while (true)
//         {
//             TableTuple &tuple = m_table->tempTuple();
//             tableutil::setRandomTupleValues(m_table, &tuple);
//             if (!table->insertTuple(tuple)) {
//                 EXPECT_TRUE(false);
//             }
//
//             /*
//              * The insert into the table (assuming a persistent table)
//              * will make a copy of the strings so the string
//              * allocations for unlined columns need to be freed here.
//              */
//             tuple.freeObjectColumns();
//         }
//     }
//     catch (SQLException& e)
//     {
//         TableTuple &tuple = m_table->tempTuple();
//         tuple.freeObjectColumns();
//         EXPECT_GT(limits.getAllocated(), 1024 * 1024);
//         string state(e.getSqlState());
//         if (state == "V0002")
//         {
//             threw = true;
//         }
//     }
//     EXPECT_TRUE(threw);
// }

/* deleteTuple in TempTable is not supported for performance reason.
TEST_F(TableTest, TupleDelete) {
    //
    // We are just going to delete all of the odd tuples, then make
    // sure they don't exist anymore
    //
    TableIterator iterator = m_table->iterator();
    TableTuple tuple(m_table.get());
    while (iterator.next(tuple)) {
        if (tuple.get(1).getBigInt() != 0) {
            temp_table->deleteTuple(tuple);
        }
    }

    iterator = m_table->iterator();
    while (iterator.next(tuple)) {
        EXPECT_EQ(false, tuple.get(1).getBigInt() != 0);
    }
}
*/

/*TEST_F(TableTest, TupleUpdateXact) {
    this->init(true);
    //
    // Loop through and randomly update values
    // We will keep track of multiple columns to make sure our updates
    // are properly applied to the tuples. We will test two things:
    //
    //      (1) Updating a tuple sets the values correctly
    //      (2) Updating a tuple without changing the values doesn't do anything
    //
    TableTuple *tuple;
    TableTuple *temp_tuple;

    vector<int64_t> totals(NUM_OF_COLUMNS, 0);

    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;
    vector<boost::shared_ptr<UndoLog> > undos;
    for (int xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        TransactionId xact_id = xact_ctr;
        undos.push_back(boost::shared_ptr<UndoLog>(new UndoLog(xact_id)));
    }

    TableIterator iterator = m_table->iterator();
    while ((tuple = iterator.next()) != NULL) {
        //printf("BEFORE: %s\n", tuple->debug(m_table.get()).c_str());
        int xact_ctr = (rand() % xact_cnt);
        bool update = (rand() % 3 != 0);
        //printf("xact_ctr:%d\n", xact_ctr);
        //if (update) printf("update!\n");
        temp_tuple = m_table->tempTuple(tuple);
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    Value new_value = valueutil::getRandomValue(COLUMN_TYPES[col_ctr]);
                    temp_tuple->set(col_ctr, new_value);

                    //
                    // We make a distinction between the updates that we will
                    // commit and those that we will rollback
                    //
                    totals[col_ctr] += xact_ctr % 2 == 0 ? new_value.castAsBigInt() : tuple->get(col_ctr).castAsBigInt();
                } else {
                    totals[col_ctr] += tuple->get(col_ctr).castAsBigInt();
                }
            }
        }
        if (update) {
            //printf("BEFORE?: %s\n", tuple->debug(m_table.get()).c_str());
            //persistent_table->setUndoLog(undos[xact_ctr]);
            EXPECT_TRUE(persistent_table->updateTuple(tuple, temp_tuple, true));
            //printf("UNDO: %s\n", undos[xact_ctr]->debug().c_str());
        }
        //printf("AFTER: %s\n", temp_tuple->debug(m_table.get()).c_str());
    }

    for (xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        if (xact_ctr % 2 == 0) {
            undos[xact_ctr]->commit();
        } else {
            undos[xact_ctr]->rollback();
        }
    }

    //iterator = m_table->iterator();
    //while ((tuple = iterator.next()) != NULL) {
    //    printf("TUPLE: %s\n", tuple->debug(m_table.get()).c_str());
    //}

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = m_table->iterator();
            while ((tuple = iterator.next()) != NULL) {
                //fprintf(stderr, "TUPLE: %s\n", tuple->debug(m_table).c_str());
                new_total += tuple->get(col_ctr).castAsBigInt();
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", m_table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
            EXPECT_EQ(totals[col_ctr], new_total);
        }
    }
}*/

/*TEST_F(TableTest, TupleDeleteXact) {
    this->init(true);
    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;

    //
    // Loop through the tuples and delete half of them in interleaving transactions
    //
    TableIterator iterator = m_table->iterator();
    TableTuple *tuple;
    int64_t total = 0;
    while ((tuple = iterator.next()) != NULL) {
        int xact_ctr = (rand() % xact_cnt);
        //
        // Keep it and store the value before deleting
        // NOTE: Since we are testing whether the deletes work, we only
        //       want to store the values for the tuples where the delete is
        //       going to get rolled back!
        //
        if (xact_ctr % 2 != 0) total += 1;//tuple->get(0).castAsBigInt();
        VOLT_DEBUG("total: %d", (int)total);
        //persistent_table->setUndoLog(undos[xact_ctr]);
        persistent_table->deleteTuple(tuple);
    }

    //
    // Now make sure all of the values add up to our total
    //
    int64_t new_total = 0;
    iterator = m_table->iterator();
    while ((tuple = iterator.next()) != NULL) {
        EXPECT_TRUE(tuple->isActive());
        new_total += 1;//tuple->get(0).getBigInt();
        VOLT_DEBUG("total2: %d", (int)total);
    }

    //printf("TOTAL = %d\tNEW_TOTAL = %d\n", total, new_total);
    EXPECT_EQ(total, new_total);
}*/

int main() {
    return TestSuite::globalInstance()->runAll();
}
