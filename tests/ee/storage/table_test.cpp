/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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

#include <cstdlib>
#include <ctime>
#include "harness.h"
#include "common/common.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/debuglog.h"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/tableutil.h"

using std::string;
using std::vector;
using namespace voltdb;

#define NUM_OF_COLUMNS 5
#define NUM_OF_TUPLES 10000

voltdb::ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { voltdb::VALUE_TYPE_BIGINT,
                                                    voltdb::VALUE_TYPE_TINYINT,
                                                    voltdb::VALUE_TYPE_SMALLINT,
                                                    voltdb::VALUE_TYPE_INTEGER,
                                                    voltdb::VALUE_TYPE_BIGINT };
int32_t COLUMN_SIZES[NUM_OF_COLUMNS]                = {
                           NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT),
                           NValue::getTupleStorageSize(voltdb::VALUE_TYPE_TINYINT),
                           NValue::getTupleStorageSize(voltdb::VALUE_TYPE_SMALLINT),
                           NValue::getTupleStorageSize(voltdb::VALUE_TYPE_INTEGER),
                           NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT) };
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]         = { true, true, true, true, true };

class TableTest : public Test {
    public:
        TableTest() : table(NULL), temp_table(NULL), persistent_table(NULL) {
            srand(0);
            init(false); // default is temp_table. call init(true) to make it transactional
        }
        ~TableTest() {
            delete table;
        }

    protected:
        void init(bool xact) {
            voltdb::CatalogId database_id = 1000;
            std::vector<boost::shared_ptr<const voltdb::TableColumn> > columns;
            char buffer[32];

            std::string *columnNames = new std::string[NUM_OF_COLUMNS];
            std::vector<voltdb::ValueType> columnTypes;
            std::vector<int32_t> columnLengths;
            std::vector<bool> columnAllowNull;
            for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
                snprintf(buffer, 32, "column%02d", ctr);
                columnNames[ctr] = buffer;
                columnTypes.push_back(COLUMN_TYPES[ctr]);
                columnLengths.push_back(COLUMN_SIZES[ctr]);
                columnAllowNull.push_back(COLUMN_ALLOW_NULLS[ctr]);
            }
            voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchema(columnTypes, columnLengths, columnAllowNull, true);
            if (xact) {
                persistent_table = voltdb::TableFactory::getPersistentTable(database_id, NULL, "test_table", schema, columnNames, -1, false, false);
                table = persistent_table;
            } else {
                temp_table = voltdb::TableFactory::getTempTable(database_id, "test_table", schema, columnNames, NULL);
                table = temp_table;
            }
            assert(tableutil::addRandomTuples(this->table, NUM_OF_TUPLES));

            // clean up
            delete[] columnNames;
        }

        voltdb::Table* table;
        voltdb::Table* temp_table;
        voltdb::Table* persistent_table;
};

TEST_F(TableTest, ValueTypes) {
    //
    // Make sure that our table has the right types and that when
    // we pull out values from a tuple that it has the right type too
    //
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table->schema());
    while (iterator.next(tuple)) {
        for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
            EXPECT_EQ(COLUMN_TYPES[ctr], this->table->schema()->columnType(ctr));
            EXPECT_EQ(COLUMN_TYPES[ctr], tuple.getType(ctr));
        }
    }
}

TEST_F(TableTest, TupleInsert) {
    //
    // All of the values have already been inserted, we just
    // need to make sure that the data makes sense
    //
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table->schema());
    while (iterator.next(tuple)) {
        //printf("%s\n", tuple->debug(this->table).c_str());
        //
        // Make sure it is not deleted
        //
        EXPECT_EQ(true, tuple.isActive());
    }

    //
    // Make sure that if we insert one tuple, we only get one tuple
    //
    voltdb::TableTuple &temp_tuple = this->table->tempTuple();
    ASSERT_EQ(true, tableutil::setRandomTupleValues(this->table, &temp_tuple));
    this->table->deleteAllTuples(true);
    ASSERT_EQ(0, this->table->activeTupleCount());
    ASSERT_EQ(true, this->table->insertTuple(temp_tuple));
    ASSERT_EQ(1, this->table->activeTupleCount());

    //
    // Then check to make sure that it has the same value and type
    //
    iterator = this->table->tableIterator();
    ASSERT_EQ(true, iterator.next(tuple));
    for (int col_ctr = 0, col_cnt = NUM_OF_COLUMNS; col_ctr < col_cnt; col_ctr++) {
        EXPECT_EQ(COLUMN_TYPES[col_ctr], tuple.getType(col_ctr));
        EXPECT_TRUE(temp_tuple.getNValue(col_ctr).op_equals(tuple.getNValue(col_ctr)).isTrue());
    }
}

TEST_F(TableTest, TupleUpdate) {
    //
    // Loop through and randomly update values
    // We will keep track of multiple columns to make sure our updates
    // are properly applied to the tuples. We will test two things:
    //
    //      (1) Updating a tuple sets the values correctly
    //      (2) Updating a tuple without changing the values doesn't do anything
    //

    std::vector<int64_t> totals;
    std::vector<int64_t> totalsNotSlim;
    totals.reserve(NUM_OF_COLUMNS);
    totalsNotSlim.reserve(NUM_OF_COLUMNS);
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        totals[col_ctr] = 0;
        totalsNotSlim[col_ctr] = 0;
    }

    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table->schema());
    while (iterator.next(tuple)) {
        bool update = (rand() % 2 == 0);
        voltdb::TableTuple &temp_tuple = table->tempTuple();
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    voltdb::NValue new_value = getRandomValue(COLUMN_TYPES[col_ctr]);
                    temp_tuple.setNValue(col_ctr, new_value);
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(new_value);
                } else {
                    totals[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                    totalsNotSlim[col_ctr] += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
                }
            }
        }
        if (update) EXPECT_EQ(true, temp_table->updateTuple(temp_tuple, tuple, true));
    }

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = this->table->tableIterator();
            while (iterator.next(tuple)) {
                new_total += ValuePeeker::peekAsBigInt(tuple.getNValue(col_ctr));
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", this->table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
            EXPECT_EQ(totals[col_ctr], new_total);
            EXPECT_EQ(totalsNotSlim[col_ctr], new_total);
        }
    }

}
/* deleteTuple in TempTable is not supported for performance reason.
TEST_F(TableTest, TupleDelete) {
    //
    // We are just going to delete all of the odd tuples, then make
    // sure they don't exist anymore
    //
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple tuple(table.get());
    while (iterator.next(tuple)) {
        if (tuple.get(1).getBigInt() != 0) {
            EXPECT_EQ(true, temp_table->deleteTuple(tuple));
        }
    }

    iterator = this->table->tableIterator();
    while (iterator.next(tuple)) {
        EXPECT_EQ(false, tuple.get(1).getBigInt() != 0);
    }
}
*/
/*TEST_F(TableTest, TupleInsertXact) {
    this->init(true);
    //
    // First clear out our table
    //
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple *tuple;
    while ((tuple = iterator.next()) != NULL) {
        EXPECT_EQ(true, persistent_table->deleteTuple(tuple));
    }

    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;
    std::vector<boost::shared_ptr<voltdb::UndoLog> > undos;
    for (int xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        voltdb::TransactionId xact_id = xact_ctr;
        undos.push_back(boost::shared_ptr<voltdb::UndoLog>(new voltdb::UndoLog(xact_id)));
    }

    int64_t total = 0;
    for (int tuple_ctr = 0; tuple_ctr < NUM_OF_TUPLES; tuple_ctr++) {
        int xact_ctr2 = (rand() % xact_cnt);
        tuple = this->table->tempTuple();
        int64_t temp = rand() % 1000;
        if (xact_ctr2 % 2 == 0) total += temp;
        voltdb::Value value = temp;
        tuple->set(0, value);
        //persistent_table->setUndoLog(undos[xact_ctr2]);
        EXPECT_EQ(true, persistent_table->insertTuple(tuple));
    }

    for (xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        if (xact_ctr % 2 == 0) {
            undos[xact_ctr]->commit();
        } else {
            undos[xact_ctr]->rollback();
        }
    }

    //
    // Now make sure all of the values add up to our total
    //
    int64_t new_total = 0;
    iterator = this->table->tableIterator();
    while ((tuple = iterator.next()) != NULL) {
        EXPECT_EQ(true, tuple->isActive());
        new_total += tuple->get(0).getBigInt();
    }

    EXPECT_EQ(total, new_total);
}*/


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
    voltdb::TableTuple *tuple;
    voltdb::TableTuple *temp_tuple;

    std::vector<int64_t> totals;
    totals.reserve(NUM_OF_COLUMNS);
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        totals[col_ctr] = 0;
    }

    //
    // Interweave the transactions. Only keep the total
    //
    //int xact_ctr;
    int xact_cnt = 6;
    sstd::vector<boost::shared_ptr<voltdb::UndoLog> > undos;
    for (int xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        voltdb::TransactionId xact_id = xact_ctr;
        undos.push_back(boost::shared_ptr<voltdb::UndoLog>(new voltdb::UndoLog(xact_id)));
    }

    voltdb::TableIterator iterator = this->table->tableIterator();
    while ((tuple = iterator.next()) != NULL) {
        //printf("BEFORE: %s\n", tuple->debug(this->table.get()).c_str());
        int xact_ctr = (rand() % xact_cnt);
        bool update = (rand() % 3 != 0);
        //printf("xact_ctr:%d\n", xact_ctr);
        //if (update) printf("update!\n");
        temp_tuple = table->tempTuple(tuple);
        for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
            //
            // Only check for numeric columns
            //
            if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
                //
                // Update Column
                //
                if (update) {
                    voltdb::Value new_value = valueutil::getRandomValue(COLUMN_TYPES[col_ctr]);
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
            //printf("BEFORE?: %s\n", tuple->debug(this->table.get()).c_str());
            //persistent_table->setUndoLog(undos[xact_ctr]);
            EXPECT_EQ(true, persistent_table->updateTuple(temp_tuple, tuple, true));
            //printf("UNDO: %s\n", undos[xact_ctr]->debug().c_str());
        }
        //printf("AFTER: %s\n", temp_tuple->debug(this->table.get()).c_str());
    }

    for (xact_ctr = 0; xact_ctr < xact_cnt; xact_ctr++) {
        if (xact_ctr % 2 == 0) {
            undos[xact_ctr]->commit();
        } else {
            undos[xact_ctr]->rollback();
        }
    }

    //iterator = this->table->tableIterator();
    //while ((tuple = iterator.next()) != NULL) {
    //    printf("TUPLE: %s\n", tuple->debug(this->table.get()).c_str());
    //}

    //
    // Check to make sure our column totals are correct
    //
    for (int col_ctr = 0; col_ctr < NUM_OF_COLUMNS; col_ctr++) {
        if (valueutil::isNumeric(COLUMN_TYPES[col_ctr])) {
            int64_t new_total = 0;
            iterator = this->table->tableIterator();
            while ((tuple = iterator.next()) != NULL) {
                //fprintf(stderr, "TUPLE: %s\n", tuple->debug(this->table).c_str());
                new_total += tuple->get(col_ctr).castAsBigInt();
            }
            //printf("\nCOLUMN: %s\n\tEXPECTED: %d\n\tRETURNED: %d\n", this->table->getColumn(col_ctr)->getName().c_str(), totals[col_ctr], new_total);
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
    voltdb::TableIterator iterator = this->table->tableIterator();
    voltdb::TableTuple *tuple;
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
        EXPECT_EQ(true, persistent_table->deleteTuple(tuple));
    }

    //
    // Now make sure all of the values add up to our total
    //
    int64_t new_total = 0;
    iterator = this->table->tableIterator();
    while ((tuple = iterator.next()) != NULL) {
        EXPECT_EQ(true, tuple->isActive());
        new_total += 1;//tuple->get(0).getBigInt();
        VOLT_DEBUG("total2: %d", (int)total);
    }

    //printf("TOTAL = %d\tNEW_TOTAL = %d\n", total, new_total);
    EXPECT_EQ(total, new_total);
}*/

int main() {
    return TestSuite::globalInstance()->runAll();
}

