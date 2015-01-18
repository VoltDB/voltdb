/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include <stdint.h>
#include "harness.h"
#include "common/common.h"
#include "common/types.h"
#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/SerializableEEException.h"
#include "common/tabletuple.h"
#include "storage/tablefactory.h"
#include "storage/persistenttable.h"
#include "storage/DRTupleStream.h"
#include "indexes/tableindex.h"
#include "execution/VoltDBEngine.h"


using std::string;
using std::vector;
using namespace voltdb;

#define NUM_OF_TUPLES 10

class ConstraintTest : public Test {
public:
    ConstraintTest() : table(NULL) {
        this->database_id = 1000;
        m_exceptionBuffer = new char[4096];
        m_engine.setBuffers( NULL, 0, NULL, 0, m_exceptionBuffer, 4096);
        m_engine.resetReusedResultOutputBuffer();
        int partitionCount = 1;
        m_engine.initialize(0, 0, 0, 0, "", DEFAULT_TEMP_TABLE_MEMORY);
        m_engine.updateHashinator( HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
    }
    ~ConstraintTest() {
        delete table;
        delete [] m_exceptionBuffer;
    }

protected:
    voltdb::Table* table;
    voltdb::CatalogId database_id;
    voltdb::VoltDBEngine m_engine;
    voltdb::MockDRTupleStream drStream;
    char signature[20];

    char *m_exceptionBuffer;

    std::vector<std::string> columnNames;
    std::vector<voltdb::ValueType> columnTypes;
    std::vector<int32_t> columnSizes;
    std::vector<bool> columnNullables;

    void addColumn(const char* name, voltdb::ValueType type, uint16_t size, bool allow_null) {
        columnNames.push_back(name);
        columnTypes.push_back(type);
        columnSizes.push_back(size);
        columnNullables.push_back(allow_null);
    };

    void setTable(voltdb::TableIndexScheme *pkey = NULL) {
        assert (columnNames.size() == columnTypes.size());
        assert (columnTypes.size() == columnSizes.size());
        assert (columnSizes.size() == columnNullables.size());
        TupleSchema *schema = TupleSchema::createTupleSchemaForTest(columnTypes, columnSizes, columnNullables);
        if (pkey != NULL) {
            pkey->tupleSchema = schema;
        }
        table = TableFactory::getPersistentTable(this->database_id, "test_table", schema, columnNames, signature, &drStream, false);
        if (pkey) {
            TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(*pkey);
            assert(pkeyIndex);
            table->addIndex(pkeyIndex);
            table->setPrimaryKeyIndex(pkeyIndex);
        }
    };

    void setTable(voltdb::TableIndexScheme &pkey) {
        setTable(&pkey);
    };
};

TEST_F(ConstraintTest, NotNull) {
    //
    // Set the first three columns to be not allow for null values
    //
    bool allow_null[4] = { false, false, false, true };
    for (int ctr = 0; ctr < 4; ctr++) {
        char name[16];
        snprintf(name, 16, "col%02d", ctr);
        addColumn(name, VALUE_TYPE_BIGINT,
                  NValue::getTupleStorageSize(voltdb::VALUE_TYPE_BIGINT),
                  allow_null[ctr]);
    }
    setTable();

    //
    // Insert tuples with different combinations of null values
    // Make sure that none of them get added
    //
    int64_t value = 1;
    for (int ctr0 = 0; ctr0 <= 1; ctr0++) {
        for (int ctr1 = 0; ctr1 <= 1; ctr1++) {
            for (int ctr2 = 0; ctr2 <= 1; ctr2++) {
                for (int ctr3 = 0; ctr3 <= 1; ctr3++) {
                    TableTuple &tuple = this->table->tempTuple();
                    tuple.setAllNulls();
                    if (ctr0) tuple.setNValue(0, voltdb::ValueFactory::getBigIntValue(value++));
                    if (ctr1) tuple.setNValue(1, voltdb::ValueFactory::getBigIntValue(value++));
                    if (ctr2) tuple.setNValue(2, voltdb::ValueFactory::getBigIntValue(value++));
                    if (ctr3) tuple.setNValue(3, voltdb::ValueFactory::getBigIntValue(value++));

                    bool expected = (ctr0 + ctr1 + ctr2 == 3);
                    bool threwException = false;
                    try {
                        bool returned = this->table->insertTuple(tuple);
                        EXPECT_EQ(expected, returned);
                    } catch (SerializableEEException &e) {
                        threwException = true;
                    }
                    EXPECT_EQ(expected, !threwException);
                }
            }
        }
    }
}

TEST_F(ConstraintTest, UniqueOneColumnNotNull) {
    //
    // Check to make sure that a single column pkey works
    //
    const int columnCount = 3;
    bool allow_null[columnCount] = { false, true, true };
    for (int ctr = 0; ctr < columnCount; ctr++) {
        char name[16];
        snprintf(name, 16, "col%02d", ctr);
        addColumn(name, VALUE_TYPE_BIGINT,
                  NValue::getTupleStorageSize(VALUE_TYPE_BIGINT),
                  allow_null[ctr]);
    }

    std::vector<int> pkey_column_indices;
    pkey_column_indices.push_back(0);
    TableIndexScheme pkey("idx_pkey", voltdb::BALANCED_TREE_INDEX,
                          pkey_column_indices, TableIndex::simplyIndexColumns(),
                          true, true, NULL);

    setTable(pkey);

    for (int64_t ctr = 0; ctr < NUM_OF_TUPLES; ctr++) {
        voltdb::TableTuple &tuple = this->table->tempTuple();
        tuple.setAllNulls();
        tuple.setNValue(0, ValueFactory::getBigIntValue(ctr));
        tuple.setNValue(1, ValueFactory::getBigIntValue(ctr));
        tuple.setNValue(2, ValueFactory::getBigIntValue(ctr));

        //
        // This should always succeed
        //
        EXPECT_EQ(true, this->table->insertTuple(tuple));

        bool exceptionThrown = false;
        try {
            //
            // And this should always fail
            //
            EXPECT_EQ(false, this->table->insertTuple(tuple));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_TRUE(exceptionThrown);

        exceptionThrown = false;
        try {
            //
            // Even if we change just one value that isn't the primary key, it should still fail!
            //
            tuple.setNValue(1, voltdb::ValueFactory::getBigIntValue(ctr + ctr));
            EXPECT_EQ(false, this->table->insertTuple(tuple));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_TRUE(exceptionThrown);

    }
}


TEST_F(ConstraintTest, UniqueOneColumnAllowNull) {
    //
    // Check to make sure that a single column pkey works with null
    //
    const int columnCount = 3;
    bool allow_null[columnCount] = { true, true, true };
    for (int ctr = 0; ctr < columnCount; ctr++) {
        char name[16];
        snprintf(name, 16, "col%02d", ctr);
        addColumn(name, VALUE_TYPE_BIGINT,
                  NValue::getTupleStorageSize(VALUE_TYPE_BIGINT), allow_null[ctr]);
    }

    std::vector<int> pkey_column_indices;
    pkey_column_indices.push_back(0);
    voltdb::TableIndexScheme pkey("idx_pkey", BALANCED_TREE_INDEX,
                                  pkey_column_indices, TableIndex::simplyIndexColumns(),
                                  true, true, NULL);

    setTable(pkey);

    int64_t value_ctr = 0;
    for (int ctr = 0; ctr < 2; ctr++) {
        bool expected_result = (ctr == 0);
        //
        // Insert a regular value
        //
        TableTuple &tuple = this->table->tempTuple();
        tuple.setAllNulls();
        tuple.setNValue(0, ValueFactory::getBigIntValue(INT64_C(1)));
        tuple.setNValue(1, ValueFactory::getBigIntValue(value_ctr++));
        tuple.setNValue(2, ValueFactory::getBigIntValue(value_ctr++));
        bool exceptionThrown = false;
        try {
            EXPECT_EQ(expected_result, this->table->insertTuple(tuple));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_EQ(expected_result, !exceptionThrown);

        //
        // Insert a null key value
        //
        TableTuple &tuple2 = this->table->tempTuple();
        tuple2.setAllNulls();
        tuple2.setNValue(1, ValueFactory::getBigIntValue(value_ctr++));
        tuple2.setNValue(2, ValueFactory::getBigIntValue(value_ctr++));
        exceptionThrown = false;
        try {
            EXPECT_EQ(expected_result, this->table->insertTuple(tuple2));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_EQ(expected_result, !exceptionThrown);
    }
}

TEST_F(ConstraintTest, UniqueTwoColumnNotNull) {
    //
    // Check to make sure that compound primary keys work
    //
    const int columnCount = 4;
    bool allow_null[columnCount] = { false, true, false, false };
    for (int ctr = 0; ctr < columnCount; ctr++) {
        char name[16];
        snprintf(name, 16, "col%02d", ctr);
        addColumn(name, VALUE_TYPE_BIGINT, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT), allow_null[ctr]);
    }

    std::vector<int> pkey_column_indices;
    pkey_column_indices.push_back(0);
    pkey_column_indices.push_back(2);
    pkey_column_indices.push_back(3);
    TableIndexScheme pkey("idx_pkey", BALANCED_TREE_INDEX,
                          pkey_column_indices, TableIndex::simplyIndexColumns(),
                          true, true, NULL);

    setTable(pkey);

    for (int64_t ctr = 0; ctr < NUM_OF_TUPLES; ctr++) {
        TableTuple &tuple = this->table->tempTuple();
        tuple.setAllNulls();
        tuple.setNValue(0, ValueFactory::getBigIntValue(ctr));
        tuple.setNValue(1, ValueFactory::getBigIntValue(ctr));
        tuple.setNValue(2, ValueFactory::getBigIntValue(ctr));
        tuple.setNValue(3, ValueFactory::getBigIntValue(ctr));
        //
        // This should always succeed
        //
        EXPECT_EQ(true, this->table->insertTuple(tuple));
        bool exceptionThrown = false;
        try {
            //
            // And this should always fail
            //
            EXPECT_EQ(false, this->table->insertTuple(tuple));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_TRUE(exceptionThrown);
    }
}

TEST_F(ConstraintTest, UniqueTwoColumnAllowNull) {
    //
    // Check to make sure that compound primary keys work
    //
    const int columnCount = 4;
    bool allow_null[columnCount] = { true, true, true, true };
    for (int ctr = 0; ctr < columnCount; ctr++) {
        char name[16];
        snprintf(name, 16, "col%02d", ctr);
        addColumn(name, VALUE_TYPE_BIGINT, NValue::getTupleStorageSize(VALUE_TYPE_BIGINT), allow_null[ctr]);
    }

    std::vector<int> pkey_column_indices;
    pkey_column_indices.push_back(0);
    pkey_column_indices.push_back(2);
    pkey_column_indices.push_back(3);
    TableIndexScheme pkey("idx_pkey", BALANCED_TREE_INDEX,
                          pkey_column_indices, TableIndex::simplyIndexColumns(),
                          true, true, NULL);

    setTable(pkey);

    int64_t value_ctr = 0;
    for (int ctr = 0; ctr < 2; ctr++) {
        bool expected_result = (ctr == 0);
        //
        // Insert a regular value
        //
        TableTuple &tuple = this->table->tempTuple();
        tuple.setAllNulls();
        tuple.setNValue(0, ValueFactory::getBigIntValue(INT64_C(1)));
        tuple.setNValue(1, ValueFactory::getBigIntValue(value_ctr++));
        tuple.setNValue(2, ValueFactory::getBigIntValue(INT64_C(2)));
        tuple.setNValue(3, ValueFactory::getBigIntValue(INT64_C(3)));
        bool exceptionThrown = false;
        try {
            EXPECT_EQ(expected_result, this->table->insertTuple(tuple));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_EQ(expected_result, !exceptionThrown);

        //
        // Insert a null key value
        //
        TableTuple &tuple2 = this->table->tempTuple();
        tuple2.setAllNulls();
        tuple2.setNValue(1, ValueFactory::getBigIntValue(value_ctr++));
        exceptionThrown = false;
        try {
            EXPECT_EQ(expected_result, this->table->insertTuple(tuple2));
        } catch (SerializableEEException &e) {
            exceptionThrown = true;
        }
        EXPECT_EQ(expected_result, !exceptionThrown);
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
