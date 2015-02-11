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

#include <sstream>
#include <iostream>
#include <boost/shared_ptr.hpp>
#include "harness.h"
#include "common/common.h"
#include "common/serializeio.h"
#include "common/debuglog.h"
#include "common/tabletuple.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "common/ValueFactory.hpp"

#define TUPLES 20

using namespace std;
using namespace voltdb;

#define NUM_OF_COLUMNS 6
ValueType col_types[NUM_OF_COLUMNS] = { VALUE_TYPE_TINYINT, VALUE_TYPE_BIGINT, VALUE_TYPE_BIGINT, VALUE_TYPE_BIGINT, VALUE_TYPE_VARCHAR, VALUE_TYPE_DOUBLE };

class TableSerializeTest : public Test {
    public:
        TableSerializeTest() :
          columnNames(NUM_OF_COLUMNS)
        {
            this->database_id = 1000;

            std::vector<voltdb::ValueType> columnTypes;
            std::vector<int32_t> columnSizes;
            std::vector<bool> columnAllowNull(NUM_OF_COLUMNS, false);
            for (int ctr = 0; ctr < NUM_OF_COLUMNS; ctr++) {
                char name[16];
                if (ctr == 0) ::snprintf(name, 16, "id");
                else ::snprintf(name, 16, "val%02d", ctr);
                columnNames[ctr] = name;
                int size = (col_types[ctr] != VALUE_TYPE_VARCHAR ? 8 : 20);
                columnSizes.push_back(static_cast<int32_t>(size));
                columnTypes.push_back(col_types[ctr]);
            }
            voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnSizes, columnAllowNull);
            table_ = TableFactory::getTempTable(this->database_id, "temp_table", schema, columnNames, NULL);

            for (int64_t i = 1; i <= TUPLES; ++i) {
                TableTuple &tuple = table_->tempTuple();
                tuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(i)));
                tuple.setNValue(1, ValueFactory::getBigIntValue(static_cast<int16_t>(i % 2)));
                tuple.setNValue(2, ValueFactory::getBigIntValue(static_cast<int32_t>(i % 3)));
                tuple.setNValue(3, ValueFactory::getBigIntValue(static_cast<int64_t>(i % 5)));
                ostringstream str;
                str << "varchar string:" << (i % 7);
                NValue stringValue = ValueFactory::getStringValue(str.str());
                tuple.setNValueAllocateForObjectCopies(4, stringValue, NULL);
                stringValue.free();
                tuple.setNValue(5, ValueFactory::getDoubleValue(3.14f * static_cast<double>(i)));
                table_->insertTuple(tuple);
            }

        }
        ~TableSerializeTest() {
            table_->deleteAllTuples(true);
            delete table_;
        }
    protected:
        CatalogId database_id;
        CatalogId table_id;
        Table* table_;
        std::vector<std::string> columnNames;
};


TEST_F(TableSerializeTest, RoundTrip) {
    // print out the first table
    /*TableTuple tuple(table_.get());
    TableIterator iter = table_->iterator();
    VOLT_DEBUG("TABLE 1");
    while (iter.next(tuple)) {
        VOLT_DEBUG(" %s", tuple.debug(table_.get()).c_str());
    }*/
    // Serialize the table
    CopySerializeOutput serialize_out;
    table_->serializeTo(serialize_out);
    size_t size = serialize_out.size();

    // Deserialize the table: verify that it matches the existing table
    ReferenceSerializeInputBE serialize_in(serialize_out.data() + sizeof(int32_t), serialize_out.size() - sizeof(int32_t));
    TempTableLimits limits;
    TupleSchema *schema = TupleSchema::createTupleSchema(table_->schema());
    Table* deserialized = TableFactory::getTempTable(this->database_id, "foo", schema, columnNames, &limits);
    deserialized->loadTuplesFrom(serialize_in, NULL);
    int colnum = table_->columnCount();
    EXPECT_EQ(colnum, deserialized->columnCount());
    for (int i = 0; i < colnum; ++i) {
        EXPECT_EQ(table_->columnName(i), deserialized->columnName(i));
    }

    // Serialize the table a second time, verify that it
    CopySerializeOutput serialize_out2;
    deserialized->serializeTo(serialize_out2);
    size_t size2 = serialize_out2.size();
    ASSERT_EQ(size, size2);
    const void *data1 = serialize_out.data();
    const void *data2 = serialize_out2.data();
    EXPECT_EQ(0, ::memcmp(data1, data2, size));
    deserialized->deleteAllTuples(true);
    delete deserialized;
}

TEST_F(TableSerializeTest, NullStrings) {
    std::vector<std::string> columnNames(1);
    std::vector<voltdb::ValueType> columnTypes(1, voltdb::VALUE_TYPE_VARCHAR);
    std::vector<int32_t> columnSizes(1, 20);
    std::vector<bool> columnAllowNull(1, false);
    voltdb::TupleSchema *schema = voltdb::TupleSchema::createTupleSchemaForTest(columnTypes, columnSizes, columnAllowNull);
    columnNames[0] = "";
    table_->deleteAllTuples(true);
    delete table_;
    table_ = TableFactory::getTempTable(this->database_id, "temp_table", schema, columnNames, NULL);

    TableTuple& tuple = table_->tempTuple();
    tuple.setNValue(0, ValueFactory::getNullStringValue());
    table_->insertTuple(tuple);

    // Serialize the table
    CopySerializeOutput serialize_out;
    table_->serializeTo(serialize_out);

    // Deserialize the table: verify that it matches the existing table
    ReferenceSerializeInputBE serialize_in(serialize_out.data() + sizeof(int32_t), serialize_out.size() - sizeof(int32_t));
    TempTableLimits limits;
    schema = TupleSchema::createTupleSchema(table_->schema());
    Table* deserialized = TableFactory::getTempTable(this->database_id, "foo", schema, columnNames, &limits);
    deserialized->loadTuplesFrom(serialize_in, NULL);

    EXPECT_EQ(1, deserialized->activeTupleCount());
    EXPECT_EQ(1, table_->activeTupleCount());
    EXPECT_EQ(1, deserialized->columnCount());
    EXPECT_EQ(1, table_->columnCount());
    EXPECT_EQ("", table_->columnName(0));
    EXPECT_EQ("", deserialized->columnName(0));
    EXPECT_EQ(VALUE_TYPE_VARCHAR, table_->schema()->columnType(0));
    EXPECT_EQ(VALUE_TYPE_VARCHAR, deserialized->schema()->columnType(0));
    EXPECT_EQ(false, table_->schema()->columnIsInlined(0));

    TableIterator iter = deserialized->iterator();
    TableTuple t(deserialized->schema());
    int count = 0;
    while (iter.next(t)) {
        const TupleSchema::ColumnInfo *columnInfo = tuple.getSchema()->getColumnInfo(0);
        EXPECT_EQ(VALUE_TYPE_VARCHAR, columnInfo->getVoltType());
        const TupleSchema::ColumnInfo *tcolumnInfo = t.getSchema()->getColumnInfo(0);
        EXPECT_EQ(VALUE_TYPE_VARCHAR, tcolumnInfo->getVoltType());
        EXPECT_TRUE(tuple.getNValue(0).isNull());
        EXPECT_TRUE(t.getNValue(0).isNull());
        EXPECT_TRUE(ValueFactory::getNullStringValue().op_equals(tuple.getNValue(0)).isTrue());
        EXPECT_TRUE(ValueFactory::getNullStringValue().op_equals(t.getNValue(0)).isTrue());
        count += 1;
    }
    EXPECT_EQ(1, count);
    delete deserialized;
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
