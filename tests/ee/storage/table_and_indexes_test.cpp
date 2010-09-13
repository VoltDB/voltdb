/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
#include <string>
#include "common/executorcontext.hpp"
#include "common/TupleSchema.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "common/DummyUndoQuantum.hpp"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "indexes/tableindex.h"

using namespace voltdb;
using namespace std;

string      districtColumnNames[11] = {
        "D_ID", "D_W_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY",
        "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID" };
string      warehouseColumnNames[9] = {
        "W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE",
        "W_ZIP", "W_TAX", "W_YTD" };
string      customerColumnNames[21] = {
        "C_ID", "C_D_ID", "C_W_ID", "C_FIRST", "C_MIDDLE", "C_LAST",
        "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE",
        "C_SINCE_TIMESTAMP", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT",
        "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA" };

class TableAndIndexTest : public Test {
    public:
        TableAndIndexTest() {
            dummyUndo = new DummyUndoQuantum();
            engine = new ExecutorContext(0, 0, dummyUndo, NULL, false, 0, "", 0);
            mem = 0;

            vector<voltdb::ValueType> districtColumnTypes;
            vector<int32_t> districtColumnLengths;
            vector<bool> districtColumnAllowNull(11, true);
            districtColumnAllowNull[0] = false;

            districtColumnTypes.push_back(VALUE_TYPE_TINYINT); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            districtColumnTypes.push_back(VALUE_TYPE_TINYINT); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(16);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(32);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(32);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(32);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(2);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(9);
            districtColumnTypes.push_back(VALUE_TYPE_DOUBLE); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            districtColumnTypes.push_back(VALUE_TYPE_DOUBLE); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            districtColumnTypes.push_back(VALUE_TYPE_INTEGER); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));

            districtTupleSchema = TupleSchema::createTupleSchema(districtColumnTypes, districtColumnLengths, districtColumnAllowNull, true);

            districtIndex1ColumnIndices.push_back(1);
            districtIndex1ColumnIndices.push_back(0);
            districtIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            districtIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);

            districtIndex1Scheme = TableIndexScheme("District primary key index", HASH_TABLE_INDEX, districtIndex1ColumnIndices, districtIndex1ColumnTypes, true, true, districtTupleSchema);

            vector<voltdb::ValueType> warehouseColumnTypes;
            vector<int32_t> warehouseColumnLengths;
            vector<bool> warehouseColumnAllowNull(9, true);
            warehouseColumnAllowNull[0] = false;

            warehouseColumnTypes.push_back(VALUE_TYPE_TINYINT); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(16);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(32);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(2);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(9);
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));

            warehouseTupleSchema = TupleSchema::createTupleSchema(warehouseColumnTypes, warehouseColumnLengths, warehouseColumnAllowNull, true);

            warehouseIndex1ColumnIndices.push_back(0);
            warehouseIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);

            warehouseIndex1Scheme = TableIndexScheme("Warehouse primary key index", ARRAY_INDEX, warehouseIndex1ColumnIndices, warehouseIndex1ColumnTypes, true, true, warehouseTupleSchema);

            vector<voltdb::ValueType> customerColumnTypes;
            vector<int32_t> customerColumnLengths;
            vector<bool> customerColumnAllowNull(21, true);
            customerColumnAllowNull[0] = false;
            customerColumnAllowNull[1] = false;
            customerColumnAllowNull[2] = false;

            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_TINYINT); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            customerColumnTypes.push_back(VALUE_TYPE_TINYINT); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(9);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(32);
            customerColumnTypes.push_back(VALUE_TYPE_TIMESTAMP); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TIMESTAMP));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(500);

            customerTupleSchema = TupleSchema::createTupleSchema(customerColumnTypes, customerColumnLengths, customerColumnAllowNull, true);

            customerIndex1ColumnIndices.push_back(2);
            customerIndex1ColumnIndices.push_back(1);
            customerIndex1ColumnIndices.push_back(0);
            customerIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex1ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex1ColumnTypes.push_back(VALUE_TYPE_INTEGER);

            customerIndex1Scheme = TableIndexScheme("Customer primary key index", HASH_TABLE_INDEX, customerIndex1ColumnIndices, customerIndex1ColumnTypes, true, true, customerTupleSchema);

            customerIndex2ColumnIndices.push_back(2);
            customerIndex2ColumnIndices.push_back(1);
            customerIndex2ColumnIndices.push_back(5);
            customerIndex2ColumnIndices.push_back(3);
            customerIndex2ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex2ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex2ColumnTypes.push_back(VALUE_TYPE_VARCHAR);
            customerIndex2ColumnTypes.push_back(VALUE_TYPE_VARCHAR);

            customerIndex2Scheme = TableIndexScheme("Customer index 1", HASH_TABLE_INDEX, customerIndex2ColumnIndices, customerIndex2ColumnTypes, true, false, customerTupleSchema);
            customerIndexes.push_back(customerIndex2Scheme);

            customerIndex3ColumnIndices.push_back(2);
            customerIndex3ColumnIndices.push_back(1);
            customerIndex3ColumnIndices.push_back(5);
            customerIndex3ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex3ColumnTypes.push_back(VALUE_TYPE_TINYINT);
            customerIndex3ColumnTypes.push_back(VALUE_TYPE_VARCHAR);

            customerIndex3Scheme = TableIndexScheme("Customer index 3", HASH_TABLE_INDEX,
                                                     customerIndex3ColumnIndices, customerIndex3ColumnTypes,
                                                     false, false, customerTupleSchema);
            customerIndexes.push_back(customerIndex3Scheme);



            districtTable = voltdb::TableFactory::getPersistentTable(0,engine, "DISTRICT",
                                                                     districtTupleSchema,
                                                                     districtColumnNames,
                                                                     districtIndex1Scheme,
                                                                     districtIndexes, 0,
                                                                     false, false);

            districtTempTable = dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "DISTRICT TEMP", districtTable, &mem));

            warehouseTable = voltdb::TableFactory::getPersistentTable(0, engine, "WAREHOUSE",
                                                                      warehouseTupleSchema,
                                                                      warehouseColumnNames,
                                                                      warehouseIndex1Scheme,
                                                                      warehouseIndexes, 0,
                                                                      false, false);

            warehouseTempTable =  dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "WAREHOUSE TEMP", warehouseTable, &mem));

            customerTable = voltdb::TableFactory::getPersistentTable(0,engine, "CUSTOMER",
                                                                     customerTupleSchema,
                                                                     customerColumnNames,
                                                                     customerIndex1Scheme,
                                                                     customerIndexes, 0,
                                                                     false, false);

            customerTempTable =  dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "CUSTOMER TEMP", customerTable, &mem));
        }

        ~TableAndIndexTest() {
            delete engine;
            delete dummyUndo;
            delete districtTable;
            delete districtTempTable;
            delete warehouseTable;
            delete warehouseTempTable;
            delete customerTable;
            delete customerTempTable;
        }

    protected:
        int mem;
        UndoQuantum *dummyUndo;
        ExecutorContext *engine;

        TupleSchema      *districtTupleSchema;
        vector<TableIndexScheme> districtIndexes;
        Table            *districtTable;
        TempTable        *districtTempTable;
        vector<int>       districtIndex1ColumnIndices;
        vector<ValueType> districtIndex1ColumnTypes;
        TableIndexScheme  districtIndex1Scheme;

        TupleSchema      *warehouseTupleSchema;
        vector<TableIndexScheme> warehouseIndexes;
        Table            *warehouseTable;
        TempTable        *warehouseTempTable;
        vector<int>       warehouseIndex1ColumnIndices;
        vector<ValueType> warehouseIndex1ColumnTypes;
        TableIndexScheme  warehouseIndex1Scheme;

        TupleSchema      *customerTupleSchema;
        vector<TableIndexScheme> customerIndexes;
        Table            *customerTable;
        TempTable        *customerTempTable;
        vector<int>       customerIndex1ColumnIndices;
        vector<ValueType> customerIndex1ColumnTypes;
        TableIndexScheme  customerIndex1Scheme;
        vector<int>       customerIndex2ColumnIndices;
        vector<ValueType> customerIndex2ColumnTypes;
        TableIndexScheme  customerIndex2Scheme;
        vector<int>       customerIndex3ColumnIndices;
        vector<ValueType> customerIndex3ColumnTypes;
        TableIndexScheme  customerIndex3Scheme;
};

TEST_F(TableAndIndexTest, BigTest) {
    vector<NValue> cachedStringValues;//To free at the end of the test
    TableTuple *temp_tuple = &districtTempTable->tempTuple();
    temp_tuple->setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)));
    temp_tuple->setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("A District"));
    temp_tuple->setNValue(2, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Street Addy"));
    temp_tuple->setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("meh"));
    temp_tuple->setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("westerfield"));
    temp_tuple->setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BA"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("99999"));
    temp_tuple->setNValue(7, cachedStringValues.back());
    temp_tuple->setNValue(8, ValueFactory::getDoubleValue(static_cast<double>(.0825)));
    temp_tuple->setNValue(9, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    temp_tuple->setNValue(10, ValueFactory::getIntegerValue(static_cast<int32_t>(21)));
    districtTempTable->insertTupleNonVirtual(*temp_tuple);

    temp_tuple = &warehouseTempTable->tempTuple();
    temp_tuple->setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("EZ Street WHouse"));
    temp_tuple->setNValue(1, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Headquarters"));
    temp_tuple->setNValue(2, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("77 Mass. Ave."));
    temp_tuple->setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Cambridge"));
    temp_tuple->setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("AZ"));
    temp_tuple->setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("12938"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    temp_tuple->setNValue(7, ValueFactory::getDoubleValue(static_cast<double>(.1234)));
    temp_tuple->setNValue(8, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    warehouseTempTable->insertTupleNonVirtual(*temp_tuple);

    temp_tuple = &customerTempTable->tempTuple();
    temp_tuple->setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(42)));
    temp_tuple->setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)));
    temp_tuple->setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("I"));
    temp_tuple->setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BE"));
    temp_tuple->setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("lastname"));
    temp_tuple->setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Place"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Place2"));
    temp_tuple->setNValue(7, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BiggerPlace"));
    temp_tuple->setNValue(8, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("AL"));
    temp_tuple->setNValue(9, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("91083"));
    temp_tuple->setNValue(10, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("(193) 099 - 9082"));
    temp_tuple->setNValue(11, cachedStringValues.back());
    temp_tuple->setNValue(12, ValueFactory::getTimestampValue(static_cast<int32_t>(123456789)));
    cachedStringValues.push_back(ValueFactory::getStringValue("BC"));
    temp_tuple->setNValue(13, cachedStringValues.back());
    temp_tuple->setNValue(14, ValueFactory::getDoubleValue(static_cast<double>(19298943.12)));
    temp_tuple->setNValue(15, ValueFactory::getDoubleValue(static_cast<double>(.13)));
    temp_tuple->setNValue(16, ValueFactory::getDoubleValue(static_cast<double>(15.75)));
    temp_tuple->setNValue(17, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    temp_tuple->setNValue(18, ValueFactory::getIntegerValue(static_cast<int32_t>(0)));
    temp_tuple->setNValue(19, ValueFactory::getIntegerValue(static_cast<int32_t>(15)));
    temp_tuple->setNValue(20, ValueFactory::getStringValue("Some History"));
    customerTempTable->insertTupleNonVirtual(*temp_tuple);

    TableTuple districtTuple = TableTuple(districtTempTable->schema());
    TableIterator districtIterator(districtTempTable);
    while (districtIterator.next(districtTuple)) {
        if (!districtTable->insertTuple(districtTuple)) {
            cout << "Failed to insert tuple from input table '"
                 << districtTempTable->name() << "' into target table '"
                 << districtTable->name() << "'" << endl;
        }
    }
    districtTempTable->deleteAllTuplesNonVirtual(true);

    TableTuple warehouseTuple = TableTuple(warehouseTempTable->schema());
    TableIterator warehouseIterator(warehouseTempTable);
    while (warehouseIterator.next(warehouseTuple)) {
        if (!warehouseTable->insertTuple(warehouseTuple)) {
            cout << "Failed to insert tuple from input table '" << warehouseTempTable->name() << "' into target table '" << warehouseTable->name() << "'" << endl;
        }
    }
    warehouseTempTable->deleteAllTuplesNonVirtual(true);

    TableTuple customerTuple = TableTuple(customerTempTable->schema());
    TableIterator customerIterator(customerTempTable);
    while (customerIterator.next(customerTuple)) {
        //cout << "Inserting tuple '" << customerTuple.debug(customerTempTable) << "' into target table '" << customerTable->name() << "', address '" << customerTable << endl;
        if (!customerTable->insertTuple(customerTuple)) {
            cout << "Failed to insert tuple from input table '" << warehouseTempTable->name() << "' into target table '" << warehouseTable->name() << "'" << endl;
        }
    }
    customerTempTable->deleteAllTuplesNonVirtual(true);

    temp_tuple->setNValue(0, ValueFactory::getIntegerValue(static_cast<int32_t>(43)));
    temp_tuple->setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)));
    temp_tuple->setNValue(2, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("We"));
    temp_tuple->setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Be"));
    temp_tuple->setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Customer"));
    temp_tuple->setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Random Department"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Place2"));
    temp_tuple->setNValue(7, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BiggerPlace"));
    temp_tuple->setNValue(8, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("AL"));
    temp_tuple->setNValue(9, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("13908"));
    temp_tuple->setNValue(10, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("(913) 909 - 0928"));
    temp_tuple->setNValue(11, cachedStringValues.back());
    temp_tuple->setNValue(12, ValueFactory::getTimestampValue(static_cast<int64_t>(123456789)));
    cachedStringValues.push_back(ValueFactory::getStringValue("GC"));
    temp_tuple->setNValue(13, cachedStringValues.back());
    temp_tuple->setNValue(14, ValueFactory::getDoubleValue(static_cast<double>(19298943.12)));
    temp_tuple->setNValue(15, ValueFactory::getDoubleValue(static_cast<double>(.13)));
    temp_tuple->setNValue(16, ValueFactory::getDoubleValue(static_cast<double>(15.75)));
    temp_tuple->setNValue(17, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    temp_tuple->setNValue(18, ValueFactory::getIntegerValue(static_cast<int32_t>(1)));
    temp_tuple->setNValue(19, ValueFactory::getIntegerValue(static_cast<int32_t>(15)));
    temp_tuple->setNValue(20, ValueFactory::getStringValue("Some History"));
    customerTempTable->insertTupleNonVirtual(*temp_tuple);

    customerIterator = TableIterator(customerTempTable);
    while (customerIterator.next(customerTuple)) {
        //cout << "Inserting tuple '" << customerTuple.debug(customerTempTable) << "' into target table '" << customerTable->name() << "', address '" << customerTable << endl;
        if (!customerTable->insertTuple(customerTuple)) {
            cout << "Failed to insert tuple from input table '" << warehouseTempTable->name() << "' into target table '" << warehouseTable->name() << "'" << endl;
        }
    }
    customerTempTable->deleteAllTuplesNonVirtual(true);

    for (vector<NValue>::const_iterator i = cachedStringValues.begin(); i != cachedStringValues.end(); i++) {
        (*i).free();
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
