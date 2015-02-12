/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

#include <string>
#include <boost/foreach.hpp>
#include <boost/unordered_map.hpp>

#include "harness.h"
#include "common/executorcontext.hpp"
#include "common/TupleSchema.h"
#include "common/debuglog.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/BinaryLogSink.h"
#include "storage/persistenttable.h"
#include "storage/tableiterator.h"
#include "storage/table.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/DRTupleStream.h"
#include "indexes/tableindex.h"

using namespace voltdb;
using namespace std;

class TableAndIndexTest : public Test {
    public:
        TableAndIndexTest() {
            engine = new ExecutorContext(0, 0, NULL, &topend, &pool, NULL, "", 0, &drStream);
            mem = 0;
            *reinterpret_cast<int64_t*>(signature) = 42;
            drStream.configure(44);

            vector<voltdb::ValueType> districtColumnTypes;
            vector<int32_t> districtColumnLengths;
            vector<bool> districtColumnAllowNull(11, true);
            districtColumnAllowNull[0] = false;

            districtColumnTypes.push_back(VALUE_TYPE_TINYINT); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            districtColumnTypes.push_back(VALUE_TYPE_TINYINT); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(15);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(15);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(15);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(15);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(2);
            districtColumnTypes.push_back(VALUE_TYPE_VARCHAR); districtColumnLengths.push_back(9);
            districtColumnTypes.push_back(VALUE_TYPE_DOUBLE); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            districtColumnTypes.push_back(VALUE_TYPE_DOUBLE); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            districtColumnTypes.push_back(VALUE_TYPE_INTEGER); districtColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));

            districtTupleSchema = TupleSchema::createTupleSchemaForTest(districtColumnTypes, districtColumnLengths, districtColumnAllowNull);
            districtReplicaTupleSchema = TupleSchema::createTupleSchemaForTest(districtColumnTypes, districtColumnLengths, districtColumnAllowNull);

            districtIndex1ColumnIndices.push_back(1);
            districtIndex1ColumnIndices.push_back(0);

            districtIndex1Scheme = TableIndexScheme("District primary key index", HASH_TABLE_INDEX,
                                                    districtIndex1ColumnIndices, TableIndex::simplyIndexColumns(),
                                                    true, false, districtTupleSchema);

            vector<voltdb::ValueType> warehouseColumnTypes;
            vector<int32_t> warehouseColumnLengths;
            vector<bool> warehouseColumnAllowNull(9, true);
            warehouseColumnAllowNull[0] = false;

            warehouseColumnTypes.push_back(VALUE_TYPE_TINYINT); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(15);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(15);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(15);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(15);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(2);
            warehouseColumnTypes.push_back(VALUE_TYPE_VARCHAR); warehouseColumnLengths.push_back(9);
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            warehouseColumnTypes.push_back(VALUE_TYPE_DOUBLE); warehouseColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));

            warehouseTupleSchema = TupleSchema::createTupleSchemaForTest(warehouseColumnTypes, warehouseColumnLengths, warehouseColumnAllowNull);

            warehouseIndex1ColumnIndices.push_back(0);

            warehouseIndex1Scheme = TableIndexScheme("Warehouse primary key index", HASH_TABLE_INDEX,
                                                     warehouseIndex1ColumnIndices, TableIndex::simplyIndexColumns(),
                                                     true, true, warehouseTupleSchema);

            vector<voltdb::ValueType> customerColumnTypes;
            vector<int32_t> customerColumnLengths;
            vector<bool> customerColumnAllowNull(21, true);
            customerColumnAllowNull[0] = false;
            customerColumnAllowNull[1] = false;
            customerColumnAllowNull[2] = false;

            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_TINYINT); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            customerColumnTypes.push_back(VALUE_TYPE_TINYINT); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TINYINT));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(9);
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(15);
            customerColumnTypes.push_back(VALUE_TYPE_TIMESTAMP); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_TIMESTAMP));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(2);
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_DOUBLE); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_DOUBLE));
            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_INTEGER); customerColumnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            customerColumnTypes.push_back(VALUE_TYPE_VARCHAR); customerColumnLengths.push_back(500);

            customerTupleSchema = TupleSchema::createTupleSchemaForTest(customerColumnTypes, customerColumnLengths, customerColumnAllowNull);

            customerIndex1ColumnIndices.push_back(2);
            customerIndex1ColumnIndices.push_back(1);
            customerIndex1ColumnIndices.push_back(0);

            customerIndex1Scheme = TableIndexScheme("Customer primary key index", HASH_TABLE_INDEX,
                                                    customerIndex1ColumnIndices, TableIndex::simplyIndexColumns(),
                                                    true, true, customerTupleSchema);

            customerIndex2ColumnIndices.push_back(2);
            customerIndex2ColumnIndices.push_back(1);
            customerIndex2ColumnIndices.push_back(5);
            customerIndex2ColumnIndices.push_back(3);

            customerIndex2Scheme = TableIndexScheme("Customer index 1", HASH_TABLE_INDEX,
                                                    customerIndex2ColumnIndices, TableIndex::simplyIndexColumns(),
                                                    true, true, customerTupleSchema);
            customerIndexes.push_back(customerIndex2Scheme);

            customerIndex3ColumnIndices.push_back(2);
            customerIndex3ColumnIndices.push_back(1);
            customerIndex3ColumnIndices.push_back(5);

            customerIndex3Scheme = TableIndexScheme("Customer index 3", HASH_TABLE_INDEX,
                                                    customerIndex3ColumnIndices, TableIndex::simplyIndexColumns(),
                                                    false, false, customerTupleSchema);
            customerIndexes.push_back(customerIndex3Scheme);


            string districtColumnNamesArray[11] = {
                "D_ID", "D_W_ID", "D_NAME", "D_STREET_1", "D_STREET_2", "D_CITY",
                "D_STATE", "D_ZIP", "D_TAX", "D_YTD", "D_NEXT_O_ID" };
            const vector<string> districtColumnNames(districtColumnNamesArray, districtColumnNamesArray + 11 );

            string warehouseColumnNamesArray[9] = {
                    "W_ID", "W_NAME", "W_STREET_1", "W_STREET_2", "W_CITY", "W_STATE",
                    "W_ZIP", "W_TAX", "W_YTD" };
            const vector<string> warehouseColumnNames(warehouseColumnNamesArray, warehouseColumnNamesArray + 9 );

            string customerColumnNamesArray[21] = {
                    "C_ID", "C_D_ID", "C_W_ID", "C_FIRST", "C_MIDDLE", "C_LAST",
                    "C_STREET_1", "C_STREET_2", "C_CITY", "C_STATE", "C_ZIP", "C_PHONE",
                    "C_SINCE_TIMESTAMP", "C_CREDIT", "C_CREDIT_LIM", "C_DISCOUNT",
                    "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DATA" };
            const vector<string> customerColumnNames(customerColumnNamesArray, customerColumnNamesArray + 21 );

            districtTable = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "DISTRICT", districtTupleSchema, districtColumnNames, signature, false, 0));
            districtTableReplica = reinterpret_cast<PersistentTable*>(voltdb::TableFactory::getPersistentTable(0, "DISTRICT", districtReplicaTupleSchema, districtColumnNames, signature, false, 0));


            TableIndex *pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(districtIndex1Scheme);
            TableIndex *pkeyIndexReplica = TableIndexFactory::TableIndexFactory::getInstance(districtIndex1Scheme);
            assert(pkeyIndex);
            districtTable->addIndex(pkeyIndex);
            districtTable->setPrimaryKeyIndex(pkeyIndex);
            districtTableReplica->addIndex(pkeyIndexReplica);
            districtTableReplica->setPrimaryKeyIndex(pkeyIndexReplica);

            // add other indexes
            BOOST_FOREACH(TableIndexScheme &scheme, districtIndexes) {
                TableIndex *index = TableIndexFactory::getInstance(scheme);
                assert(index);
                districtTable->addIndex(index);
            }

            districtTempTable = dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "DISTRICT TEMP", districtTable,
                                                 &limits));

            warehouseTable = voltdb::TableFactory::getPersistentTable(0, "WAREHOUSE",
                                                                      warehouseTupleSchema, warehouseColumnNames,
                                                                      signature, false,
                                                                      0, false, false);

            pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(warehouseIndex1Scheme);
            assert(pkeyIndex);
            warehouseTable->addIndex(pkeyIndex);
            warehouseTable->setPrimaryKeyIndex(pkeyIndex);

            // add other indexes
            BOOST_FOREACH(TableIndexScheme &scheme, warehouseIndexes) {
                TableIndex *index = TableIndexFactory::getInstance(scheme);
                assert(index);
                warehouseTable->addIndex(index);
            }

            warehouseTempTable =  dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "WAREHOUSE TEMP", warehouseTable,
                                                 &limits));

            customerTable = voltdb::TableFactory::getPersistentTable(0, "CUSTOMER",
                                                                     customerTupleSchema, customerColumnNames,
                                                                     signature, false,
                                                                     0, false, false);

            pkeyIndex = TableIndexFactory::TableIndexFactory::getInstance(customerIndex1Scheme);
            assert(pkeyIndex);
            customerTable->addIndex(pkeyIndex);
            customerTable->setPrimaryKeyIndex(pkeyIndex);

            // add other indexes
            BOOST_FOREACH(TableIndexScheme &scheme, customerIndexes) {
                TableIndex *index = TableIndexFactory::getInstance(scheme);
                assert(index);
                customerTable->addIndex(index);
            }

            customerTempTable =  dynamic_cast<TempTable*>(
                TableFactory::getCopiedTempTable(0, "CUSTOMER TEMP", customerTable,
                                                 &limits));
        }

        ~TableAndIndexTest() {
            delete engine;
            delete districtTable;
            delete districtTableReplica;
            delete districtTempTable;
            delete warehouseTable;
            delete warehouseTempTable;
            delete customerTable;
            delete customerTempTable;
        }

    protected:
        int mem;
        TempTableLimits limits;
        ExecutorContext *engine;
        DRTupleStream drStream;
        DummyTopend topend;
        Pool pool;
        BinaryLogSink sink;

        TupleSchema      *districtTupleSchema;
        TupleSchema      *districtReplicaTupleSchema;
        vector<TableIndexScheme> districtIndexes;
        PersistentTable  *districtTable;
        PersistentTable  *districtTableReplica;
        TempTable        *districtTempTable;
        vector<int>       districtIndex1ColumnIndices;
        TableIndexScheme  districtIndex1Scheme;

        TupleSchema      *warehouseTupleSchema;
        vector<TableIndexScheme> warehouseIndexes;
        Table            *warehouseTable;
        TempTable        *warehouseTempTable;
        vector<int>       warehouseIndex1ColumnIndices;
        TableIndexScheme  warehouseIndex1Scheme;

        TupleSchema      *customerTupleSchema;
        vector<TableIndexScheme> customerIndexes;
        Table            *customerTable;
        TempTable        *customerTempTable;
        vector<int>       customerIndex1ColumnIndices;
        TableIndexScheme  customerIndex1Scheme;
        vector<int>       customerIndex2ColumnIndices;
        vector<ValueType> customerIndex2ColumnTypes;
        TableIndexScheme  customerIndex2Scheme;
        vector<int>       customerIndex3ColumnIndices;
        TableIndexScheme  customerIndex3Scheme;
        char signature[20];
};

/*
 * Check that inserting, deleting and updating works and propagates via DR buffers
 */
TEST_F(TableAndIndexTest, DrTest) {
    drStream.m_enabled = true;
    vector<NValue> cachedStringValues;//To free at the end of the test
    TableTuple temp_tuple = districtTempTable->tempTuple();
    temp_tuple.setNValue(0, ValueFactory::getTinyIntValue(static_cast<int8_t>(7)));
    temp_tuple.setNValue(1, ValueFactory::getTinyIntValue(static_cast<int8_t>(3)));
    cachedStringValues.push_back(ValueFactory::getStringValue("A District"));
    temp_tuple.setNValue(2, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Street Addy"));
    temp_tuple.setNValue(3, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("meh"));
    temp_tuple.setNValue(4, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("westerfield"));
    temp_tuple.setNValue(5, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BA"));
    temp_tuple.setNValue(6, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("99999"));
    temp_tuple.setNValue(7, cachedStringValues.back());
    temp_tuple.setNValue(8, ValueFactory::getDoubleValue(static_cast<double>(.0825)));
    temp_tuple.setNValue(9, ValueFactory::getDoubleValue(static_cast<double>(15241.45)));
    temp_tuple.setNValue(10, ValueFactory::getIntegerValue(static_cast<int32_t>(21)));

    /*
     * Test that insert propagates
     */
    districtTable->insertTuple(temp_tuple);

    //Flush to generate a buffer
    drStream.periodicFlush(-1, 42);
    EXPECT_TRUE( topend.receivedDRBuffer );

    //Buidl the map expected by the binary log sink
    boost::unordered_map<int64_t, PersistentTable*> tables;
    tables[42] = districtTableReplica;

    //Fetch the generated block of log data
    boost::shared_ptr<StreamBlock> sb = topend.blocks[0];
    topend.blocks.pop_back();
    boost::shared_array<char> data = topend.data[0];
    topend.data.pop_back();
    topend.receivedDRBuffer = false;

    //Add a length prefix for test, then apply it
    *reinterpret_cast<int32_t*>(&data.get()[4]) = htonl(static_cast<int32_t>(sb->offset()));
    drStream.m_enabled = false;
    sink.apply(&data[4], tables, &pool);
    drStream.m_enabled = true;

    //Should have one row from the insert
    EXPECT_EQ( 1, districtTableReplica->activeTupleCount());

    TableIterator iterator = districtTableReplica->iterator();
    ASSERT_TRUE(iterator.hasNext());
    TableTuple nextTuple(districtTableReplica->schema());
    iterator.next(nextTuple);
    EXPECT_EQ(nextTuple.getNValue(7).compare(cachedStringValues.back()), 0);

    //Prepare to insert in a new txn
    engine->setupForPlanFragments( NULL, 100, 100, 99, 72);

    /*
     * Test that update propagates
     */
    TableTuple toUpdate = districtTable->lookupTuple(temp_tuple);

    //Use a different string value for one column
    cachedStringValues.push_back(ValueFactory::getStringValue("shoopdewoop"));
    temp_tuple.setNValue(3, cachedStringValues.back());
    districtTable->updateTuple( toUpdate, temp_tuple);

    //Flush to generate the log buffer
    drStream.periodicFlush(-1, 101);
    EXPECT_TRUE( topend.receivedDRBuffer );

    //Grab the generated block of log data
    sb = topend.blocks[0];
    topend.blocks.pop_back();
    data = topend.data[0];
    topend.data.pop_back();
    topend.receivedDRBuffer = false;

    //Add a length prefix for test and apply it
    *reinterpret_cast<int32_t*>(&data.get()[4]) = htonl(static_cast<int32_t>(sb->offset()));
    drStream.m_enabled = false;
    sink.apply(&data[4], tables, &pool);
    drStream.m_enabled = true;

    //Expect one row with the update
    EXPECT_EQ( 1, districtTableReplica->activeTupleCount());

    //Validate the update took place
    TableTuple toDelete = districtTable->lookupTuple(temp_tuple);
    EXPECT_EQ(0, toDelete.getNValue(3).compare(cachedStringValues.back()));

    //Prep another transaction to test propagating a delete
    engine->setupForPlanFragments( NULL, 102, 102, 101, 89);

    districtTable->deleteTuple( toDelete, true);

    //Flush to generate the buffer
    drStream.periodicFlush(-1, 102);
    EXPECT_TRUE( topend.receivedDRBuffer );

    //Grab the generated blocks of data
    sb = topend.blocks[0];
    topend.blocks.pop_back();
    data = topend.data[0];
    topend.data.pop_back();
    topend.receivedDRBuffer = false;

    //Add a length prefix for test, and apply the update
    *reinterpret_cast<int32_t*>(&data.get()[4]) = htonl(static_cast<int32_t>(sb->offset()));
    drStream.m_enabled = false;
    sink.apply(&data[4], tables, &pool);
    drStream.m_enabled = true;

    //Expect no rows after the delete propagates
    EXPECT_EQ( 0, districtTableReplica->activeTupleCount());

    for (vector<NValue>::const_iterator i = cachedStringValues.begin(); i != cachedStringValues.end(); i++) {
        (*i).free();
    }
}

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
    cachedStringValues.push_back(ValueFactory::getStringValue("EZ Street House"));
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
    cachedStringValues.push_back(ValueFactory::getStringValue("(193) 099-9082"));
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
    TableIterator districtIterator = districtTempTable->iterator();
    while (districtIterator.next(districtTuple)) {
        if (!districtTable->insertTuple(districtTuple)) {
            cout << "Failed to insert tuple from input table '"
                 << districtTempTable->name() << "' into target table '"
                 << districtTable->name() << "'" << endl;
        }
    }
    districtTempTable->deleteAllTuplesNonVirtual(true);

    TableTuple warehouseTuple = TableTuple(warehouseTempTable->schema());
    TableIterator warehouseIterator = warehouseTempTable->iterator();
    while (warehouseIterator.next(warehouseTuple)) {
        if (!warehouseTable->insertTuple(warehouseTuple)) {
            cout << "Failed to insert tuple from input table '" << warehouseTempTable->name() << "' into target table '" << warehouseTable->name() << "'" << endl;
        }
    }
    warehouseTempTable->deleteAllTuplesNonVirtual(true);

    TableTuple customerTuple = TableTuple(customerTempTable->schema());
    TableIterator customerIterator = customerTempTable->iterator();
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
    cachedStringValues.push_back(ValueFactory::getStringValue("Random Depart"));
    temp_tuple->setNValue(6, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("Place2"));
    temp_tuple->setNValue(7, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("BiggerPlace"));
    temp_tuple->setNValue(8, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("AL"));
    temp_tuple->setNValue(9, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("13908"));
    temp_tuple->setNValue(10, cachedStringValues.back());
    cachedStringValues.push_back(ValueFactory::getStringValue("(913) 909-0928"));
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

    customerIterator = customerTempTable->iterator();
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
