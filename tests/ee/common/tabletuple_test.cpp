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

#include "common/SynchronizedThreadLock.h"
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "common/ThreadLocalPool.h"
#include "common/TupleSchemaBuilder.h"

#include "storage/tablefactory.h"
#include "storage/table.h"
#include "storage/temptable.h"

#include "test_utils/ScopedTupleSchema.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/UniqueEngine.hpp"


using namespace voltdb;

class TableTupleTest : public Test {
public:
    ~TableTupleTest() {
        voltdb::globalDestroyOncePerProcess();
    }
};

TEST_F(TableTupleTest, ComputeNonInlinedMemory)
{
    UniqueEngine engine = UniqueEngineBuilder().build();
    Pool *pool = ExecutorContext::getTempStringPool();

    // Make sure that inlined strings are actually inlined
    int32_t maxInlinableLength = UNINLINEABLE_OBJECT_LENGTH/MAX_BYTES_PER_UTF8_CHARACTER - 1;
    ScopedTupleSchema allInlineSchema{Tools::buildSchema(ValueType::tBIGINT,
            std::make_pair(ValueType::tVARCHAR, maxInlinableLength))};
    PoolBackedTupleStorage tupleStorage;
    tupleStorage.init(allInlineSchema.get(), pool);
    tupleStorage.allocateActiveTuple();
    TableTuple inlineTuple = tupleStorage;

    Tools::setTupleValues(&inlineTuple, int64_t(0), "dude");
    EXPECT_EQ(0, inlineTuple.getNonInlinedMemorySizeForPersistentTable());

    // Now check that an non-inlined schema returns the right thing.
    int32_t nonInlinableLength = UNINLINEABLE_OBJECT_LENGTH + 10000;
    ScopedTupleSchema nonInlinedSchema{Tools::buildSchema(ValueType::tBIGINT,
            std::make_pair(ValueType::tVARCHAR, nonInlinableLength))};
    tupleStorage.init(nonInlinedSchema.get(), pool);
    tupleStorage.allocateActiveTuple();
    TableTuple nonInlinedTuple = tupleStorage;

    NValue nonInlinedString = Tools::nvalueFromNative("123456");
    Tools::setTupleValues(&nonInlinedTuple, int64_t(0), nonInlinedString);
    EXPECT_EQ(nonInlinedString.getAllocationSizeForObjectInPersistentStorage(),
              nonInlinedTuple.getNonInlinedMemorySizeForPersistentTable());
}

TEST_F(TableTupleTest, HiddenColumns)
{
    UniqueEngine engine = UniqueEngineBuilder().build();

    TupleSchemaBuilder builder(2, 2);
    builder.setColumnAtIndex(0, ValueType::tBIGINT);
    builder.setColumnAtIndex(1, ValueType::tVARCHAR, 256);
    builder.setHiddenColumnAtIndex(0, HiddenColumn::XDCR_TIMESTAMP);
    builder.setHiddenColumnAtIndex(1, HiddenColumn::MIGRATE_TXN);
    ScopedTupleSchema schema(builder.build());

    StandAloneTupleStorage autoStorage(schema.get());
    TableTuple& tuple = autoStorage.tuple();

    NValue nvalVisibleBigint = ValueFactory::getBigIntValue(999);
    NValue nvalVisibleString = ValueFactory::getStringValue("catdog");
    NValue nvalHiddenBigint = ValueFactory::getBigIntValue(1066);

    tuple.setNValue(0, nvalVisibleBigint);
    tuple.setNValue(1, nvalVisibleString);
    tuple.setHiddenNValue(0, nvalHiddenBigint);
    tuple.setHiddenNValue(1, NValue::getNullValue(ValueType::tBIGINT));

    EXPECT_EQ(0, tuple.getNValue(0).compare(nvalVisibleBigint));
    EXPECT_EQ(0, tuple.getNValue(1).compare(nvalVisibleString));
    EXPECT_EQ(0, tuple.getHiddenNValue(0).compare(nvalHiddenBigint));
    EXPECT_EQ(0, tuple.getHiddenNValue(1).compare(NValue::getNullValue(ValueType::tBIGINT)));

    EXPECT_EQ(8 + (4 + 6) + 8, tuple.maxDRSerializationSize());

    tuple.setHiddenNValue(1, NValue::getNullValue(ValueType::tBIGINT));

    // The hidden string is null, takes 8 serialized byte
    EXPECT_EQ(8 + (4 + 6) + 8, tuple.maxDRSerializationSize());

    nvalVisibleString.free();
}

TEST_F(TableTupleTest, ToJsonArray)
{
    UniqueEngine engine = UniqueEngineBuilder().build();

    TupleSchemaBuilder builder(3, 2);
    builder.setColumnAtIndex(0, ValueType::tBIGINT);
    builder.setColumnAtIndex(1, ValueType::tVARCHAR, 256);
    builder.setColumnAtIndex(2, ValueType::tVARCHAR, 256);
    builder.setHiddenColumnAtIndex(0, HiddenColumn::XDCR_TIMESTAMP);
    builder.setHiddenColumnAtIndex(1, HiddenColumn::MIGRATE_TXN);
    ScopedTupleSchema schema(builder.build());

    StandAloneTupleStorage autoStorage(schema.get());
    TableTuple& tuple = autoStorage.tuple();

    NValue nvalVisibleBigint = ValueFactory::getBigIntValue(999);
    NValue nvalVisibleString = ValueFactory::getStringValue("数据库");
    NValue nvalHiddenBigint = ValueFactory::getBigIntValue(1066);

    tuple.setNValue(0, nvalVisibleBigint);
    tuple.setNValue(1, nvalVisibleString);
    tuple.setNValue(2, ValueFactory::getNullValue());
    tuple.setHiddenNValue(0, nvalHiddenBigint);
    tuple.setHiddenNValue(1, nvalHiddenBigint);

    EXPECT_EQ(0, strcmp(tuple.toJsonArray().c_str(), "[\"999\",\"\\u6570\\u636e\\u5e93\",\"null\"]"));

    nvalVisibleString.free();
}

TEST_F(TableTupleTest, VolatilePoolBackedTuple) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    Pool pool;

    // A schema with
    //    - one fixed size column
    //    - one inlined variable-length column
    //    - one non-inlined variable-length column
    ScopedTupleSchema schema{Tools::buildSchema(ValueType::tBIGINT,
                                                std::make_pair(ValueType::tVARCHAR, 12),
                                                std::make_pair(ValueType::tVARCHAR, 256))};
    PoolBackedTupleStorage poolBackedTuple;
    poolBackedTuple.init(schema.get(), &pool);
    poolBackedTuple.allocateActiveTuple();
    TableTuple &tuple = poolBackedTuple;

    Tools::setTupleValues(&tuple, int64_t(0), "foo", "foo bar");

    // Pool-backed tuples are used as "scratch areas" so their data is
    // frequently mutated.  NValues that reference them could have
    // their data changed.
    //
    // Non-inlined data is not volatile though.
    ASSERT_TRUE(tuple.inlinedDataIsVolatile());
    ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());

    NValue nv = tuple.getNValue(0);
    ASSERT_FALSE(nv.getVolatile());

    nv = tuple.getNValue(1);
    ASSERT_TRUE(nv.getVolatile());

    // After the NValue is made to be non-inlined (copied to temp string pool)
    // it is no longer volatile
    nv.allocateObjectFromPool();
    ASSERT_FALSE(nv.getVolatile());

    nv = tuple.getNValue(2);
    ASSERT_FALSE(nv.getVolatile());
}

TEST_F(TableTupleTest, VolatileStandAloneTuple) {
    UniqueEngine engine = UniqueEngineBuilder().build();

    // A schema with
    //    - one fixed size column
    //    - one inlined variable-length column
    //    - one non-inlined variable-length column
    ScopedTupleSchema schema{Tools::buildSchema(ValueType::tBIGINT,
                                                std::make_pair(ValueType::tVARCHAR, 12),
                                                std::make_pair(ValueType::tVARCHAR, 256))};
    StandAloneTupleStorage standAloneTuple{schema.get()};
    TableTuple tuple = standAloneTuple.tuple();
    Tools::setTupleValues(&tuple, int64_t(0), "foo", "foo bar");

    // Stand alone tuples are similar to pool-backed tuples.
    ASSERT_TRUE(tuple.inlinedDataIsVolatile());
    ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());

    NValue nv = tuple.getNValue(0);
    ASSERT_FALSE(nv.getVolatile());

    nv = tuple.getNValue(1);
    ASSERT_TRUE(nv.getVolatile());

    nv = tuple.getNValue(2);
    ASSERT_FALSE(nv.getVolatile());
}

TEST_F(TableTupleTest, VolatileTempTuple) {
    UniqueEngine engine = UniqueEngineBuilder().build();

    // A schema with
    //    - one fixed-length column
    //    - one inlined variable-length column
    //    - one non-inlined variable-length column
    TupleSchema *schema = Tools::buildSchema(ValueType::tBIGINT,
                                             std::make_pair(ValueType::tVARCHAR, 12),
                                             std::make_pair(ValueType::tVARCHAR, 256));
    std::vector<std::string> columnNames{"id", "inlined", "noninlined"};
    std::unique_ptr<Table> table{TableFactory::buildTempTable("T",
                                                              schema,
                                                              columnNames,
                                                              NULL)};
    TableTuple tuple = table->tempTuple();
    Tools::setTupleValues(&tuple, int64_t(0), "foo", "foo bar");

    ASSERT_TRUE(tuple.inlinedDataIsVolatile());
    ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());

    NValue nv = tuple.getNValue(0);
    ASSERT_FALSE(nv.getVolatile());

    nv = tuple.getNValue(1);
    ASSERT_TRUE(nv.getVolatile());

    nv = tuple.getNValue(2);
    ASSERT_FALSE(nv.getVolatile());

    table->insertTuple(tuple);
    TableIterator it = table->iterator();
    TableTuple iterTuple{schema};
    while (it.next(iterTuple)) {
        // Regular, TupleBlock-backed tuples are never volatile.
        ASSERT_FALSE(iterTuple.inlinedDataIsVolatile());
        ASSERT_FALSE(iterTuple.nonInlinedDataIsVolatile());

        nv = iterTuple.getNValue(0);
        ASSERT_FALSE(nv.getVolatile());

        nv = iterTuple.getNValue(1);
        ASSERT_FALSE(nv.getVolatile());

        nv = iterTuple.getNValue(2);
        ASSERT_FALSE(nv.getVolatile());
    }
}

TEST_F(TableTupleTest, VolatileTempTuplePersistent) {
    UniqueEngine engine = UniqueEngineBuilder().build();

    // A schema with
    //    - one fixed-length column
    //    - one inlined variable-length column
    //    - one non-inlined variable-length column
    TupleSchema *schema = Tools::buildSchema(ValueType::tBIGINT,
                                             std::make_pair(ValueType::tVARCHAR, 12),
                                             std::make_pair(ValueType::tVARCHAR, 256));
    std::vector<std::string> columnNames{"id", "inlined", "noninlined"};
    char signature[20];
    std::unique_ptr<Table> table{TableFactory::getPersistentTable(0,
                                                                  "perstbl",
                                                                  schema,
                                                                  columnNames,
                                                                  signature)};
    TableTuple tuple = table->tempTuple();
    Tools::setTupleValues(&tuple, int64_t(0), "foo", "foo bar");

    ASSERT_TRUE(tuple.inlinedDataIsVolatile());
    ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());

    NValue nv = tuple.getNValue(0);
    ASSERT_FALSE(nv.getVolatile());

    nv = tuple.getNValue(1);
    ASSERT_TRUE(nv.getVolatile());

    nv = tuple.getNValue(2);
    ASSERT_FALSE(nv.getVolatile());

    table->insertTuple(tuple);
    TableIterator it = table->iterator();
    TableTuple iterTuple{schema};
    while (it.next(iterTuple)) {
        // Regular, TupleBlock-backed tuples are never volatile.
        ASSERT_FALSE(iterTuple.inlinedDataIsVolatile());
        ASSERT_FALSE(iterTuple.nonInlinedDataIsVolatile());

        nv = iterTuple.getNValue(0);
        ASSERT_FALSE(nv.getVolatile());

        nv = iterTuple.getNValue(1);
        ASSERT_FALSE(nv.getVolatile());

        nv = iterTuple.getNValue(2);
        ASSERT_FALSE(nv.getVolatile());
    }
}

TEST_F(TableTupleTest, HeaderDefaults) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    Pool pool;

    // A schema with
    //    - one fixed size column
    //    - one inlined variable-length column
    //    - one non-inlined variable-length column
    ScopedTupleSchema schema{Tools::buildSchema(ValueType::tBIGINT,
                                                std::make_pair(ValueType::tVARCHAR, 12),
                                                std::make_pair(ValueType::tVARCHAR, 256))};
    char *storage = static_cast<char*>(pool.allocateZeroes(schema->tupleLength() + TUPLE_HEADER_SIZE));
    TableTuple theTuple{storage, schema.get()};

    ASSERT_FALSE(theTuple.isActive());
    ASSERT_FALSE(theTuple.isDirty());
    ASSERT_FALSE(theTuple.isPendingDelete());
    ASSERT_FALSE(theTuple.isPendingDeleteOnUndoRelease());
    ASSERT_TRUE(theTuple.inlinedDataIsVolatile());
    ASSERT_FALSE(theTuple.nonInlinedDataIsVolatile());

    theTuple.resetHeader();

    ASSERT_FALSE(theTuple.isActive());
    ASSERT_FALSE(theTuple.isDirty());
    ASSERT_FALSE(theTuple.isPendingDelete());
    ASSERT_FALSE(theTuple.isPendingDeleteOnUndoRelease());
    ASSERT_TRUE(theTuple.inlinedDataIsVolatile());
    ASSERT_FALSE(theTuple.nonInlinedDataIsVolatile());
}

TEST_F(TableTupleTest, HiddenColumnSerialization) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    Pool pool;

    TupleSchemaBuilder schemaBuilder(3, 2);
    schemaBuilder.setColumnAtIndex(0, ValueType::tBIGINT);
    schemaBuilder.setColumnAtIndex(1, ValueType::tVARCHAR, 60);
    schemaBuilder.setColumnAtIndex(2, ValueType::tINTEGER);
    schemaBuilder.setHiddenColumnAtIndex(0, HiddenColumn::MIGRATE_TXN);
    schemaBuilder.setHiddenColumnAtIndex(1, HiddenColumn::XDCR_TIMESTAMP);

    ScopedTupleSchema schema(schemaBuilder.build());
    char *storage = static_cast<char*>(pool.allocateZeroes(schema->tupleLength() + TUPLE_HEADER_SIZE));
    TableTuple tuple(storage, schema.get());

    NValue nvalVisibleBigint = ValueFactory::getBigIntValue(999);
    NValue nvalVisibleString = ValueFactory::getStringValue("catdog");
    NValue nvalVisibleInt = ValueFactory::getIntegerValue(1000);
    NValue nvalHiddenMigrate = ValueFactory::getBigIntValue(1066);
    NValue nvalHiddenXdcr = ValueFactory::getBigIntValue(1067);

    tuple.setNValue(0, nvalVisibleBigint);
    tuple.setNValue(1, nvalVisibleString);
    tuple.setNValue(2, nvalVisibleInt);
    tuple.setHiddenNValue(0, nvalHiddenMigrate);
    tuple.setHiddenNValue(1, nvalHiddenXdcr);

    char serialized[128];
    ReferenceSerializeOutput unfilteredOutput(serialized, sizeof(serialized));
    HiddenColumnFilter filterNone = HiddenColumnFilter::create(HiddenColumnFilter::NONE, schema.get());
    tuple.serializeTo(unfilteredOutput, &filterNone);

    // Reserved size + 3 bigints + string + integer
    int unfilteredSize = 4 + NValue::getTupleStorageSize(ValueType::tBIGINT) * 3 + (4 + 6) +
            NValue::getTupleStorageSize(ValueType::tINTEGER);
    ASSERT_EQ(unfilteredSize, unfilteredOutput.size());

    // Validate serialized contents has all columns
    ReferenceSerializeInputBE unfilteredDeserailizer(serialized, unfilteredSize);
    ASSERT_EQ(unfilteredSize - 4, unfilteredDeserailizer.readInt());
    ASSERT_EQ(0, nvalVisibleBigint.compare(ValueFactory::getBigIntValue(unfilteredDeserailizer.readLong())));
    ASSERT_EQ(0, nvalVisibleString.compare(ValueFactory::getStringValue(unfilteredDeserailizer.readTextString(), &pool)));
    ASSERT_EQ(0, nvalVisibleInt.compare(ValueFactory::getBigIntValue(unfilteredDeserailizer.readInt())));
    ASSERT_EQ(0, nvalHiddenMigrate.compare(ValueFactory::getBigIntValue(unfilteredDeserailizer.readLong())));
    ASSERT_EQ(0, nvalHiddenXdcr.compare(ValueFactory::getBigIntValue(unfilteredDeserailizer.readLong())));
    ASSERT_FALSE(unfilteredDeserailizer.hasRemaining());

    ::memset(serialized, '\0', sizeof(serialized));
    ReferenceSerializeOutput filteredOutput(serialized, sizeof(serialized));

    HiddenColumnFilter filterMigrate = HiddenColumnFilter::create(HiddenColumnFilter::EXCLUDE_MIGRATE, schema.get());
    tuple.serializeTo(filteredOutput, &filterMigrate);

    int filteredSize = unfilteredSize - NValue::getTupleStorageSize(ValueType::tBIGINT);
    ASSERT_EQ(filteredSize, filteredOutput.size());

    // Validate serialized contents has everything except the hidden migrate column
    ReferenceSerializeInputBE filteredDeserailizer(serialized, filteredSize);
    ASSERT_EQ(filteredSize - 4, filteredDeserailizer.readInt());
    ASSERT_EQ(0, nvalVisibleBigint.compare(ValueFactory::getBigIntValue(filteredDeserailizer.readLong())));
    ASSERT_EQ(0, nvalVisibleString.compare(ValueFactory::getStringValue(filteredDeserailizer.readTextString(), &pool)));
    ASSERT_EQ(0, nvalVisibleInt.compare(ValueFactory::getBigIntValue(filteredDeserailizer.readInt())));
    ASSERT_EQ(0, nvalHiddenXdcr.compare(ValueFactory::getBigIntValue(filteredDeserailizer.readLong())));
    ASSERT_FALSE(unfilteredDeserailizer.hasRemaining());

    nvalVisibleString.free();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
