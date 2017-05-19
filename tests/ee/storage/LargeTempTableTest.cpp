/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
#include <vector>

#include "harness.h"

#include "common/LargeTempTableBlockCache.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "storage/LargeTableIterator.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

template<typename T>
NValue nvalueFromNative(T val);

template<>
NValue nvalueFromNative(int64_t val) {
    return ValueFactory::getBigIntValue(val);
}

template<>
NValue nvalueFromNative(int val) {
    return nvalueFromNative(static_cast<int64_t>(val));
}

template<>
NValue nvalueFromNative(std::string val) {
    return ValueFactory::getTempStringValue(val);
}

template<>
NValue nvalueFromNative(const char* val) {
    return ValueFactory::getTempStringValue(val, strlen(val));
}

template<>
NValue nvalueFromNative(double val) {
    return ValueFactory::getDoubleValue(val);
}

NValue toDecimal(double val) {
    return ValueFactory::getDecimalValue(val);
}

template<>
NValue nvalueFromNative(NValue nval) {
    return nval;
}

void setTupleValuesHelper(TableTuple* tuple, int index) {
    assert(tuple->getSchema()->columnCount() == index);
}

template<typename T, typename ... Args>
void setTupleValuesHelper(TableTuple* tuple, int index, T arg, Args... args) {
    tuple->setNValue(index, nvalueFromNative(arg));
    setTupleValuesHelper(tuple, index + 1, args...);
}

template<typename ... Args>
void setTupleValues(TableTuple* tuple, Args... args) {
    setTupleValuesHelper(tuple, 0, args...);
}


void buildSchemaHelper(std::vector<ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes) {
    return;
}

template<typename... Args>
void buildSchemaHelper(std::vector<ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       ValueType valueType,
                       Args... args);
template<typename... Args>
void buildSchemaHelper(std::vector<ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::pair<ValueType, int> typeAndSize,
                       Args... args);

template<typename... Args>
void buildSchemaHelper(std::vector<ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       ValueType valueType,
                       Args... args) {
    assert(! isVariableLengthType(valueType));
    columnTypes->push_back(valueType);
    columnSizes->push_back(NValue::getTupleStorageSize(valueType));
    buildSchemaHelper(columnTypes, columnSizes, args...);
}

template<typename... Args>
void buildSchemaHelper(std::vector<ValueType>* columnTypes,
                       std::vector<int32_t>* columnSizes,
                       std::pair<ValueType, int> typeAndSize,
                       Args... args) {
    assert(isVariableLengthType(typeAndSize.first));
    columnTypes->push_back(typeAndSize.first);
    columnSizes->push_back(typeAndSize.second);
    buildSchemaHelper(columnTypes, columnSizes, args...);
}

template<typename... Args>
TupleSchema* buildSchema(Args... args) {
    std::vector<ValueType> columnTypes;
    std::vector<int32_t> columnSizes;

    buildSchemaHelper(&columnTypes, &columnSizes, args...);

    std::vector<bool> allowNull(columnTypes.size(), true);
    std::vector<bool> inBytes(columnTypes.size(), false);

    return TupleSchema::createTupleSchema(columnTypes, columnSizes, allowNull, inBytes);
}


class LargeTempTableTest : public Test {
protected:

    void assertTupleValuesEqualHelper(TableTuple* tuple, int index) {
        assert(tuple->getSchema()->columnCount() == index);
    }

    template<typename T, typename ...Args>
    void assertTupleValuesEqualHelper(TableTuple* tuple, int index, T expected, Args... args) {
        NValue actualNVal = tuple->getNValue(index);
        NValue expectedNVal = nvalueFromNative(expected);

        ASSERT_EQ(ValuePeeker::peekValueType(expectedNVal), ValuePeeker::peekValueType(actualNVal));
        ASSERT_EQ(0, expectedNVal.compare(actualNVal));

        assertTupleValuesEqualHelper(tuple, index + 1, args...);
    }

    template<typename... Args>
    void assertTupleValuesEqual(TableTuple* tuple, Args... expectedVals) {
        assertTupleValuesEqualHelper(tuple, 0, expectedVals...);
    }
};


class LTTTopend : public voltdb::DummyTopend {
public:

    bool storeLargeTempTableBlock(int64_t blockId, LargeTempTableBlock* block) {
        m_map.emplace(std::make_pair(blockId, StoredBlock(block->releasePool(), block->releaseBlock())));
        return true;
    }

    std::unique_ptr<LargeTempTableBlock> loadLargeTempTableBlock(int64_t blockId) {
        auto it = m_map.find(blockId);
        std::unique_ptr<LargeTempTableBlock> block = it->second.toLargeTempTableBlock();
        m_map.erase(blockId);
        return block;
    }

    bool releaseLargeTempTableBlock(int64_t blockId) {
        auto it = m_map.find(blockId);
        if (it == m_map.end()) {
            assert(false);
            return false;
        }

        m_map.erase(blockId);
        return true;
    }

    size_t storedBlockCount() const {
        return m_map.size();
    }

private:

    struct StoredBlock {
        StoredBlock(std::unique_ptr<Pool> pool, TBPtr tbp)
            : m_pool(std::move(pool))
            , m_tbp(tbp)
        {
        }

        std::unique_ptr<LargeTempTableBlock> toLargeTempTableBlock() {
            return std::unique_ptr<LargeTempTableBlock>(new LargeTempTableBlock(std::move(m_pool), m_tbp));
        }

    private:
        std::unique_ptr<Pool> m_pool;
        TBPtr m_tbp;
    };

    std::map<int64_t, StoredBlock> m_map;
};


TEST_F(LargeTempTableTest, Basic) {

    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    TupleSchema* schema = buildSchema(VALUE_TYPE_BIGINT,
                                      VALUE_TYPE_DOUBLE,
                                      std::make_pair(VALUE_TYPE_VARCHAR, 128));

    std::vector<std::string> names{"pk", "val", "text"};

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);

    ltt->incrementRefcount();

    StandAloneTupleStorage tupleWrapper(schema);
    TableTuple tuple = tupleWrapper.tuple();

    std::vector<int> pkVals{66, 67, 68};
    std::vector<double> floatVals{3.14, 6.28, 7.77};
    std::vector<std::string> textVals{"foo", "bar", "baz"};

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());
    for (int i = 0; i < pkVals.size(); ++i) {
        setTupleValues(&tuple, pkVals[i], floatVals[i], textVals[i]);
        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    ltt->finishInserts();

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    {
        LargeTableIterator iter = ltt->largeIterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            assertTupleValuesEqual(&iterTuple, pkVals[i], floatVals[i], textVals[i]);
            ++i;
        }

        ASSERT_EQ(pkVals.size(), i);
    }



    ltt->decrementRefcount();

    ASSERT_EQ(0, lttBlockCache->totalBlockCount());
    ASSERT_EQ(0, lttBlockCache->allocatedMemory());
}

TEST_F(LargeTempTableTest, MultiBlock) {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    ASSERT_EQ(0, lttBlockCache->totalBlockCount());

    TupleSchema* schema = buildSchema(VALUE_TYPE_BIGINT,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DECIMAL,
                                      VALUE_TYPE_DECIMAL,
                                      VALUE_TYPE_DECIMAL,
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15),
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15),
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15));

    std::vector<std::string> names{
        "pk",
        "val0",
        "val1",
        "val2",
        "dec0",
        "dec1",
        "dec2",
        "text0",
        "text1",
        "text2"
    };

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);
    ltt->incrementRefcount();

    StandAloneTupleStorage tupleWrapper(schema);
    TableTuple tuple = tupleWrapper.tuple();
    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    const int NUM_TUPLES = 500;
    for (int i = 0; i < NUM_TUPLES; ++i) {
        std::string text(15, 'a' + (i % 26));
        setTupleValues(&tuple,
                       i,
                       0.5 * i,
                       0.5 * i + 1,
                       0.5 * i + 2,
                       toDecimal(0.5 * i),
                       toDecimal(0.5 * i + 1),
                       toDecimal(0.5 * i + 2),
                       text,
                       text,
                       text);

        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    ltt->finishInserts();

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

#ifndef MEMCHECK
    ASSERT_EQ(2, ltt->allocatedBlockCount());
#else
    ASSERT_EQ(NUM_TUPLES, ltt->allocatedBlockCount());
#endif

    {
        LargeTableIterator iter = ltt->largeIterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            std::string text(15, 'a' + (i % 26));
            assertTupleValuesEqual(&iterTuple,
                                   i,
                                   0.5 * i,
                                   0.5 * i + 1,
                                   0.5 * i + 2,
                                   toDecimal(0.5 * i),
                                   toDecimal(0.5 * i + 1),
                                   toDecimal(0.5 * i + 2),
                                   text,
                                   text,
                                   text);
            ++i;
        }

        ASSERT_EQ(500, i);
    }

    ltt->decrementRefcount();

    ASSERT_EQ(0, lttBlockCache->totalBlockCount());
    ASSERT_EQ(0, lttBlockCache->allocatedMemory());
}

TEST_F(LargeTempTableTest, OverflowCache) {
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

#ifndef MEMCHECK
    voltdb::LargeTempTableBlockCache::CACHE_SIZE_IN_BYTES() = 400000;
#else
    voltdb::LargeTempTableBlockCache::CACHE_SIZE_IN_BYTES() = 80000;
#endif

    TupleSchema* schema = buildSchema(VALUE_TYPE_BIGINT,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DOUBLE,
                                      VALUE_TYPE_DECIMAL,
                                      VALUE_TYPE_DECIMAL,
                                      VALUE_TYPE_DECIMAL,
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15),
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15),
                                      std::make_pair(VALUE_TYPE_VARCHAR, 15));

    std::vector<std::string> names{
        "pk",
        "val0",
        "val1",
        "val2",
        "dec0",
        "dec1",
        "dec2",
        "text0",
        "text1",
        "text2"
    };

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);
    ltt->incrementRefcount();

    StandAloneTupleStorage tupleWrapper(schema);
    TableTuple tuple = tupleWrapper.tuple();
    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    const int NUM_TUPLES = 1500;
    for (int i = 0; i < NUM_TUPLES; ++i) {
        std::string text(15, 'a' + (i % 26));
        setTupleValues(&tuple,
                       i,
                       0.5 * i,
                       0.5 * i + 1,
                       0.5 * i + 2,
                       toDecimal(0.5 * i),
                       toDecimal(0.5 * i + 1),
                       toDecimal(0.5 * i + 2),
                       text,
                       text,
                       text);

        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    // Notify that we're done inserting so last block can be
    // unpinned.
    ltt->finishInserts();

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

#ifndef MEMCHECK
    // The table uses 4 blocks, but only 2 at a time can be cached.
    ASSERT_EQ(4, lttBlockCache->totalBlockCount());
    ASSERT_EQ(2, lttBlockCache->residentBlockCount());
#else
    ASSERT_EQ(NUM_TUPLES, lttBlockCache->totalBlockCount());
    ASSERT_EQ(303, lttBlockCache->residentBlockCount());
#endif

    {
        LargeTableIterator iter = ltt->largeIterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            std::string text(15, 'a' + (i % 26));
            assertTupleValuesEqual(&iterTuple,
                                   i,
                                   0.5 * i,
                                   0.5 * i + 1,
                                   0.5 * i + 2,
                                   toDecimal(0.5 * i),
                                   toDecimal(0.5 * i + 1),
                                   toDecimal(0.5 * i + 2),
                                   text,
                                   text,
                                   text);
            ++i;
        }

        ASSERT_EQ(NUM_TUPLES, i);
    }

    ltt->decrementRefcount();

    ASSERT_EQ(0, lttBlockCache->totalBlockCount());
    ASSERT_EQ(0, lttBlockCache->allocatedMemory());

    LTTTopend* topend = static_cast<LTTTopend*>(ExecutorContext::getExecutorContext()->getTopend());
    ASSERT_EQ(0, topend->storedBlockCount());
}

int main() {

    assert (voltdb::ExecutorContext::getExecutorContext() == NULL);

    ThreadLocalPool tlPool;
    boost::scoped_ptr<voltdb::Pool> testPool(new voltdb::Pool());
    voltdb::UndoQuantum* wantNoQuantum = NULL;
    std::unique_ptr<voltdb::Topend> topend(new LTTTopend());
    boost::scoped_ptr<voltdb::ExecutorContext>
        executorContext(new voltdb::ExecutorContext(0,              // siteId
                                                    0,              // partitionId
                                                    wantNoQuantum,  // undoQuantum
                                                    topend.get(),   // topend
                                                    testPool.get(), // tempStringPool
                                                    NULL,           // engine
                                                    "",             // hostname
                                                    0,              // hostId
                                                    NULL,           // drTupleStream
                                                    NULL,           // drReplicatedStream
                                                    0));            // drClusterId

    return TestSuite::globalInstance()->runAll();
}
