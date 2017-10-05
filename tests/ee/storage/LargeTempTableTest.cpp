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

#include "test_utils/UniqueEngine.hpp"

#include "common/LargeTempTableBlockCache.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

#include "test_utils/Tools.hpp"

using namespace voltdb;

class LargeTempTableTest : public Test {
protected:

    void assertTupleValuesEqualHelper(TableTuple* tuple, int index) {
        ASSERT_EQ(tuple->getSchema()->columnCount(), index);
    }

    template<typename T, typename ...Args>
    void assertTupleValuesEqualHelper(TableTuple* tuple, int index, T expected, Args... args) {
        NValue actualNVal = tuple->getNValue(index);
        NValue expectedNVal = Tools::nvalueFromNative(expected);

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
        std::unique_ptr<char[]> storage = block->releaseData();
        m_map.insert(std::make_pair(blockId, std::move(storage)));
        return true;
    }

    bool loadLargeTempTableBlock(int64_t blockId, LargeTempTableBlock* block) {
        auto it = m_map.find(blockId);
        std::unique_ptr<char[]> storage;
        storage.swap(it->second);
        block->setData(std::move(storage));
        m_map.erase(blockId);
        return true;
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

    ~LTTTopend() {
        assert(m_map.size() == 0);
    }

private:

    std::map<int64_t, std::unique_ptr<char[]>> m_map;
};


TEST_F(LargeTempTableTest, Basic) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    TupleSchema* schema = Tools::buildSchema(VALUE_TYPE_BIGINT,
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
        Tools::setTupleValues(&tuple, pkVals[i], floatVals[i], textVals[i]);
        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    ltt->finishInserts();

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    {
        TableIterator iter = ltt->iterator();
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
    UniqueEngine engine = UniqueEngineBuilder().build();
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    ASSERT_EQ(0, lttBlockCache->totalBlockCount());

    TupleSchema* schema = Tools::buildSchema(VALUE_TYPE_BIGINT,
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
        Tools::setTupleValues(&tuple,
                              i,
                              0.5 * i,
                              0.5 * i + 1,
                              0.5 * i + 2,
                              Tools::toDec(0.5 * i),
                              Tools::toDec(0.5 * i + 1),
                              Tools::toDec(0.5 * i + 2),
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
        TableIterator iter = ltt->iterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            std::string text(15, 'a' + (i % 26));
            assertTupleValuesEqual(&iterTuple,
                                   i,
                                   0.5 * i,
                                   0.5 * i + 1,
                                   0.5 * i + 2,
                                   Tools::toDec(0.5 * i),
                                   Tools::toDec(0.5 * i + 1),
                                   Tools::toDec(0.5 * i + 2),
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
    std::unique_ptr<Topend> topend{new LTTTopend()};
#ifndef MEMCHECK
    int64_t tempTableMemoryLimitInBytes = 400000;
#else
    int64_t tempTableMemoryLimitInBytes = 80000;
#endif
    UniqueEngine engine = UniqueEngineBuilder()
        .setTopend(std::move(topend))
        .setTempTableMemoryLimit(tempTableMemoryLimitInBytes)
        .build();
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    TupleSchema* schema = Tools::buildSchema(VALUE_TYPE_BIGINT,
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
        Tools::setTupleValues(&tuple,
                              i,
                              0.5 * i,
                              0.5 * i + 1,
                              0.5 * i + 2,
                              Tools::toDec(0.5 * i),
                              Tools::toDec(0.5 * i + 1),
                              Tools::toDec(0.5 * i + 2),
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
        TableIterator iter = ltt->iterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            std::string text(15, 'a' + (i % 26));
            assertTupleValuesEqual(&iterTuple,
                                   i,
                                   0.5 * i,
                                   0.5 * i + 1,
                                   0.5 * i + 2,
                                   Tools::toDec(0.5 * i),
                                   Tools::toDec(0.5 * i + 1),
                                   Tools::toDec(0.5 * i + 2),
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

    LTTTopend* theTopend = static_cast<LTTTopend*>(ExecutorContext::getExecutorContext()->getTopend());
    ASSERT_EQ(0, theTopend->storedBlockCount());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
