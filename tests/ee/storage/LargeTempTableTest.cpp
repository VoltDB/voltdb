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

#include "test_utils/LargeTempTableTopend.hpp"
#include "test_utils/UniqueEngine.hpp"
#include "test_utils/ScopedTupleSchema.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/TupleComparingTest.hpp"

#include "common/LargeTempTableBlockCache.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

class LargeTempTableTest : public TupleComparingTest {
};

// Use boost::optional to represent null values
boost::optional<std::string> getStringValue(size_t maxLen, int selector) {
    // generate some "interesting" values:
    // - NULL
    // - empty string
    // - short string
    // - long string
    int key = selector % 4;
    switch (key) {
    case 0:
        return boost::optional<std::string>(); // NULL
    case 1:
        return boost::optional<std::string>(std::string(""));
    case 2:
        return boost::optional<std::string>(std::string(maxLen / 2, 'a'));
    case 3:
    default:
        return boost::optional<std::string>(std::string(maxLen, 'z'));
    }
}

TEST_F(LargeTempTableTest, Basic) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    TupleSchema* schema = Tools::buildSchema(VALUE_TYPE_BIGINT,
                                             VALUE_TYPE_DOUBLE,
                                             std::make_pair(VALUE_TYPE_VARCHAR, 15),
                                             std::make_pair(VALUE_TYPE_VARCHAR, 128));
    std::vector<std::string> columnNames{
        "pk", "val", "inline_text", "noninline_text"
    };
    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        columnNames);

    ltt->incrementRefcount();

    TableTuple tuple = ltt->tempTuple();

    // Temp tuple for large temp tables is like the temp tuple for
    // normal temp tables and persistent tables:
    //   - inlined, variable-length data is volatile
    //   - non-inlined, variable-length data is in the temp string pool,
    //     which is not volatile.
    ASSERT_TRUE(tuple.inlinedDataIsVolatile());
    ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());

    std::vector<int64_t> pkVals{66, 67, 68};
    std::vector<double> floatVals{3.14, 6.28, 7.77};
    std::vector<std::string> inlineTextVals{"foo", "bar", "baz"};
    std::vector<std::string> nonInlineTextVals{"ffoo", "bbar", "bbaz"};

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());
    for (int i = 0; i < pkVals.size(); ++i) {
        Tools::setTupleValues(&tuple, pkVals[i], floatVals[i], inlineTextVals[i], nonInlineTextVals[i]);
        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    try {
        TableIterator it = ltt->iterator();
        ASSERT_TRUE_WITH_MESSAGE(false, "Expected release of pinned block to fail");
    }
    catch (const SerializableEEException &exc) {
        ASSERT_NE(std::string::npos, exc.message().find("Attempt to iterate over large temp table before finishInserts() is called"));
    }

    ltt->finishInserts();

    // finishInserts is idempotent and may be called multiple times
    ltt->finishInserts();

    try {
        Tools::setTupleValues(&tuple, int64_t(-1), 3.14, "dino", "ddino");
        ltt->insertTuple(tuple);
        ASSERT_TRUE_WITH_MESSAGE(false, "Expected insertTuple() to fail after finishInserts() called");
    }
    catch (const SerializableEEException& exc) {
        ASSERT_NE(std::string::npos, exc.message().find("Attempt to insert after finishInserts() called"));
    }

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    {
        TableIterator iter = ltt->iterator();
        TableTuple iterTuple(ltt->schema());
        int i = 0;
        while (iter.next(iterTuple)) {
            if (! assertTupleValuesEqual(&iterTuple,
                                         pkVals[i],
                                         floatVals[i],
                                         inlineTextVals[i],
                                         nonInlineTextVals[i])) {
                break;
            }
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

    const int INLINE_LEN = 15;
    const int NONINLINE_LEN = 50000;

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
        "text2",
        "bigtext"
    };

    TupleSchema* schema = Tools::buildSchema(
        //                                       status byte: 1
        VALUE_TYPE_BIGINT,                                //  8
        VALUE_TYPE_DOUBLE,                                //  8
        VALUE_TYPE_DOUBLE,                                //  8
        VALUE_TYPE_DOUBLE,                                //  8
        VALUE_TYPE_DECIMAL,                               // 16
        VALUE_TYPE_DECIMAL,                               // 16
        VALUE_TYPE_DECIMAL,                               // 16
        std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),   // 61
        std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),   // 61
        std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),   // 61
        std::make_pair(VALUE_TYPE_VARCHAR, NONINLINE_LEN)); //  8 (pointer to non-inlined)
    // --> Tuple length is 272 bytes (not counting non-inlined data)

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);
    ltt->incrementRefcount();

    TableTuple tuple = ltt->tempTuple();
    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    const int NUM_TUPLES = 500;
    // Attempt to insert enough rows so that we have more than one
    // block in this table.
    //   inline data:
    //                 136000    (500 * 272)
    //
    // Four kinds of non-inlined strings:
    //   NULL               0    (125 * 0)
    //   empty string    1500    (125 * 12, StringRef and length prefix)
    //   half string  3126500    (125 * (25000 + 12))
    //   whole string 6251500    (125 * (50000 + 12))
    //
    // Total -->      9515500
    //
    // LTT blocks are 8MB so this data should use two blocks.
    for (int64_t i = 0; i < NUM_TUPLES; ++i) {
        Tools::setTupleValues(&tuple,
                              i,
                              0.5 * i,
                              0.5 * i + 1,
                              0.5 * i + 2,
                              Tools::toDec(0.5 * i),
                              Tools::toDec(0.5 * i + 1),
                              Tools::toDec(0.5 * i + 2),
                              getStringValue(INLINE_LEN, i),
                              getStringValue(INLINE_LEN, i + 1),
                              getStringValue(INLINE_LEN, i + 2),
                              getStringValue(NONINLINE_LEN, i));
        ASSERT_TRUE(tuple.inlinedDataIsVolatile());
        ASSERT_FALSE(tuple.nonInlinedDataIsVolatile());
        ltt->insertTuple(tuple);
    }

    // The block we were inserting into will be pinned
    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    // Indicate that we are done inserting...
    ltt->finishInserts();

    // Block is now unpinned
    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    ASSERT_EQ(2, ltt->allocatedBlockCount());

    {
        TableIterator iter = ltt->iterator();
        TableTuple iterTuple(ltt->schema());
        int64_t i = 0;
        while (iter.next(iterTuple)) {
            boost::optional<std::string> inlineStr0 = getStringValue(INLINE_LEN, i);
            boost::optional<std::string> inlineStr1 = getStringValue(INLINE_LEN, i + 1);
            boost::optional<std::string> inlineStr2 = getStringValue(INLINE_LEN, i + 2);
            boost::optional<std::string> nonInlineStr = getStringValue(NONINLINE_LEN, i);

            assertTupleValuesEqual(&iterTuple,
                                   i,
                                   0.5 * i,
                                   0.5 * i + 1,
                                   0.5 * i + 2,
                                   Tools::toDec(0.5 * i),
                                   Tools::toDec(0.5 * i + 1),
                                   Tools::toDec(0.5 * i + 2),
                                   inlineStr0,
                                   inlineStr1,
                                   inlineStr2,
                                   nonInlineStr);
            // Check volatility of inserted values
            NValue nv = iterTuple.getNValue(0);
            ASSERT_FALSE(nv.getVolatile()); // bigint

            nv = iterTuple.getNValue(7); // inlined varchar
            ASSERT_TRUE(nv.getVolatile());

            // It can be made non-volatile by allocating in a pool:
            nv.allocateObjectFromPool();
            ASSERT_FALSE(nv.getVolatile());
            ASSERT_EQ(0, Tools::nvalueCompare(inlineStr0, nv));

            nv = iterTuple.getNValue(10); // non-inlined varchar
            ASSERT_TRUE(nv.getVolatile());

            // It can be made non-volatile by allocating in a pool:
            nv.allocateObjectFromPool();
            ASSERT_FALSE(nv.getVolatile());
            ASSERT_EQ(0, Tools::nvalueCompare(nonInlineStr, nv));

            ++i;
        }

        ASSERT_EQ(500, i);
    }

    ltt->decrementRefcount();

    ASSERT_EQ(0, lttBlockCache->totalBlockCount());
    ASSERT_EQ(0, lttBlockCache->allocatedMemory());
}

TEST_F(LargeTempTableTest, OverflowCache) {
    std::unique_ptr<Topend> topend{new LargeTempTableTopend()};

    // Define an LTT block cache that can hold only two blocks:
    int64_t tempTableMemoryLimitInBytes = 16 * 1024 * 1024;
    UniqueEngine engine = UniqueEngineBuilder()
        .setTopend(std::move(topend))
        .setTempTableMemoryLimit(tempTableMemoryLimitInBytes)
        .build();
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

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
        "text2",
        "bigtext"
    };

    const int INLINE_LEN = 15;
    const int NONINLINE_LEN = 50000;

    TupleSchema* schema = Tools::buildSchema(VALUE_TYPE_BIGINT,
                                             VALUE_TYPE_DOUBLE,
                                             VALUE_TYPE_DOUBLE,
                                             VALUE_TYPE_DOUBLE,
                                             VALUE_TYPE_DECIMAL,
                                             VALUE_TYPE_DECIMAL,
                                             VALUE_TYPE_DECIMAL,
                                             std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),
                                             std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),
                                             std::make_pair(VALUE_TYPE_VARCHAR, INLINE_LEN),
                                             std::make_pair(VALUE_TYPE_VARCHAR, NONINLINE_LEN));

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);
    ltt->incrementRefcount();

    StandAloneTupleStorage tupleWrapper(schema);
    TableTuple tuple = tupleWrapper.tuple();
    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    const int NUM_TUPLES = 1500;
    // This will create around 28MB of data, using the accounting from
    // the MultiBlock test, above.
    // (4 total blocks with the last around half full)
    for (int64_t i = 0; i < NUM_TUPLES; ++i) {
        Tools::setTupleValues(&tuple,
                              i,
                              0.5 * i,
                              0.5 * i + 1,
                              0.5 * i + 2,
                              Tools::toDec(0.5 * i),
                              Tools::toDec(0.5 * i + 1),
                              Tools::toDec(0.5 * i + 2),
                              getStringValue(INLINE_LEN, i),
                              getStringValue(INLINE_LEN, i + 1),
                              getStringValue(INLINE_LEN, i + 2),
                              getStringValue(NONINLINE_LEN, i));
        ltt->insertTuple(tuple);
    }

    ASSERT_EQ(1, lttBlockCache->numPinnedEntries());

    // Notify that we're done inserting so last block can be
    // unpinned.
    ltt->finishInserts();

    ASSERT_EQ(0, lttBlockCache->numPinnedEntries());

    // The table uses 4 blocks, but only 2 at a time can be cached.
    ASSERT_EQ(4, lttBlockCache->totalBlockCount());
    ASSERT_EQ(2, lttBlockCache->residentBlockCount());
    ASSERT_EQ(16*1024*1024, lttBlockCache->allocatedMemory());

    {
        TableIterator iter = ltt->iterator();
        TableTuple iterTuple(ltt->schema());
        int64_t i = 0;
        while (iter.next(iterTuple)) {
            bool success = assertTupleValuesEqual(&iterTuple,
                                                  i,
                                                  0.5 * i,
                                                  0.5 * i + 1,
                                                  0.5 * i + 2,
                                                  Tools::toDec(0.5 * i),
                                                  Tools::toDec(0.5 * i + 1),
                                                  Tools::toDec(0.5 * i + 2),
                                                  getStringValue(INLINE_LEN, i),
                                                  getStringValue(INLINE_LEN, i + 1),
                                                  getStringValue(INLINE_LEN, i + 2),
                                                  getStringValue(NONINLINE_LEN, i));
            if (! success) {
                break;
            }
            ++i;
        }

        ASSERT_EQ(NUM_TUPLES, i);
    }

    ltt->decrementRefcount();

    ASSERT_EQ(0, lttBlockCache->totalBlockCount());
    ASSERT_EQ(0, lttBlockCache->allocatedMemory());

    LargeTempTableTopend* theTopend = dynamic_cast<LargeTempTableTopend*>(ExecutorContext::getExecutorContext()->getTopend());
    ASSERT_EQ(0, theTopend->storedBlockCount());
}

TEST_F(LargeTempTableTest, basicBlockCache) {
    std::unique_ptr<Topend> topend{new LargeTempTableTopend()};

    // Define an LTT block cache that can hold only two blocks:
    int64_t tempTableMemoryLimitInBytes = 16 * 1024 * 1024;
    UniqueEngine engine = UniqueEngineBuilder()
        .setTopend(std::move(topend))
        .setTempTableMemoryLimit(tempTableMemoryLimitInBytes)
        .build();
    ScopedTupleSchema schema(Tools::buildSchema(VALUE_TYPE_BIGINT, VALUE_TYPE_DOUBLE));
    LargeTempTableBlockCache* lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
    LargeTempTableBlock* block = lttBlockCache->getEmptyBlock(schema.get());
    int64_t blockId = block->id();

    ASSERT_NE(NULL, block);
    ASSERT_TRUE(block->isPinned());

    // It's responsibilty of client code (iterators, executors) to
    // unpin blocks when they're no longer needed, so releasing a
    // pinned block is an error.  This is verified below.

    try {
        lttBlockCache->releaseBlock(blockId);
        ASSERT_TRUE_WITH_MESSAGE(false, "Expected release of pinned block to fail");
    }
    catch (const SerializableEEException &exc) {
        ASSERT_NE(std::string::npos, exc.message().find("Request to release pinned block"));
    }

    try {
        lttBlockCache->releaseAllBlocks();
        ASSERT_TRUE_WITH_MESSAGE(false, "Expected release of pinned block to fail");
    }
    catch (const SerializableEEException &exc) {
        ASSERT_NE(std::string::npos, exc.message().find("Request to release pinned block"));
    }

    block->unpin();
    lttBlockCache->releaseBlock(blockId);
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
