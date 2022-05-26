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

#include <chrono>
#include <tuple>
#include <utility>


#include "boost/foreach.hpp"
#include "boost/lexical_cast.hpp"

#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"

#include "expressions/tuplevalueexpression.h"

#include "storage/LargeTempTableBlock.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

#include "harness.h"

#include "test_utils/LargeTempTableTopend.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/TupleComparingTest.hpp"
#include "test_utils/UniqueEngine.hpp"
#include "test_utils/UniqueTable.hpp"

using namespace voltdb;

class LargeTempTableSortTest : public TupleComparingTest {
public:

    LargeTempTableSortTest()
    {
    }

protected:

    UniqueTable<LargeTempTable> createAndFillLargeTempTable(int32_t varcharLengthBytes,
                                                            int32_t inlinePadding,
                                                            int32_t numBlocks) {
        LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();
        TupleSchema *schema = getSchemaOfLength(varcharLengthBytes, inlinePadding);
        std::vector<std::string> names;
        names.push_back("strfld");
        for (int i = 1; i < schema->columnCount(); ++i) {
            names.push_back(std::string("tiny") + boost::lexical_cast<std::string>(i));
        }

        auto ltt = makeUniqueTable(TableFactory::buildLargeTempTable("ltmp", schema, names));

        int expectedTuples = 0;
        for (int i = 0; i < numBlocks; ++i) {
            LargeTempTableBlock* block = lttBlockCache.getEmptyBlock(schema);
            fillBlock(block);
            expectedTuples += block->activeTupleCount();
            block->unpin();
            ltt->inheritBlock(block->id());
        }

        if (expectedTuples != ltt->activeTupleCount()) {
            FAIL("Expected tuples does not match tuple count of table");
            return NULL;
        }

        return ltt;
    }

    UniqueTable<LargeTempTable> copyLargeTempTable(LargeTempTable* srcTable) {
        auto dstTable = makeUniqueTable(TableFactory::buildCopiedLargeTempTable("copy", srcTable));

        TableIterator tblIt = srcTable->iterator();
        TableTuple tuple(srcTable->schema());
        while (tblIt.next(tuple)) {
            dstTable->insertTuple(tuple);
        }

        dstTable->finishInserts();

        return dstTable;
    }

    bool validateSortWithLimitOffset(Table* sortedRefTable,
                                     Table* actualTable,
                                     const AbstractExecutor::TupleComparer& comparer,
                                     int limit,
                                     int offset) {
        std::ostringstream oss;

        oss << "Validating sort (offset = "
            << offset << ", limit = " << limit << "): ";

        // First determine the expected tuple count
        int expectedTupleCount = sortedRefTable->activeTupleCount() - offset;
        if (expectedTupleCount < 0) {
            expectedTupleCount = 0;
        }
        else if (limit != -1) {
            expectedTupleCount = std::min(limit, expectedTupleCount);
        }

        int actualTupleCount = actualTable->activeTupleCount();
        if (actualTupleCount != expectedTupleCount) {
            oss << "tuple count is wrong; expected: " << expectedTupleCount
                << ", actual: " << actualTupleCount << "\n";
            std::cerr << oss.str();
            return false;
        }

        TableIterator refTblIt = sortedRefTable->iterator();
        TableTuple refTuple(sortedRefTable->schema());

        // Advance reference table past offset
        for (int i = 0; i < offset; ++i) {
            bool hasTuple = refTblIt.next(refTuple);
            if (! hasTuple) {
                break;
            }
        }

        int tupleCount = 0;
        TableIterator actualTblIt = actualTable->iterator();
        TableTuple actualTuple(actualTable->schema());
        while (actualTblIt.next(actualTuple)) {
            bool hasTuple = refTblIt.next(refTuple);
            if (! hasTuple) {
                oss << "actual table has too many rows: " << actualTable->activeTupleCount() << "\n";
                std::cerr << oss.str();
                return false;
            }

            for (int i = 0; i < refTuple.columnCount(); ++i) {
                NValue refNVal = refTuple.getNValue(i);
                NValue actualNVal = actualTuple.getNValue(i);
                int cmp = Tools::nvalueCompare(refNVal, actualNVal);
                if (cmp != 0) {
                    oss << "at tuple " << tupleCount << ", values in position " << i << " invalid; "
                        << "expected: " << refNVal.debug() << ", actual: " << actualNVal.debug() << "\n";
                    std::cerr << oss.str();
                    return false;
                }
            }

            ++tupleCount;
        }

        if (limit > 0 && tupleCount > limit) {
            oss << "actual table has more than " << limit << " rows (it has "
                << actualTable->activeTupleCount() << ") and exceeds limit\n";
            std::cerr << oss.str();
            return false;
        }

        if (tupleCount < limit) {
            bool hasTuple = refTblIt.next(refTuple);
            if (hasTuple) {
                oss << "actual table has fewer rows than expected\n";
                std::cerr << oss.str();
                return false;
            }
        }

        return true;
    }

private:
    TupleSchema* getSchemaOfLength(int32_t varcharLengthBytes, int32_t inlinePadding) {
        TupleSchemaBuilder builder(inlinePadding + 1);
        builder.setColumnAtIndex(0, ValueType::tVARCHAR, varcharLengthBytes, true, true);
        for (int i = 0; i < inlinePadding; ++i) {
            builder.setColumnAtIndex(i + 1, ValueType::tTINYINT);
        }
        return builder.build();
    }

    void fillBlock(LargeTempTableBlock* block) {
        Pool* tempPool = ExecutorContext::getTempStringPool();
        StandAloneTupleStorage storage{block->schema()};
        TableTuple tupleToInsert = storage.tuple();

        for (int i = 1; i < block->schema()->columnCount(); ++i) {
            tupleToInsert.setNValue(i, Tools::nvalueFromNative(int8_t(i)));
        }

        uint32_t varcharLength = block->schema()->getColumnInfo(0)->length;
        do {
            tupleToInsert.setNValue(0, ValueFactory::getRandomValue(ValueType::tVARCHAR, varcharLength, tempPool));
        }
        while (block->insertTuple(tupleToInsert));
    }
};

namespace {

template<class Compare>
bool lessThanOrEqual(const Compare& lessThan, const TableTuple tuple0, const TableTuple tuple1) {
    if (lessThan(tuple0, tuple1)) {
        return true;
    }

    if (lessThan(tuple1, tuple0)) { // greater than
        return false;
    }

    return true; // equal
}

bool verifySortedTable(const AbstractExecutor::TupleComparer& comparer,
                       LargeTempTable* table) {
    Pool* pool = ExecutorContext::getTempStringPool();

    StandAloneTupleStorage prevTupleStorage(table->schema());
    TableTuple prevTuple = prevTupleStorage.tuple();

    TableIterator verifyIt = table->iterator();
    TableTuple verifyTuple(table->schema());
    bool success = verifyIt.next(verifyTuple);
    if (! success) {
        std::cerr << "verifySortedTable failed; no tuples" << std::endl;
        return false;
    }

    prevTuple.copyForPersistentInsert(verifyTuple, pool);

    int tupleNum = 1;
    while (verifyIt.next(verifyTuple)) {
        success = lessThanOrEqual(comparer, prevTuple, verifyTuple);
        if (!success) {
            std::cerr << "Failed to verify " << tupleNum << "th tuple:\n";
            std::cerr << "    prev tuple: " << prevTuple.debug() << "\n";
            std::cerr << "    curr tuple: " << verifyTuple.debug() << std::endl;
            return false;
        }
        prevTuple.copyForPersistentInsert(verifyTuple, pool);
        ++tupleNum;
    }

    if (table->activeTupleCount() != tupleNum) {
        std::cerr << "Failed to verify table; tuple count wrong: "
                  << "expected " << table->activeTupleCount() << ", actual " << tupleNum
                  << std::endl;
        return false;
    }

    return true;
}

} // end anonymous namespace


TEST_F(LargeTempTableSortTest, sortLargeTempTable) {
    using namespace std::chrono;

    // varchar field length (bytes), inline padding, number of blocks
    typedef std::tuple<int32_t, int32_t, int32_t> SortTableSpec;

#ifndef MEMCHECK
    // Vary the max size of temp table storage, since this affects how
    // many blocks we can merge at once.
    std::vector<int64_t> tempTableMemoryLimits{
        voltdb::DEFAULT_TEMP_TABLE_MEMORY, // 100 MB (default)
        1024 * 1024 * 200   // 200 MB
    };

    // Try varying schema, some with non-inlined and some without
    std::vector<SortTableSpec> specs{
        SortTableSpec{8192, 16, 25}, // large non-inlined data
        SortTableSpec{63, 8192, 1}, // large tuples, no non-inlined data
    };
#else // memcheck mode
    // Memcheck is slow, so just use the default TT storage size
    std::vector<int64_t> tempTableMemoryLimits{
        voltdb::DEFAULT_TEMP_TABLE_MEMORY/4
    };

    // Use larger tuples so the sorts are faster.  Also test all
    // inlined as well as some non-inlined data.
    std::vector<SortTableSpec> specs{
            SortTableSpec{64, 4096, 4}, // some non-inlined data
            SortTableSpec{16, 4096, 4}, // large tuples, no non-inlined data
    };
#endif

    std::cout << "\n";
    BOOST_FOREACH(auto memoryLimit, tempTableMemoryLimits) {
        UniqueEngineBuilder builder;
        builder.setTopend(std::unique_ptr<LargeTempTableTopend>(new LargeTempTableTopend()));
        std::cout << "          With " << (memoryLimit / (1024*1024))
                  << " MB of temp table memory:\n";
        builder.setTempTableMemoryLimit(memoryLimit);

        UniqueEngine engine = builder.build();

        BOOST_FOREACH(auto spec, specs) {
            int32_t varcharLength = std::get<0>(spec);
            int32_t inlinePadding = std::get<1>(spec);
            int32_t numBlocks = std::get<2>(spec);

            std::cout << "            Generating " << numBlocks
                      << " blocks of tuples (VARCHAR(" << varcharLength << " BYTES), <"
                      << inlinePadding << " TINYINT fields>)...";
            std::cout.flush();

            auto ltt = createAndFillLargeTempTable(varcharLength, inlinePadding, numBlocks);

            TupleValueExpression tve{0, 0}; // table 0, field 0
            std::vector<AbstractExpression*> keys{&tve};
            std::vector<SortDirectionType> dirs{SORT_DIRECTION_TYPE_ASC};
            AbstractExecutor::TupleComparer comparer{keys, dirs};

            std::cout << "sorting...";
            std::cout.flush();

            int rowsBefore = ltt->activeTupleCount();

            auto startTime = high_resolution_clock::now();

            ltt->sort(NULL, comparer, -1, 0); // no limit (-1), no offset (0)

            auto endTime = high_resolution_clock::now();
            auto totalSortDurationMicros = duration_cast<microseconds>(endTime - startTime);
            std::cout << "sorted " << ltt->activeTupleCount() << " tuples in "
                      << (totalSortDurationMicros.count() / 1000000.0) << " seconds.\n";

            ASSERT_EQ(rowsBefore, ltt->activeTupleCount());
            ASSERT_TRUE(verifySortedTable(comparer, ltt.get()));
        } // end for each sort config
    } // end for each engine config

    std::cout << "          ";
}

namespace {

// limit, offset
typedef std::tuple<int, int> SortConfig;

#ifndef MEMCHECK
std::vector<SortConfig> generateSortConfigs(const LargeTempTable *ltt) {
    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    std::vector<SortConfig> configs;

    std::vector<int> limits;
    std::vector<int> offsets;

    limits.push_back(0);
    limits.push_back(1);

    offsets.push_back(0);
    offsets.push_back(1);

    // Add some interesting numbers:
    int totalTuples = static_cast<int>(ltt->activeTupleCount());
    if (totalTuples > 0) {
        LargeTempTableBlock *block = lttBlockCache.getBlockForDebug(ltt->getBlockIds()[0]);
        int tuplesPerBlock = static_cast<int>(block->activeTupleCount());

        std::vector<int> interestingValues {
            tuplesPerBlock,
            totalTuples
        };

        BOOST_FOREACH (int n, interestingValues) {
            limits.push_back(n);
            offsets.push_back(n);
        }
    }

    limits.push_back(-1); // no limit

    BOOST_FOREACH(int limit, limits) {
        BOOST_FOREACH(int offset, offsets) {
            configs.push_back(SortConfig{limit, offset});
        }
    }

    return configs;
}
#endif /* not(defined(MEMCHECK)) */

} // end anonymous namespace

#ifndef MEMCHECK
TEST_F(LargeTempTableSortTest, limitOffset) {
    using namespace std::chrono;

    UniqueEngineBuilder builder;
    builder.setTopend(std::unique_ptr<LargeTempTableTopend>(new LargeTempTableTopend()));
    UniqueEngine engine = builder.build();

    TupleValueExpression tve{0, 0}; // table 0, field 0
    std::vector<AbstractExpression*> keys{&tve};
    std::vector<SortDirectionType> dirs{SORT_DIRECTION_TYPE_ASC};
    AbstractExecutor::TupleComparer comparer{keys, dirs};

    // VARCHAR field length, num TINYINT columns, num blocks
    typedef std::tuple<int, int, int> TableConfig;

    std::vector<TableConfig> tableConfigs{
        // empty table
        TableConfig{1, 1, 0},
        // no non-inlined data
        TableConfig{63, 8192, 3},
        // 13 blocks ensures that we need two merge pass.
        // Larger records make the test faster
        TableConfig{8192, 8192, 13}
    };

    std::cout << "\n            "
              << std::setw(8) << "LIMIT" << "  "
              << std::setw(8) << "OFFSET" << "  "
              << "TIME TO SORT (ms)" << std::endl;

    BOOST_FOREACH(TableConfig tableConfig, tableConfigs) {
        int varcharBytes = std::get<0>(tableConfig);
        int inlinedBytes = std::get<1>(tableConfig);
        int numBlocks = std::get<2>(tableConfig);

        auto inputTable = createAndFillLargeTempTable(varcharBytes, inlinedBytes, numBlocks);

        std::cout << "          Table config: (VARCHAR("
                  << varcharBytes << " bytes), <"
                  << inlinedBytes << " TINYINT fields>), "
                  << inputTable->activeTupleCount() << " tuples in " << numBlocks << " blocks\n";


        auto sortedRefTable = copyLargeTempTable(inputTable.get());
        sortedRefTable->sort(NULL, comparer, -1, 0); // no limit (-1), no offset (0)

        auto sortConfigs = generateSortConfigs(inputTable.get());

        BOOST_FOREACH(SortConfig sortConfig, sortConfigs) {


            int limit = std::get<0>(sortConfig);
            int offset = std::get<1>(sortConfig);
            std::cout << "            "
                      << std::setw(8) << limit << "  " << std::setw(8) << offset << "  ";
            std::flush(std::cout);

            auto actualTable = copyLargeTempTable(inputTable.get());

            auto startTime = high_resolution_clock::now();

            actualTable->sort(NULL, comparer, limit, offset);

            auto endTime = high_resolution_clock::now();
            auto totalSortDurationMicros = duration_cast<microseconds>(endTime - startTime);

            std::cout << std::setw(8) << std::setprecision(3) << std::fixed
                      << (totalSortDurationMicros.count() / 1000.0) << std::endl;

            ASSERT_TRUE(validateSortWithLimitOffset(sortedRefTable.get(), actualTable.get(), comparer, limit, offset));
        }
    }
    std::cout << "        ";
}
#endif /* not(defined(MEMCHECK)) */

int main(int argc, char* argv[]) {
    using namespace std::chrono;
    auto startTime = high_resolution_clock::now();

    int rc = TestSuite::globalInstance()->runAll();

    auto endTime = high_resolution_clock::now();
    auto totalSortDurationMicros = duration_cast<microseconds>(endTime - startTime);

    std::cout << "This test took "
              << std::setprecision(3) << std::fixed
              << (totalSortDurationMicros.count() / 1000000.0) << " seconds." << std::endl;

    return rc;
}
