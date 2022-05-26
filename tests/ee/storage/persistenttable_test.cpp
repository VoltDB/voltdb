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
#include "test_utils/ScopedTupleSchema.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/TupleComparingTest.hpp"

#include "common/SynchronizedThreadLock.h"
#include "common/tabletuple.h"
#include "common/TupleSchemaBuilder.h"
#include "common/types.h"
#include "common/ValueFactory.hpp"

#include "execution/VoltDBEngine.h"

#include "indexes/tableindex.h"

#include "storage/persistenttable.h"
#include "storage/TableCatalogDelegate.hpp"
#include "storage/tablefactory.h"
#include "storage/tableutil.h"

#include "boost/scoped_ptr.hpp"
#include "common/debuglog.h"

using namespace voltdb;

class PersistentTableTest : public TupleComparingTest {
public:
    PersistentTableTest()
        : m_undoToken(0)
        , m_uniqueId(0)
    {
        m_engine.reset(new VoltDBEngine());
        m_engine->initialize(1,     // clusterIndex
                             1,     // siteId
                             0,     // partitionId
                             1,     // sitesPerHost
                             0,     // hostId
                             "",    // hostname
                             0,     // drClusterId
                             1024,  // defaultDrBufferSize
                             false,
                             -1,
                             false,
                             voltdb::DEFAULT_TEMP_TABLE_MEMORY,
                             true,  // this is the loweest SiteId/PartitionId
                             95);   // compaction threshold
        m_engine->setUndoToken(m_undoToken);
    }
    ~PersistentTableTest()
    {
        m_engine.reset();
        voltdb::globalDestroyOncePerProcess();
    }

protected:

    voltdb::VoltDBEngine* getEngine() const {
        return m_engine.get();
    }

    // Calling this will bump the unique ID in the executor context
    // and create a new DR timestamp value.
    void beginWork() {
        ExecutorContext::getExecutorContext()->setupForPlanFragments(
            m_engine->getCurrentUndoQuantum(),
            0,  // txn id
            0,  // sp handle
            0,  // last committed sp handle
            m_uniqueId,
            false);
        // DR timestamp discards the low 14 bits of the unique ID,
        // so we must increment by this amount to produce a new DR
        // timestamp next time around.
        m_uniqueId += (1 << 14);
    }

    void commit() {
        m_engine->releaseUndoToken(m_undoToken, false);
        ++m_undoToken;
        m_engine->setUndoToken(m_undoToken);
    }

    void rollback() {
        m_engine->undoUndoToken(m_undoToken);
        ++m_undoToken;
        m_engine->setUndoToken(m_undoToken);
    }

    static const std::string& catalogPayload() {
        static const std::string payload(
            "add / clusters cluster\n"
            "set /clusters#cluster localepoch 1199145600\n"
            "set /clusters#cluster drRole \"xdcr\"\n"
            "add /clusters#cluster databases database\n"
            "set /clusters#cluster/databases#database schema \"eJwDAAAAAAE=\"\n"
            ""
            "set /clusters#cluster/databases#database schema \"eJwlTDkCgDAI230NDSWUtdX/f8mgAzkBeoLBkZMBEw6C59cwrDRumLJiap5O07L9rStkqd0M8ZGa36ehHXZL52rGcng4USjf1wuc0Rgz\"\n"
            "add /clusters#cluster/databases#database tables T\n"
            "set /clusters#cluster/databases#database/tables#T isreplicated false\n"
            "set $PREV partitioncolumn /clusters#cluster/databases#database/tables#T/columns#PK\n"
            "set $PREV estimatedtuplecount 0\n"
            "set $PREV materializer null\n"
            "set $PREV signature \"T|bv\"\n"
            "set $PREV tuplelimit 2147483647\n"
            "set $PREV isDRed true\n"
            "add /clusters#cluster/databases#database/tables#T columns DATA\n"
            "set /clusters#cluster/databases#database/tables#T/columns#DATA index 1\n"
            "set $PREV type 9\n"
            "set $PREV size 256\n"
            "set $PREV nullable true\n"
            "set $PREV name \"DATA\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#T columns PK\n"
            "set /clusters#cluster/databases#database/tables#T/columns#PK index 0\n"
            "set $PREV type 6\n"
            "set $PREV size 8\n"
            "set $PREV nullable false\n"
            "set $PREV name \"PK\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#T indexes VOLTDB_AUTOGEN_IDX_PK_T_PK\n"
            "set /clusters#cluster/databases#database/tables#T/indexes#VOLTDB_AUTOGEN_IDX_PK_T_PK unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#T/indexes#VOLTDB_AUTOGEN_IDX_PK_T_PK columns PK\n"
            "set /clusters#cluster/databases#database/tables#T/indexes#VOLTDB_AUTOGEN_IDX_PK_T_PK/columns#PK index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#T/columns#PK\n"
            "add /clusters#cluster/databases#database/tables#T constraints VOLTDB_AUTOGEN_IDX_PK_T_PK\n"
            "set /clusters#cluster/databases#database/tables#T/constraints#VOLTDB_AUTOGEN_IDX_PK_T_PK type 4\n"
            "set $PREV oncommit \"\"\n"
            "set $PREV index /clusters#cluster/databases#database/tables#T/indexes#VOLTDB_AUTOGEN_IDX_PK_T_PK\n"
            "set $PREV foreignkeytable null\n"

            "add /clusters#cluster/databases#database tables X\n"
            "set /clusters#cluster/databases#database/tables#X isreplicated false\n"
            "set $PREV partitioncolumn /clusters#cluster/databases#database/tables#T/columns#PK\n"
            "set $PREV estimatedtuplecount 0\n"
            "set $PREV materializer null\n"
            "set $PREV signature \"X|bv\"\n"
            "set $PREV tuplelimit 2147483647\n"
            "set $PREV isDRed true\n"
            "add /clusters#cluster/databases#database/tables#X columns DATA\n"
            "set /clusters#cluster/databases#database/tables#X/columns#DATA index 1\n"
            "set $PREV type 9\n"
            "set $PREV size 256\n"
            "set $PREV nullable true\n"
            "set $PREV name \"DATA\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#X columns PK\n"
            "set /clusters#cluster/databases#database/tables#X/columns#PK index 0\n"
            "set $PREV type 6\n"
            "set $PREV size 8\n"
            "set $PREV nullable false\n"
            "set $PREV name \"PK\"\n"
            "set $PREV defaultvalue null\n"
            "set $PREV defaulttype 0\n"
            "set $PREV matview null\n"
            "set $PREV aggregatetype 0\n"
            "set $PREV matviewsource null\n"
            "set $PREV inbytes false\n"
            "add /clusters#cluster/databases#database/tables#X indexes VOLTDB_AUTOGEN_IDX_PK_X_PK\n"
            "set /clusters#cluster/databases#database/tables#X/indexes#VOLTDB_AUTOGEN_IDX_PK_X_PK unique true\n"
            "set $PREV assumeUnique false\n"
            "set $PREV countable true\n"
            "set $PREV type 1\n"
            "set $PREV expressionsjson \"\"\n"
            "set $PREV predicatejson \"\"\n"
            "add /clusters#cluster/databases#database/tables#X/indexes#VOLTDB_AUTOGEN_IDX_PK_X_PK columns PK\n"
            "set /clusters#cluster/databases#database/tables#X/indexes#VOLTDB_AUTOGEN_IDX_PK_X_PK/columns#PK index 0\n"
            "set $PREV column /clusters#cluster/databases#database/tables#X/columns#PK\n"
            "add /clusters#cluster/databases#database/tables#X constraints VOLTDB_AUTOGEN_IDX_PK_X_PK\n"
            "set /clusters#cluster/databases#database/tables#X/constraints#VOLTDB_AUTOGEN_IDX_PK_X_PK type 4\n"
            "set $PREV oncommit \"\"\n"
            "set $PREV index /clusters#cluster/databases#database/tables#X/indexes#VOLTDB_AUTOGEN_IDX_PK_X_PK\n"
            "set $PREV foreignkeytable null\n"
            "");
        return payload;
    }

    void validateCounts(size_t nIndexes, PersistentTable* table, PersistentTable* dupTable,
                        size_t nTuples, size_t nDupTuples) {
        validateCounts(table, nTuples, nIndexes);
        validateCounts(dupTable, nDupTuples, nIndexes);
    }

    void validateCounts(PersistentTable* table, size_t nTuples, size_t nIndexes) {
        auto iterator = table->iterator();
        ASSERT_EQ(nTuples > 0, iterator.hasNext());
        ASSERT_EQ(nTuples, table->activeTupleCount());
        ASSERT_EQ(nIndexes, table->indexCount());
        for (auto index : table->allIndexes()) {
            ASSERT_EQ(nTuples, index->getSize());
        }
    }

private:
    boost::scoped_ptr<VoltDBEngine> m_engine;
    int64_t m_undoToken;
    int64_t m_uniqueId;
};


namespace {

template<class ValueType>
TableTuple findTuple(Table* table, ValueType key) {
    TableIterator iterator = table->iterator();
    TableTuple iterTuple(table->schema());
    while (iterator.next(iterTuple)) {
        if (Tools::nvalueCompare(iterTuple.getNValue(0), key) == 0) {
            return iterTuple;
        }
    }

    return TableTuple(); // null tuple
}

}

TEST_F(PersistentTableTest, DRTimestampColumn) {

    // Load a catalog where active/active DR is turned on for the database,
    // And we have a table "T" which is being DRed.
    VoltDBEngine* engine = getEngine();
    engine->loadCatalog(0, catalogPayload());

    PersistentTable *table =
            engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    ASSERT_EQ(true, table->hasDRTimestampColumn());
    ASSERT_EQ(0, table->getDRTimestampColumnIndex());

    const voltdb::TupleSchema *schema = table->schema();
    ASSERT_EQ(1, schema->hiddenColumnCount());

    typedef std::tuple<int64_t, std::string> StdTuple;
    std::vector<StdTuple> stdTuples{
        StdTuple{1900, "Je me souviens"},
        StdTuple{1901, "Ut Incepit Fiedelis Sic Permanet"},
        StdTuple{1902, "Splendor sine occasu"}
    };

    // Let's do some inserts into the table.
    beginWork();

    voltdb::StandAloneTupleStorage storage(schema);
    TableTuple srcTuple = storage.tuple();
    BOOST_FOREACH(auto stdTuple, stdTuples) {
        Tools::initTuple(&srcTuple, stdTuple);
        table->insertTuple(srcTuple);
    }

    commit();

    // Now verify that the right DR timestamp was created in the
    // hidden column for each row.
    int64_t drTimestampOrig = ExecutorContext::getExecutorContext()->currentDRTimestamp();
    NValue drTimestampValueOrig = ValueFactory::getBigIntValue(drTimestampOrig);

    TableTuple tuple(schema);
    auto iterator = table->iteratorDeletingAsWeGo();
    const int timestampColIndex = table->getDRTimestampColumnIndex();
    BOOST_FOREACH(auto stdTuple, stdTuples) {
        TableTuple tuple = findTuple(table, std::get<0>(stdTuple));
        ASSERT_FALSE(tuple.isNullTuple());
        // DR timestamp is set for each row.
        EXPECT_EQ(0, tuple.getHiddenNValue(timestampColIndex).compare(drTimestampValueOrig));
        ASSERT_TUPLES_EQ(stdTuple, tuple);
    }

    // Now let's update the middle tuple with a new value, and make
    // sure the DR timestamp changes.
    beginWork();

    StdTuple newStdTuple{1901, "Nunavut Sannginivut"};
    tuple = findTuple(table, std::get<0>(newStdTuple));
    TableTuple &tempTuple = table->copyIntoTempTuple(tuple);
    tempTuple.setNValue(1, ValueFactory::getTempStringValue(std::get<1>(newStdTuple)));
    table->updateTupleWithSpecificIndexes(tuple,
                                          tempTuple,
                                          table->allIndexes());

    // Verify updated tuple has the new timestamp.
    int64_t drTimestampNew = ExecutorContext::getExecutorContext()->currentDRTimestamp();
    ASSERT_NE(drTimestampNew, drTimestampOrig);

    NValue drTimestampValueNew = ValueFactory::getBigIntValue(drTimestampNew);
    iterator = table->iteratorDeletingAsWeGo();
    for (int i = 0; i < 3; ++i) {
        StdTuple expectedTuple;
        if (i == 1) {
            expectedTuple = newStdTuple;
        }
        else {
            expectedTuple = stdTuples[i];
        }

        TableTuple tuple = findTuple(table, std::get<0>(expectedTuple));
        ASSERT_FALSE(tuple.isNullTuple());
        // DR timestamp is set for each row.
        if (i == 1) {
            EXPECT_EQ(0, tuple.getHiddenNValue(timestampColIndex).compare(drTimestampValueNew));
        }
        else {
            EXPECT_EQ(0, tuple.getHiddenNValue(timestampColIndex).compare(drTimestampValueOrig));
        }
        ASSERT_TUPLES_EQ(expectedTuple, tuple);
    }

    // After rolling back, we should have all our original values,
    // including the DR timestamp.
    rollback();

    BOOST_FOREACH(auto stdTuple, stdTuples) {
        TableTuple tuple = findTuple(table, std::get<0>(stdTuple));
        ASSERT_FALSE(tuple.isNullTuple());
        // DR timestamp is set for each row.
        EXPECT_EQ(0, tuple.getHiddenNValue(timestampColIndex).compare(drTimestampValueOrig));
        ASSERT_TUPLES_EQ(stdTuple, tuple);
    }
}

TEST_F(PersistentTableTest, TruncateTableTest) {
    bool added;
    VoltDBEngine* engine = getEngine();
    engine->loadCatalog(0, catalogPayload());
    PersistentTable *table = dynamic_cast<PersistentTable*>(engine->getTableByName("T"));
    ASSERT_NE(NULL, table);
    ASSERT_EQ(1, table->allocatedBlockCount());

    beginWork();
    const int tuplesToInsert = 10;
    added = tableutil::addRandomTuples(table, tuplesToInsert);
    ASSERT_TRUE(added);
    commit();

    size_t blockCount = table->allocatedBlockCount();
    table = dynamic_cast<PersistentTable*>(engine->getTableByName("T"));
    ASSERT_NE(NULL, table);
    ASSERT_EQ(blockCount, table->allocatedBlockCount());

    beginWork();
    added = tableutil::addRandomTuples(table, tuplesToInsert);
    ASSERT_TRUE(added);
    table->truncateTable(engine);
    commit();

    // refresh table pointer by fetching the table from catalog as in truncate old table
    // gets replaced with new cloned empty table
    table = dynamic_cast<PersistentTable*>(engine->getTableByName("T"));
    ASSERT_NE(NULL, table);
    ASSERT_EQ(1, table->allocatedBlockCount());
}

TEST_F(PersistentTableTest, SwapTablesTest) {
    PersistentTable* namedTable;
    VoltDBEngine* engine = getEngine();
    engine->loadCatalog(0, catalogPayload());

    PersistentTable* table;
    PersistentTable* dupTable;

    std::vector<std::string> tableIndexes;
    tableIndexes.push_back("VOLTDB_AUTOGEN_IDX_PK_T_PK");
    std::vector<std::string> dupIndexes;
    dupIndexes.push_back("VOLTDB_AUTOGEN_IDX_PK_X_PK");


    //
    // Swap empty tables.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the post-swap state of tables and indexes.
    validateCounts(1, table, dupTable, 0, 0);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, 0, 0);


    //
    // Swap a table with an empty table.
    //

    const int tuplesToInsert = 10;

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert, 0);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the post-swap state of tables and indexes.

    // After the swap, the first table should be empty.
    // After the swap, the second table should be populated.
    validateCounts(1, table, dupTable, 0, tuplesToInsert);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, 0, tuplesToInsert);

    //
    // Swap with data in both tables.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    // Populate the empty table currently swapped to table
    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert*3));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*3, tuplesToInsert);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the post-swap state of tables and indexes.

    // After the swap, the first table should contain the original population.
    // After the swap, the second table should contain the second population.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    // swap and then undo to swap back.
    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the post-swap state of tables and indexes.

    // After the swap, the first table should contain the second population.
    // After the swap, the second table should contain the original population.
    validateCounts(1, table, dupTable, tuplesToInsert*3, tuplesToInsert);

    rollback();

    beginWork();

    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the pre-swap state of the rolled back tables and indexes.
    // After the rollback, the first table should contain the original population.
    // After the rollback, the second table should contain the second population.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    // Test explicit do and undo within the same eventually aborted transaction.
    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    dupTable->swapTable(table, dupIndexes, tableIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the pre-swap state of the twice-swapped tables and indexes.
    // After the do+undo, the first table should contain the original population.
    // After the do+undo, the second table should contain the second population.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    rollback();

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    // Validate the pre-swap state of the rolled back tables and indexes.
    // After the rollback, the first table should contain the original population.
    // After the rollback, the second table should contain the second population.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    // Test explicit do and undo within the same eventually committed transaction.

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    dupTable->swapTable(table, dupIndexes, tableIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the pre-swap state of the rolled back tables and indexes.
    // After the do+undo, the first table should contain the original population.
    // After the do+undo, the second table should contain the second population.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);


    //
    // Test TRUNCATE and swap in the same committed transaction.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*3);

    // X is not a partitioned table.
    dupTable->truncateTable(engine);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();

    ASSERT_NE(NULL, dupTable);

    ////W-hy T-he F-ailure? validateCounts(1, table, dupTable, tuplesToInsert, 0);
    //// XXX: Junit testing validates that truncate table causes a later "table count"
    //// query to correctly return 0. In contrast, this validation was still finding
    //// a non-zero active tuple count on the truncated table. There must be something
    //// subtly different/missing in how we're validating table counts here prior to
    //// commit -- but WHAT?

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    ////W-hy T-he F-ailure? validateCounts(1, table, dupTable, 0, tuplesToInsert);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, 0, tuplesToInsert);


    //
    // Test TRUNCATE and swap in the same aborted transaction.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    validateCounts(1, table, dupTable, 0, tuplesToInsert);

    dupTable->truncateTable(engine);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    ////W-hy T-he F-ailure? validateCounts(1, table, dupTable, 0, 0);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    ////W-hy T-he F-ailure? validateCounts(1, table, dupTable, 0, 0);

    rollback();


    //
    // Test WRITES and swaps in the same aborted transaction.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    validateCounts(1, table, dupTable, 0, tuplesToInsert);

    // Test writes and swaps in the same aborted transaction.

    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert*2));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*2, tuplesToInsert);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*2);

    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert*4));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*5, tuplesToInsert*2);

    rollback();


    //
    // Test WRITES and swaps in the same committed transaction.
    //

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    validateCounts(1, table, dupTable, 0, tuplesToInsert);

    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert*2));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*2, tuplesToInsert);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    validateCounts(1, table, dupTable, tuplesToInsert, tuplesToInsert*2);

    ASSERT_TRUE(tableutil::addRandomTuples(table, tuplesToInsert*4));

    // Validate the pre-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*5, tuplesToInsert*2);

    table->swapTable(dupTable, tableIndexes, dupIndexes);
    namedTable = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_EQ(namedTable, dupTable);
    namedTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_EQ(namedTable, table);
    // Refresh the local pointers to reflect the updated table name associations.
    table = dupTable;
    dupTable = namedTable;

    // Validate the post-swap state of tables and indexes.
    validateCounts(1, table, dupTable, tuplesToInsert*2, tuplesToInsert*5);

    commit();

    // After the commit, re-assert the same counts.
    validateCounts(1, table, dupTable, tuplesToInsert*2, tuplesToInsert*5);

    beginWork();
    table = engine->getTableDelegate("T")->getPersistentTable();
    ASSERT_NE(NULL, table);
    dupTable = engine->getTableDelegate("X")->getPersistentTable();
    ASSERT_NE(NULL, dupTable);

    validateCounts(1, table, dupTable, tuplesToInsert*2, tuplesToInsert*5);

    rollback();
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
