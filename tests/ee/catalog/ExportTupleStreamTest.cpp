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
#include "storage/temptable.h"
#include "storage/persistenttable.h"
#include "storage/TableCatalogDelegate.hpp"
#include "test_utils/plan_testing_baseclass.h"
#include "ExportTupleStreamTest.hpp"
#include "storage/ExportTupleStream.h"

using namespace voltdb;

class ExportTupleStreamTest : public PlanTestingBaseClass<EngineTestTopend> {
public:
    void checkExportTupleStream(ExportTupleStream** expectedWrapper, int columnCount) {
        auto tcd = m_engine->getTableDelegate("A");
        ASSERT_TRUE(tcd);
        auto streamedTable = tcd->getPersistentTable()->getStreamedTable();
        ASSERT_TRUE(streamedTable);
        // Test how many columns are there in the table? Did the ALTER TABLE ADD COLUMN succeed?
        ASSERT_EQ(columnCount, streamedTable->columnCount());
        ExportTupleStream* wrapper = streamedTable->getWrapper();
        ASSERT_TRUE(wrapper);
        if (*expectedWrapper) {
            // Did the wrapper pointer change? This should not change at any time.
            ASSERT_EQ(*expectedWrapper, wrapper);
        } else {
            // If we do not have a baseline pointer yet, set one.
            expectedWrapper = &wrapper;
        }
        int64_t seqNo;
        size_t streamBytesUsed;
        int64_t generationId;
        int64_t nextSeqNoFromWrapper = wrapper->getSequenceNumber();
        streamedTable->getExportStreamPositions(seqNo, streamBytesUsed, generationId);
        // Verify the sequence number.
        ASSERT_EQ(seqNo + 1, nextSeqNoFromWrapper);
    }
};

TEST_F(ExportTupleStreamTest, TestExportTableChange) {
    /*
        CREATE TABLE a (
            last_update timestamp default now not null
        ) USING TTL 1 SECONDS ON COLUMN last_update BATCH_SIZE 2
            MIGRATE TO TARGET archive;
    */
    initialize(catalogPayloadBasic);
    ExportTupleStream* wrapper = NULL;
    checkExportTupleStream(&wrapper, 1);

    // ALTER TABLE a ADD COLUMN a INT NOT NULL;
    // Alter table add column, this is a stream update, and can only be done on empty streams.
    m_engine->updateCatalog(time(NULL), true, catalogPayloadAddColumnA);
    checkExportTupleStream(&wrapper, 2);

    // CREATE INDEX idx_a ON a (last_update) WHERE NOT MIGRATING;
    // Create an index on the stream table, this is NOT a stream update.
    m_engine->updateCatalog(time(NULL), false, catalogPayloadCreateIndex);
    checkExportTupleStream(&wrapper, 2);

    // Now migrat can work. Let's insert rows. Note: BATCH_SIZE = 2
    fragmentId_t insertPlanId = 100;
    fragmentId_t migratePlanId = 200;
    m_topend->addPlan(insertPlanId, isertPlan);
    m_topend->addPlan(migratePlanId, migratePlan);
    // Prepare parameters for each fragment (statement):
    initParamsBuffer();
    voltdb::ReferenceSerializeInputBE params(m_parameter_buffer.get(), m_smallBufferSize);
    // The insert statement does not have any query parameters.
    boost::scoped_array<fragmentId_t> scopedPlanfragmentIds(new fragmentId_t[4]);
    fragmentId_t* planfragmentIds = scopedPlanfragmentIds.get();
    for (int i = 0; i < 4; i++) {
        planfragmentIds[i] = insertPlanId;
    }
    // Insert 4 rows.
    ASSERT_EQ(0, m_engine->executePlanFragments(4, planfragmentIds, NULL, params, 1000, 1000, 1000, 1000, 1, false));
    m_engine->releaseUndoToken(1, false);
    // Execute MIGRATE FROM A WHERE not migrating AND LAST_UPDATE <= NOW;
    planfragmentIds[0] = migratePlanId;
    ASSERT_EQ(0, m_engine->executePlanFragments(1, planfragmentIds, NULL, params, 2000, 2000, 2000, 2000, 2, false));
    m_engine->releaseUndoToken(2, false);
    checkExportTupleStream(&wrapper, 2);
    // Delete migrate rows
    m_engine->deleteMigratedRows(3000, 2000, 2000, "A", 2000, 3);
    m_engine->releaseUndoToken(3, false);
    checkExportTupleStream(&wrapper, 2);

    // ALTER TABLE a USING TTL 1 SECONDS ON COLUMN last_update BATCH_SIZE 1 MIGRATE TO TARGET archive;
    // Alter table change TTL, this is NOT a stream update.
    m_engine->updateCatalog(time(NULL), false, catalogPayloadChangeBatchSize);
    checkExportTupleStream(&wrapper, 2);

    // What we just did was updating catalog when the table was empty.
    // Do one more (last) catalog update when the table is not empty.
    planfragmentIds[0] = insertPlanId;
    ASSERT_EQ(0, m_engine->executePlanFragments(1, planfragmentIds, NULL, params, 4000, 4000, 4000, 4000, 4, false));
    m_engine->releaseUndoToken(4, false);
    m_engine->quiesce(4000);
    // ALTER TABLE a ADD COLUMN b INT;
    m_engine->updateCatalog(time(NULL), true, catalogPayloadAddColumnB);
    checkExportTupleStream(&wrapper, 3);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
