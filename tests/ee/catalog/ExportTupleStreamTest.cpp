/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
    void checkExportTupleStream(ExportTupleStream** expectedWrapper, size_t streamBytesUsed, int columnCount) {
        int64_t actualSeqNo;
        size_t actualStreamBytesUsed;
        auto tcd = m_engine->getTableDelegate("A");
        ASSERT_TRUE(tcd);
        auto streamedTable = tcd->getPersistentTable()->getStreamedTable();
        ASSERT_TRUE(streamedTable);
        ASSERT_EQ(columnCount, streamedTable->columnCount());
        ExportTupleStream* wrapper = streamedTable->getWrapper();
        ASSERT_TRUE(wrapper);
        if (*expectedWrapper) {            
            ASSERT_EQ(*expectedWrapper, wrapper);
        }
        streamedTable->getExportStreamPositions(actualSeqNo, actualStreamBytesUsed);
        ASSERT_EQ(seqNo, actualSeqNo);
        ASSERT_EQ(streamBytesUsed, actualStreamBytesUsed);
        streamedTable->setExportStreamPositions(0, streamBytesUsed + 3);
        expectedWrapper = &wrapper;
    }
};

TEST_F(ExportTupleStreamTest, TestEmptyExportTableChange) {
    // Create table.
    initialize(catalogPayloadBasic);
    ExportTupleStream* wrapper = NULL;
    checkExportTupleStream(&wrapper, 0, 0, 1);

    // Alter table add column, this is a stream update.
    m_engine->updateCatalog(time(NULL), true, catalogPayloadAddColumn);
    checkExportTupleStream(&wrapper, 2, 3, 2);

    // Create an index on the stream table, this is NOT a stream update.
    m_engine->updateCatalog(time(NULL), false, catalogPayloadCreateIndex);
    checkExportTupleStream(&wrapper, 4, 5, 2);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
