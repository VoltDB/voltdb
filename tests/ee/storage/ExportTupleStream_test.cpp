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

#include <cstring>
#include <cstdlib>
#include <queue>
#include <deque>
#include <sstream>
#include "harness.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/StreamBlock.h"
#include "storage/ExportTupleStream.h"
#include "common/Topend.h"
#include "common/executorcontext.hpp"
#include "boost/smart_ptr.hpp"

using namespace voltdb;

static const int COLUMN_COUNT = 5;

////5 integers
static const int TUPLE_SIZE = 20;
//RowSize(int32_t)+PartitionIndex(int32_t)+ColumnCount(int32_t)+nullMaskLength(2)
static const int STREAM_HEADER_SZ = 14;
//MetadataDataSize 5*int64_t+1byte
static const int METADATA_DATA_SIZE = 41;
//Data size without schema information. = 75
static const int MAGIC_TUPLE_SIZE = TUPLE_SIZE + STREAM_HEADER_SZ + METADATA_DATA_SIZE;
// Size of Buffer header including schema and uso
static const int BUFFER_HEADER_SIZE = MAGIC_HEADER_SPACE_FOR_JAVA + ExportTupleStream::s_EXPORT_BUFFER_HEADER_SIZE;

// 1k buffer
static const int BUFFER_SIZE = 1024;

class MockVoltDBEngine : public VoltDBEngine {
public:
    MockVoltDBEngine()
    {
        m_enginesOldest = NULL;
        m_enginesNewest = NULL;
    }

    ~MockVoltDBEngine() { }


    ExportTupleStream** getNewestExportStreamWithPendingRowsForAssignment() {
        return &m_enginesNewest;
    }

    ExportTupleStream** getOldestExportStreamWithPendingRowsForAssignment() {
        return &m_enginesOldest;
    }

    bool streamsToFlush() {
        assert(m_enginesNewest == m_enginesOldest);
        return m_enginesOldest != NULL;
    }

private:
    ExportTupleStream* m_enginesOldest;
    ExportTupleStream* m_enginesNewest;
};

class ExportTupleStreamTest : public Test {
public:
    ExportTupleStreamTest()
      : m_engine(new MockVoltDBEngine())
      , m_context(new ExecutorContext(1, 1, NULL, &m_topend, &m_pool, (VoltDBEngine*)m_engine,
                                    "localhost", 2, NULL, NULL, 0))
      , m_tableName("FOO")
    {
        srand(0);

        // set up the schema used to fill the new buffer
        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull;

        for (int i = 0; i < COLUMN_COUNT; i++) {
            columnTypes.push_back(ValueType::tINTEGER);
            columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
            columnAllowNull.push_back(false);
        }
        m_schema =
          TupleSchema::createTupleSchemaForTest(columnTypes,
                                         columnLengths,
                                         columnAllowNull);

        // allocate a new buffer and wrap it
        m_wrapper = new ExportTupleStream(1, 1, 0, m_tableName);

            // excercise a smaller buffer capacity
        m_wrapper->setDefaultCapacityForTest(BUFFER_SIZE);

        // set up the tuple we're going to use to fill the buffer
        // set the tuple's memory to zero
        ::memset(m_tupleMemory, 0, 8 * (COLUMN_COUNT + 1));

        // deal with the horrible hack that needs to set the first
        // value to true (rtb?? what is this horrible hack?)
        *(reinterpret_cast<bool*>(m_tupleMemory)) = true;
        m_tuple = new TableTuple(m_schema);
        m_tuple->move(m_tupleMemory);
        m_tupleSize = MAGIC_TUPLE_SIZE;
        m_tuplesToFill = (BUFFER_SIZE - BUFFER_HEADER_SIZE) / (m_tupleSize);
//        cout << "tuple size: " << m_tupleSize << " column name size: metadata - " << m_wrapper->getMDColumnNamesSerializedSize()
//                << ", column names - " << columnNamesLength << std::endl;
    }

    typedef struct rollbackMarker {
        rollbackMarker(size_t m, int64_t s) {
            mark = m;
            seqNo = s;
        }
        size_t mark;
        int64_t seqNo;
    } rollbackMarker;

    rollbackMarker appendTuple(int64_t currentTxnId, int64_t seqNo) {
        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
        }

        size_t mark = m_wrapper->bytesUsed();
        int64_t prevSeqNo = m_wrapper->getSequenceNumber();
        int64_t uniqueId = UniqueId::makeIdFromComponents(currentTxnId + VOLT_EPOCH_IN_MILLIS, 0, 0);

        // append into the buffer (sequence number should be the same as the currentTxnId for test purposes)
        m_wrapper->appendTuple(m_engine, currentTxnId, seqNo, uniqueId, *m_tuple,
                               1,
                               ExportTupleStream::INSERT);
        return rollbackMarker(mark, prevSeqNo);
    }

    void commit(int64_t currentTxnId) {
        int64_t uniqueId = UniqueId::makeIdFromComponents(currentTxnId + VOLT_EPOCH_IN_MILLIS, 0, 0);
        m_wrapper->commit(m_engine, currentTxnId, uniqueId);
    }

    void rollbackExportTo(rollbackMarker marker) {
        m_wrapper->rollbackExportTo(marker.mark, marker.seqNo);
    }

    void periodicFlush(int64_t timeInMillis, int64_t lastCommittedSpHandle) {
        m_wrapper->periodicFlush(timeInMillis, lastCommittedSpHandle);
        *m_engine->getNewestExportStreamWithPendingRowsForAssignment() = NULL;
        *m_engine->getOldestExportStreamWithPendingRowsForAssignment() = NULL;
    }

    bool checkTargetFlushTime(int64_t target) {
        if (!m_engine->streamsToFlush() || !m_wrapper->testFlushPending()) return -1;
        int64_t expectedUniqueId = UniqueId::makeIdFromComponents(target + VOLT_EPOCH_IN_MILLIS, 0, 0);
        return m_wrapper->testFlushBuffCreateTime() == UniqueId::tsInMillis(expectedUniqueId);
    }

    bool testNoStreamsToFlush() {
        return !m_engine->streamsToFlush() && !m_wrapper->testFlushPending();
    }

    virtual ~ExportTupleStreamTest() {
        delete m_wrapper;
        delete m_tuple;
        if (m_schema)
            TupleSchema::freeTupleSchema(m_schema);
        delete m_engine;
        voltdb::globalDestroyOncePerProcess();
    }

protected:
    ExportTupleStream* m_wrapper;
    TupleSchema* m_schema;
    char m_tupleMemory[(COLUMN_COUNT + 1) * 8];
    TableTuple* m_tuple;
    DummyTopend m_topend;
    Pool m_pool;
    MockVoltDBEngine* m_engine;
    boost::scoped_ptr<ExecutorContext> m_context;
    size_t m_tupleSize;
    int m_tuplesToFill;
    std::string m_tableName;
};

// Several of these cases were move to TestExportDataSource in Java
// where some ExportTupleStream functionality now lives
// Cases of interest:
// 1. periodicFlush with a clean buffer (no open txns) generates a new buffer
//    DONE
// 2. appendTuple fills and generates a new buffer (committed TXN ID advances)
//    DONE
// 3. appendTuple fills a buffer with a single TXN ID, uncommitted,
//    commits somewhere in the next buffer
//    DONE
// 4. case 3 but where commit is via periodic flush
//    DONE
// 5. case 2 but where the last tuple is rolled back
//    DONE
// 6. periodicFlush with a busy buffer (an open txn) doesn't generate a new buffer
//    DONE
// 7. roll back the last tuple, periodicFlush, get the expected length
//    DONE
// 8. Case 1 but where the first buffer is just released, not polled
//    DONE
// 9. Roll back a transaction that has filled more than one buffer,
//    then add a transaction, then commit and poll
//    DONE
// 10. Rollback the first tuple, then append, make sure only 1 tuple
//     DONE
// 11. Test that releasing tuples that aren't committed returns an error
//     DONE
// 12. Test that a release value that isn't a buffer boundary returns an error
//     DONE
// 13. Test that releasing all the data followed by a poll results in no data
//     DONE
// 14. Test that a periodicFlush with both txn IDs far in the future behaves
//     correctly
//     DONE
// 15. Test that a release value earlier than our current history return safely
//     DONE
// 16. Test that a release that includes all the pending buffers works properly
//     DONE
//---
// Additional floating release/poll tests
//
// 17. Test that a release in the middle of a finished buffer followed
//     by a poll returns a StreamBlock with a proper releaseOffset
//     (and other meta-data), basically consistent with handing the
//     un-ack'd portion of the block to Java.
//     - Invalidates old test (12)
//
// 18. Test that a release in the middle of the current buffer returns
//     a StreamBlock consistent with indicating that no data is
//     currently available.  Then, if that buffer gets filled and
//     finished, that the next poll returns the correct remainder of
//     that buffer.

/**
 * Get one tuple
 */
TEST_F(ExportTupleStreamTest, DoOneTuple) {

    // write a new tuple and then flush the buffer
    appendTuple(2, 1);
    commit(2);
    ASSERT_TRUE(checkTargetFlushTime(2));
    periodicFlush(-1, 2);
    ASSERT_TRUE(testNoStreamsToFlush());

    // we should only have one tuple in the buffer
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    EXPECT_EQ(results->uso(), 0);
    std::ostringstream os;
    os << "Offset mismatch. Expected: " << m_tupleSize << ", actual: " << results->offset();
    ASSERT_TRUE_WITH_MESSAGE(results->offset() == m_tupleSize, os.str().c_str());
}

/**
 * Test the really basic operation order
 */
TEST_F(ExportTupleStreamTest, BasicOps) {

    // verify the block count statistic.
    size_t allocatedByteCount = m_wrapper->testAllocatedBytesInEE();

    EXPECT_TRUE(allocatedByteCount == 0);
    std::ostringstream os;
    int cnt = 0;
    // Push 2 rows to fill the block less thn half
    for (cnt = 1; cnt < 3; cnt++) {
        appendTuple(cnt, cnt);
        commit(cnt);
    }

    ASSERT_TRUE(checkTargetFlushTime(1));
    periodicFlush(-1, 2);
    ASSERT_TRUE(testNoStreamsToFlush());
    allocatedByteCount = (m_tupleSize * 2) + BUFFER_HEADER_SIZE;
    os << "Allocated byte count - expected: " << allocatedByteCount << ", actual: " << m_wrapper->testAllocatedBytesInEE();
    ASSERT_TRUE_WITH_MESSAGE( allocatedByteCount == m_wrapper->testAllocatedBytesInEE(), os.str().c_str());
    os.str(""); os << "Blocks on top-end expected: " << 1 << ", actual: " << m_topend.exportBlocks.size();
    ASSERT_TRUE_WITH_MESSAGE(m_topend.exportBlocks.size() == 1, os.str().c_str());
    boost::shared_ptr<ExportStreamBlock> results2 = m_topend.exportBlocks.front();
    EXPECT_EQ(results2->uso(), 0);

    os.str(""); os << "startSequenceNumber expected: " << 1 << ", actual: " << results2->startSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results2->startSequenceNumber() == 1, os.str().c_str());
    os.str(""); os << "lastSequenceNumber expected: " << 2 << ", actual: " << results2->lastSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results2->lastSequenceNumber() == 2, os.str().c_str());
    os.str(""); os << "committedSequenceNumber expected: " << 2 << ", actual: " << results2->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results2->getCommittedSequenceNumber() == 2, os.str().c_str());

    // Push 3 rows
    for (cnt = 3; cnt < 6; cnt++) {
        appendTuple(cnt, cnt);
        commit(cnt);
    }
    ASSERT_TRUE(checkTargetFlushTime(3));
    periodicFlush(-1, 5);
    ASSERT_TRUE(testNoStreamsToFlush());

    boost::shared_ptr<ExportStreamBlock> results3 = m_topend.exportBlocks.back();
    os.str(""); os << "lastSequenceNumber expected: " << 5 << ", actual: " << results3->lastSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results3->lastSequenceNumber() == 5, os.str().c_str());
    os.str(""); os << "committedSequenceNumber expected: " << 5 << ", actual: " << results3->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results3->getCommittedSequenceNumber() == 5, os.str().c_str());

    // 3 rows - 2 blocks (2, 3)
    allocatedByteCount = (m_tupleSize * 5) + (BUFFER_HEADER_SIZE * 2);
    os.str(""); os << "Allocated byte count - expected: " << allocatedByteCount << ", actual: " << m_wrapper->testAllocatedBytesInEE();
    ASSERT_TRUE_WITH_MESSAGE( allocatedByteCount == m_wrapper->testAllocatedBytesInEE(), os.str().c_str());
    os.str(""); os << "Blocks on top-end expected: " << 2 << ", actual: " << m_topend.exportBlocks.size();
    ASSERT_TRUE_WITH_MESSAGE(m_topend.exportBlocks.size() == 2, os.str().c_str());

    // get the first buffer flushed
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * 2));

    // now get the second
    ASSERT_FALSE(m_topend.exportBlocks.empty());
    results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    os.str(""); os << "Second block uso - expected: " << (m_tupleSize * 2) << ", actual: " << results->uso();
    ASSERT_TRUE_WITH_MESSAGE(results->uso() == (m_tupleSize * 2), os.str().c_str());
    os.str(""); os << "Second block offset - expected: " << (m_tupleSize * 2) << ", actual: " << results->offset();
    ASSERT_TRUE_WITH_MESSAGE(results->offset() == (m_tupleSize * 3), os.str().c_str());

    // ack all of the data and re-verify block count
    os.str(""); os << "Allocated byte count - expected: " << 0 << ", actual: " << m_wrapper->testAllocatedBytesInEE();
    EXPECT_TRUE(m_wrapper->testAllocatedBytesInEE()== 0);
}

/**
 * Verify that a periodicFlush with distant TXN IDs works properly
 */
TEST_F(ExportTupleStreamTest, FarFutureFlush) {
    std::ostringstream os;
    for (int i = 1; i < 3; i++) {
        appendTuple(i, i);
        commit(i);
    }
    ASSERT_TRUE(checkTargetFlushTime(1));
    periodicFlush(-1, 99);
    ASSERT_TRUE(testNoStreamsToFlush());

    for (int i = 100; i < 103; i++) {
        appendTuple(i, i-97);
        commit(i);
    }
    // Make sure the flushTime is based on the first txn of the buffer
    ASSERT_TRUE(checkTargetFlushTime(100));
    periodicFlush(-1, 130);
    ASSERT_TRUE(testNoStreamsToFlush());

    // get the first buffer flushed
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    os << "USO in first block - expected: " << 0 << ", actual " << results->uso();
    ASSERT_TRUE_WITH_MESSAGE(results->uso() == 0, os.str().c_str());
    os.str(""); os << "Offset expected: " << (m_tupleSize * 2) << ", actual " << results->offset();
    ASSERT_TRUE_WITH_MESSAGE((results->offset() == m_tupleSize * 2), os.str().c_str());

    // now get the second
    ASSERT_FALSE(m_topend.exportBlocks.empty());
    results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    os << "uso in second block - expected: " << (m_tupleSize * 2) << ", actual " << results->uso();
    ASSERT_TRUE_WITH_MESSAGE(results->uso() == (m_tupleSize * 2), os.str().c_str());
    os << "Offset expected: " << (m_tupleSize * 3) << ", actual " << results->offset();
    ASSERT_TRUE_WITH_MESSAGE((results->offset() == m_tupleSize * 3), os.str().c_str());
}

/**
 * Fill a buffer by appending tuples that advance the last committed TXN
 */
TEST_F(ExportTupleStreamTest, Fill) {

    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= m_tuplesToFill; i++) {
        appendTuple(i, i);
        commit(i);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedExportBuffer);

    // now, drop in one more (this will push the previous buffer with committed Txns
    appendTuple(m_tuplesToFill + 1, m_tuplesToFill + 1);
    ASSERT_TRUE(checkTargetFlushTime(1));
    ASSERT_FALSE(m_topend.receivedExportBuffer);
    commit(m_tuplesToFill + 1);
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    // Make sure the target flush time is for the last txn
    ASSERT_TRUE(checkTargetFlushTime(m_tuplesToFill + 1));
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * m_tuplesToFill));
}

/**
 * Fill a buffer with a single TXN, and then finally close it in the next
 * buffer.
 */
TEST_F(ExportTupleStreamTest, FillSingleTxnAndAppend) {

    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= m_tuplesToFill; i++) {
        appendTuple(1, i);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedExportBuffer);

    // now, drop in one more on the same TXN ID
    appendTuple(1, m_tuplesToFill+1);

    // We shouldn't yet get a buffer because we haven't closed the current
    // transaction
    ASSERT_FALSE(m_topend.receivedExportBuffer);

    commit(1);

    // now, finally drop in a tuple that should not affect the flush time of the new buffer
    appendTuple(2, m_tuplesToFill+2);
    ASSERT_TRUE(checkTargetFlushTime(1));


    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * m_tuplesToFill));

    // The first buffer should not have any committed sequence numbers
    std::ostringstream os;
    os.str(""); os << "committedSequenceNumber expected: " << -1L << ", actual: " << results->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->getCommittedSequenceNumber() == -1L, os.str().c_str());
}


/**
 * Fill a buffer with a single TXN, close it with the first tuple in
 * the next buffer, and then roll back that tuple, and verify that our
 * committed buffer is still there.
 */
TEST_F(ExportTupleStreamTest, FillSingleTxnAndCommitWithRollback) {

    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= m_tuplesToFill; i++)
    {
        appendTuple(1, i);
    }
    commit(1);
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedExportBuffer);

    // now, drop in one more on a new TXN ID.  This should commit
    // the whole first buffer.  Roll back the new tuple and make sure
    // we have a good buffer
    rollbackMarker lastAppendTupleMarker = appendTuple(2, m_tuplesToFill+1);
    ASSERT_FALSE(m_topend.receivedExportBuffer);
    rollbackExportTo(lastAppendTupleMarker);
    ASSERT_FALSE(m_topend.receivedExportBuffer);
    ASSERT_TRUE(checkTargetFlushTime(1));

    // so flush and make sure we got something sane
    periodicFlush(-1, 1);
    ASSERT_TRUE(testNoStreamsToFlush());
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * m_tuplesToFill));

    // The first buffer should contain the whole transaction
    std::ostringstream os;
    os.str(""); os << "startSequenceNumber expected: " << 1 << ", actual: " << results->startSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->startSequenceNumber() == 1, os.str().c_str());
    os.str(""); os << "lastSequenceNumber expected: " << m_tuplesToFill << ", actual: " << results->lastSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->lastSequenceNumber() == m_tuplesToFill, os.str().c_str());
    os.str(""); os << "committedSequenceNumber expected: " << m_tuplesToFill << ", actual: " << results->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->getCommittedSequenceNumber() == m_tuplesToFill, os.str().c_str());
}

/**
 * Verify that several filled buffers all with one open transaction returns
 * nada.
 */
TEST_F(ExportTupleStreamTest, FillWithOneTxn) {

    // fill several buffers
    for (int i = 0; i <= (m_tuplesToFill + 10) * 3; i++)
    {
        appendTuple(2, i+1);
    }
    // We shouldn't yet get a buffer even though we've filled a bunch because
    // the transaction is still open.
    ASSERT_FALSE(m_topend.receivedExportBuffer);
    ASSERT_TRUE(checkTargetFlushTime(2));
}

/**
 * Simple rollback test, verify that we can rollback the first tuple,
 * append another tuple, and only get one tuple in the output buffer.
 */
TEST_F(ExportTupleStreamTest, RollbackFirstTupleForNewTxn) {

    rollbackMarker lastAppendTupleMarker = appendTuple(2, 1);
    ASSERT_TRUE(testNoStreamsToFlush());
    // rollback the first tuple
    rollbackExportTo(lastAppendTupleMarker);

    // write a new tuple and then flush the buffer
    appendTuple(3, 1);
    commit(3);
    ASSERT_TRUE(checkTargetFlushTime(3));
    periodicFlush(-1, 3);
    ASSERT_TRUE(testNoStreamsToFlush());

    // we should only have one tuple in the buffer
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), m_tupleSize);
    EXPECT_EQ(results->getRowCount(), 1);
}

/**
 * Simple rollback test, verify that we can rollback the first tuple,
 * append another tuple, and only get one tuple in the output buffer.
 */
TEST_F(ExportTupleStreamTest, RollbackFirstTupleAndRestartTxn) {

    rollbackMarker lastAppendTupleMarker = appendTuple(2, 1);
    // rollback the first tuple
    rollbackExportTo(lastAppendTupleMarker);

    // write a new tuple and then flush the buffer
    appendTuple(2, 1);
    commit(2);
    ASSERT_TRUE(checkTargetFlushTime(2));
    periodicFlush(-1, 2);
    ASSERT_TRUE(testNoStreamsToFlush());

    // we should only have one tuple in the buffer
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), m_tupleSize);
    EXPECT_EQ(results->getRowCount(), 1);
}


/**
 * Another simple rollback test, verify that a tuple in the middle of
 * a buffer can get rolled back and leave the committed transaction
 * untouched.
 */
TEST_F(ExportTupleStreamTest, RollbackMiddleTuple) {

    // append a bunch of tuples
    for (int i = 1; i <= m_tuplesToFill - 1; i++) {
        appendTuple(i, i);
        commit(i);
    }

    // add another and roll it back and flush
    rollbackMarker lastAppendTupleMarker = appendTuple(m_tuplesToFill, m_tuplesToFill);
    rollbackExportTo(lastAppendTupleMarker);
    // have not finished filling the first buffer yet so the flush should still be the first txn
    ASSERT_TRUE(checkTargetFlushTime(1));
    periodicFlush(-1, m_tuplesToFill);
    ASSERT_TRUE(testNoStreamsToFlush());

    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), ((m_tuplesToFill - 1) * m_tupleSize));
    EXPECT_EQ(results->getRowCount(), m_tuplesToFill - 1);
}

/**
 * Verify that a transaction can generate entire buffers, they can all
 * be rolled back, and the original committed bytes are untouched.
 */
TEST_F(ExportTupleStreamTest, RollbackWholeBufferRowByRow)
{
    // append a bunch of tuples
    for (int i = 1; i <= 3; i++) {
        appendTuple(i, i);
        commit(i);
    }

    // now, fill a couple of buffers with tuples from a single transaction
    std::stack<rollbackMarker> appendedTuples;

    // Only support row by row rollback in export
    for (int i = 0; i < (m_tuplesToFill + 10) * 2; i++)
    {
        appendedTuples.push(appendTuple(11, 4+i));
    }

    ASSERT_FALSE(m_topend.receivedExportBuffer);
    while (!appendedTuples.empty()) {
        rollbackMarker next = appendedTuples.top();
        rollbackExportTo(next);
        appendedTuples.pop();
    }

    ASSERT_FALSE(m_topend.receivedExportBuffer);
    ASSERT_EQ(m_wrapper->getCurrBlock()->getRowCount(), 3);
    ASSERT_TRUE(checkTargetFlushTime(1));
    periodicFlush(-1, 3);
    ASSERT_TRUE(testNoStreamsToFlush());

    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * 3));
    EXPECT_EQ(results->getRowCount(), 3);
}

TEST_F(ExportTupleStreamTest, RollbackWholeBufferByTxn)
{
    // append a bunch of tuples
    for (int i = 1; i <= 3; i++) {
        appendTuple(i, i);
        commit(i);
    }

    // now, fill a couple of buffers with tuples from a single transaction
    rollbackMarker lastAppendTupleMarker = appendTuple(11, 4);

    // Only support row by row rollback in export
    for (int i = 1; i < (m_tuplesToFill + 10) * 2; i++)
    {
        appendTuple(11, 4+i);
    }

    ASSERT_FALSE(m_topend.receivedExportBuffer);
    rollbackExportTo(lastAppendTupleMarker);

    ASSERT_FALSE(m_topend.receivedExportBuffer);
    ASSERT_EQ(m_wrapper->getCurrBlock()->getRowCount(), 3);
    ASSERT_TRUE(checkTargetFlushTime(1));
    periodicFlush(-1, 3);
    ASSERT_TRUE(testNoStreamsToFlush());

    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * 3));
    EXPECT_EQ(results->getRowCount(), 3);
}

TEST_F(ExportTupleStreamTest, PartialRollback)
{
    // append a bunch of tuples
    for (int i = 1; i <= 3; i++) {
        appendTuple(i, i);
        commit(i);
    }
    // now, fill a couple of buffers with tuples from a single transaction
    /*------------------------------
     * Txn1 | Txn2 | Txn3 | Txn11  |
     *------------------------------
     * Txn11 | <- rollback         |
     *------------------------------
     * Txn11                       |
     *------------------------------
     * Txn11 |
     *--------
     */
    size_t mark = 0;
    int64_t seqNo = 0;
    for (int i = 4; i < (m_tuplesToFill + 1) * 2; i++)
    {
        appendTuple(11, i);

        if (i == m_tuplesToFill + 1) {
            mark = m_wrapper->bytesUsed();
            seqNo = m_wrapper->getSequenceNumber();
        }
    }
    m_wrapper->rollbackExportTo(mark, seqNo);
    ASSERT_TRUE(checkTargetFlushTime(1));
    commit(11);
    EXPECT_GT(m_wrapper->getCurrBlock()->getRowCount(), 0);
    // Commit flushes the first buffer but leaves the tail of 11 (single row) as m_currBlock
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    m_wrapper->periodicFlush(-1, 11);
    ASSERT_TRUE(m_topend.receivedExportBuffer);
    boost::shared_ptr<ExportStreamBlock> results = m_topend.exportBlocks.front();
    m_topend.exportBlocks.pop_front();
    EXPECT_EQ(m_wrapper->getCurrBlock()->getRowCount(), 0);
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (m_tupleSize * m_tuplesToFill));
    EXPECT_EQ(results->getRowCount(), m_tuplesToFill);

    // First buffer should have last committed txn == 3
    std::ostringstream os;
    os.str(""); os << "committedSequenceNumber expected: " << 3 << ", actual: " << results->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->getCommittedSequenceNumber() == 3, os.str().c_str());

    results = m_topend.exportBlocks.front();
    EXPECT_EQ(results->uso(), m_tupleSize * m_tuplesToFill);
    EXPECT_EQ(results->offset(), (m_tupleSize * 1));
    EXPECT_EQ(results->getRowCount(), 1);

    os.str(""); os << "committedSequenceNumber expected: " << 14 << ", actual: " << results->getCommittedSequenceNumber();
    ASSERT_TRUE_WITH_MESSAGE(results->getCommittedSequenceNumber() == 14, os.str().c_str());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
