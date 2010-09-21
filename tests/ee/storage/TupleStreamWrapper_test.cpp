/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
#include "harness.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "storage/StreamBlock.h"
#include "storage/TupleStreamWrapper.h"

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 5;
// Annoyingly, there's no easy way to compute the exact Exported tuple
// size without incestuously using code we're trying to test.  I've
// pre-computed this magic size for an Exported tuple of 5 integer
// columns, which includes:
// 5 Export header columns * sizeof(int64_t) = 40
// 1 Export header column * sizeof(int64_t) = 8
// 2 bytes for null mask (10 columns rounds to 16, /8 = 2) = 2
// sizeof(int32_t) for row header = 4
// 5 * sizeof(int64_t) for tuple data = 40
// total: 87
const int MAGIC_TUPLE_SIZE = 94;
// 1k buffer
const int BUFFER_SIZE = 1024;

class TupleStreamWrapperTest : public Test {
public:
    TupleStreamWrapperTest() : m_wrapper(NULL), m_schema(NULL), m_tuple(NULL) {
        srand(0);

        // set up the schema used to fill the new buffer
        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull;
        for (int i = 0; i < COLUMN_COUNT; i++) {
            columnTypes.push_back(VALUE_TYPE_INTEGER);
            columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
            columnAllowNull.push_back(false);
        }
        m_schema =
          TupleSchema::createTupleSchema(columnTypes,
                                         columnLengths,
                                         columnAllowNull,
                                         true);

        // allocate a new buffer and wrap it
        m_wrapper = new TupleStreamWrapper(1, 1, 1);

        // excercise a smaller buffer capacity
        m_wrapper->setDefaultCapacity(BUFFER_SIZE);

        // set up the tuple we're going to use to fill the buffer
        // set the tuple's memory to zero
        ::memset(m_tupleMemory, 0, 8 * (COLUMN_COUNT + 1));

        // deal with the horrible hack that needs to set the first
        // value to true (rtb?? what is this horrible hack?)
        *(reinterpret_cast<bool*>(m_tupleMemory)) = true;
        m_tuple = new TableTuple(m_schema);
        m_tuple->move(m_tupleMemory);
    }

    void appendTuple(int64_t lastCommittedTxnId, int64_t currentTxnId)
    {
        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
        }
        // append into the buffer
        m_wrapper->appendTuple(lastCommittedTxnId,
                               currentTxnId, 1, 1, *m_tuple,
                               TupleStreamWrapper::INSERT);
    }

    virtual ~TupleStreamWrapperTest() {
        m_wrapper->cleanupManagedBuffers();
        delete m_wrapper;
        delete m_tuple;
        if (m_schema)
            TupleSchema::freeTupleSchema(m_schema);
    }

protected:
    TupleStreamWrapper* m_wrapper;
    TupleSchema* m_schema;
    char m_tupleMemory[(COLUMN_COUNT + 1) * 8];
    TableTuple* m_tuple;
};

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
TEST_F(TupleStreamWrapperTest, DoOneTuple)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // write a new tuple and then flush the buffer
    appendTuple(0, 1);
    m_wrapper->periodicFlush(-1, 0, 1, 1);

    // we should only have one tuple in the buffer
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE);
    EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE);
}

/**
 * Test the really basic operation order
 */
TEST_F(TupleStreamWrapperTest, BasicOps)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 10);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // get the first buffer flushed
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 9));

    // now get the second
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 10));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 10));

    // additional polls should return the current uso and no data
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Verify that a periodicFlush with distant TXN IDs works properly
 */
TEST_F(TupleStreamWrapperTest, FarFutureFlush)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 99, 100);

    for (int i = 100; i < 110; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 130, 131);

    // get the first buffer flushed
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 9));

    // now get the second
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 10));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 10));

    // additional polls should return the current uso and no data
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Fill a buffer by appending tuples that advance the last committed TXN
 */
TEST_F(TupleStreamWrapperTest, Fill) {

    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(i-1, i);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, drop in one more
    appendTuple(tuples_to_fill, tuples_to_fill + 1);

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
}

/**
 * Fill a buffer with a single TXN, and then finally close it in the next
 * buffer.
 */
TEST_F(TupleStreamWrapperTest, FillSingleTxnAndAppend) {

    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, drop in one more on the same TXN ID
    appendTuple(0, 1);

    // We shouldn't yet get a buffer because we haven't closed the current
    // transaction
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, finally drop in a tuple that closes the first TXN
    appendTuple(1, 2);

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
}

/**
 * Fill a buffer with a single TXN, and then finally close it in the next
 * buffer using periodicFlush
 */
TEST_F(TupleStreamWrapperTest, FillSingleTxnAndFlush) {

    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, drop in one more on the same TXN ID
    appendTuple(0, 1);

    // We shouldn't yet get a buffer because we haven't closed the current
    // transaction
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // Now, flush the buffer with the tick
    m_wrapper->periodicFlush(-1, 0, 1, 1);

    // should be able to get 2 buffers, one full and one with one tuple
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * tuples_to_fill));

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE);
    EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE);
}

/**
 * Fill a buffer with a single TXN, close it with the first tuple in
 * the next buffer, and then roll back that tuple, and verify that our
 * committed buffer is still there.
 */
TEST_F(TupleStreamWrapperTest, FillSingleTxnAndCommitWithRollback) {

    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, drop in one more on a new TXN ID.  This should commit
    // the whole first buffer.  Roll back the new tuple and make sure
    // we have a good buffer
    size_t mark = m_wrapper->bytesUsed();
    appendTuple(1, 2);
    m_wrapper->rollbackTo(mark);

    // we'll get the old fake m_currBlock buffer first
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // so flush and make sure we got something sane
    m_wrapper->periodicFlush(-1, 0, 1, 2);
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * tuples_to_fill));
}

/**
 * Verify that several filled buffers all with one open transaction returns
 * nada.
 */
TEST_F(TupleStreamWrapperTest, FillWithOneTxn) {

    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill several buffers
    for (int i = 0; i <= (tuples_to_fill + 10) * 3; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer even though we've filled a bunch because
    // the transaction is still open.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Simple rollback test, verify that we can rollback the first tuple,
 * append another tuple, and only get one tuple in the output buffer.
 */
TEST_F(TupleStreamWrapperTest, RollbackFirstTuple)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    appendTuple(0, 1);
    // rollback the first tuple
    m_wrapper->rollbackTo(0);

    // write a new tuple and then flush the buffer
    appendTuple(0, 1);
    m_wrapper->periodicFlush(-1, 0, 1, 1);

    // we should only have one tuple in the buffer
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE);
    EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE);
}

/**
 * Another simple rollback test, verify that a tuple in the middle of
 * a buffer can get rolled back and leave the committed transaction
 * untouched.
 */
TEST_F(TupleStreamWrapperTest, RollbackMiddleTuple)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // append a bunch of tuples
    for (int i = 1; i <= 10; i++)
    {
        appendTuple(i-1, i);
    }

    // add another and roll it back and flush
    size_t mark = m_wrapper->bytesUsed();
    appendTuple(10, 11);
    m_wrapper->rollbackTo(mark);
    m_wrapper->periodicFlush(-1, 0, 10, 11);

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 10));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 10));
}

/**
 * Verify that a transaction can generate entire buffers, they can all
 * be rolled back, and the original committed bytes are untouched.
 */
TEST_F(TupleStreamWrapperTest, RollbackWholeBuffer)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // append a bunch of tuples
    for (int i = 1; i <= 10; i++)
    {
        appendTuple(i-1, i);
    }

    // now, fill a couple of buffers with tuples from a single transaction
    size_t mark = m_wrapper->bytesUsed();
    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    for (int i = 0; i < (tuples_to_fill + 10) * 2; i++)
    {
        appendTuple(10, 11);
    }
    m_wrapper->rollbackTo(mark);
    m_wrapper->periodicFlush(-1, 0, 10, 11);

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 10));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 10));
}

/**
 * Test basic release.  create two buffers, release the first one, and
 * ensure that our next poll returns the second one.
 */
TEST_F(TupleStreamWrapperTest, SimpleRelease)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // release the first buffer
    bool released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 9));
    EXPECT_TRUE(released);

    // now get the second
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 10));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 10));
}

/**
 * Test that attempting to release uncommitted bytes only returns what
 * is committed
 */
TEST_F(TupleStreamWrapperTest, ReleaseUncommitted)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // Add some committed tuples
    for (int i = 1; i < 4; i++)
    {
        appendTuple(i-1, i);
    }

    // now, add some uncommitted data
    for (int i = 4; i < 10; i++)
    {
        appendTuple(3, 4);
    }

    // release part of the committed data
    bool released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 2));
    EXPECT_TRUE(released);

    // now try to release everything
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 10));
    EXPECT_TRUE(released);

    // now, poll and verify that we have moved to the end of the committed data
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 3));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 3));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // now, commit everything and make sure we get the long transaction
    m_wrapper->periodicFlush(-1, 0, 19, 19);
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 3));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 6));
}

/**
 * Test that attempting to release on a non-buffer boundary will return
 * the remaining un-acked partial buffer when we poll
 */
TEST_F(TupleStreamWrapperTest, ReleaseOnNonBoundary)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // release part of the first buffer
    bool released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 4));
    EXPECT_TRUE(released);

    // get the first and make we get the remainder
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 5));
}

/**
 * Test that releasing everything in steps and then polling results in
 * the right StreamBlock
 */
TEST_F(TupleStreamWrapperTest, ReleaseAllInAlignedSteps)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // release the first buffer
    bool released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 9));
    EXPECT_TRUE(released);

    // release the second buffer
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
    EXPECT_TRUE(released);

    // now get the current state
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Test that releasing multiple blocks at once and then polling results in
 * the right StreamBlock
 */
TEST_F(TupleStreamWrapperTest, ReleaseAllAtOnce)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // release everything
    bool released;
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
    EXPECT_TRUE(released);

    // now get the current state
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Test that releasing bytes earlier than recorded history just succeeds
 */
TEST_F(TupleStreamWrapperTest, ReleasePreHistory)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 9, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper->periodicFlush(-1, 0, 19, 19);

    // release everything
    bool released;
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
    EXPECT_TRUE(released);

    // now release something early in what just got released
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 4));
    EXPECT_TRUE(released);

    // now get the current state
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

/**
 * Test that releasing at a point in the current stream block
 * works correctly
 */
TEST_F(TupleStreamWrapperTest, ReleaseInCurrentBlock)
{
    // we get nothing with no data
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // Fill the current buffer with some stuff
    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }

    // release part of the way into the current buffer
    bool released;
    released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 4));
    EXPECT_TRUE(released);

    // Poll and verify that we get a StreamBlock that indicates that
    // there's no data available at the new release point
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // Now, flush the buffer and then verify that the next poll gets
    // the right partial result
    m_wrapper->periodicFlush(-1, 0, 9, 9);
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
    EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 5));
}

/**
 * Test that reset allows re-polling data
 */
TEST_F(TupleStreamWrapperTest, ResetInFirstBlock)
{
    // Fill the current buffer with some stuff
    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }

    // Flush all data
    m_wrapper->periodicFlush(-1, 0, 10, 10);

    // Poll and verify that data is returned
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE * 9);
    EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE * 9);

    // Poll again and see that an empty block is returned
    // (Not enough data to require more than one block)
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), MAGIC_TUPLE_SIZE * 9);
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedUso(), results->uso());

    // Reset the stream and get the first poll again
    m_wrapper->resetPollMarker();
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE * 9);
    EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE * 9);
}

TEST_F(TupleStreamWrapperTest, ResetInPartiallyAckedBlock)
{
    // Fill the current buffer with some stuff
    for (int i = 1; i < 10; i++) {
        appendTuple(i-1, i);
    }

    // Ack the first 4 tuples.
    bool released = m_wrapper->releaseExportBytes(MAGIC_TUPLE_SIZE * 4);
    EXPECT_TRUE(released);

    // Poll and verify that we get a StreamBlock that indicates that
    // there's no data available at the new release point
    // (because the full block is not committed)
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);

    // reset the poll point; this should not change anything.
    m_wrapper->resetPollMarker();

    // Same verification as above.
    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
    EXPECT_EQ(results->offset(), 0);
    EXPECT_EQ(results->unreleasedSize(), 0);
}

TEST_F(TupleStreamWrapperTest, ResetInPartiallyAckedCommittedBlock)
{
    // write some, committing as tuples are added
    int i = 0;  // keep track of the current txnid
    for (i = 1; i < 10; i++) {
        appendTuple(i-1, i);
    }

    // partially ack the buffer
    bool released = m_wrapper->releaseExportBytes(MAGIC_TUPLE_SIZE * 4);
    EXPECT_TRUE(released);

    // wrap and require a new buffer
    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE + 10;
    for (int j = 0; j < tuples_to_fill; j++, i++) {
        appendTuple(i, i+1);
    }

    // poll - should get the content post release (in the old buffer)
    StreamBlock* results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
    EXPECT_TRUE(results->offset() > 0);

    // poll again.
     m_wrapper->getCommittedExportBytes();

    // reset. Aftwards, should be able to get original block back
    m_wrapper->resetPollMarker();

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
    EXPECT_TRUE(results->offset() > 0);

    // flush should also not change the reset base poll point
    m_wrapper->periodicFlush(-1, 0, i, i);
    m_wrapper->resetPollMarker();

    results = m_wrapper->getCommittedExportBytes();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
    EXPECT_TRUE(results->offset() > 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
