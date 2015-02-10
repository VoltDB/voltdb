/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "harness.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/StreamBlock.h"
#include "storage/DRTupleStream.h"
#include "common/Topend.h"
#include "common/executorcontext.hpp"
#include "boost/smart_ptr.hpp"

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 5;
// Annoyingly, there's no easy way to compute the exact DR tuple
// size without incestuously using code we're trying to test.  I've
// pre-computed this magic size for an Exported tuple of 5 integer
// columns, which includes:
// 1 Export header column * sizeof(int8_t) = 1
// 2 bytes for null mask (10 columns rounds to 16, /8 = 2) = 2
// sizeof(int32_t) for row header = 4
// 5 * sizeof(int32_t) for tuple data = 40
// total: 67
const int MAGIC_TUPLE_SIZE = 39;
const int MAGIC_TRANSACTION_SIZE = 36;
const int MAGIC_BEGIN_SIZE = 22;
const int MAGIC_END_SIZE = MAGIC_TRANSACTION_SIZE - MAGIC_BEGIN_SIZE;
const int MAGIC_TUPLE_PLUS_TRANSACTION_SIZE = MAGIC_TUPLE_SIZE + MAGIC_TRANSACTION_SIZE;
// 1k buffer
const int BUFFER_SIZE = 983;

class DRTupleStreamTest : public Test {
public:
    DRTupleStreamTest() : m_schema(NULL), m_tuple(NULL),
        m_context(new ExecutorContext( 1, 1, NULL, &m_topend, NULL, NULL, "localhost", 2, &m_wrapper)) {
        m_wrapper.m_enabled = true;
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
          TupleSchema::createTupleSchemaForTest(columnTypes,
                                         columnLengths,
                                         columnAllowNull);

        // allocate a new buffer and wrap it
        m_wrapper.configure(1);

        // excercise a smaller buffer capacity
        m_wrapper.setDefaultCapacity(BUFFER_SIZE + 8);

        // set up the tuple we're going to use to fill the buffer
        // set the tuple's memory to zero
        ::memset(m_tupleMemory, 0, 8 * (COLUMN_COUNT + 1));

        // deal with the horrible hack that needs to set the first
        // value to true (rtb?? what is this horrible hack?)
        *(reinterpret_cast<bool*>(m_tupleMemory)) = true;
        m_tuple = new TableTuple(m_schema);
        m_tuple->move(m_tupleMemory);
    }

    size_t appendTuple(int64_t lastCommittedSpHandle, int64_t currentSpHandle)
    {
        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
        }
        // append into the buffer
        return m_wrapper.appendTuple(lastCommittedSpHandle, tableHandle, currentSpHandle,
                               currentSpHandle, *m_tuple,
                               DR_RECORD_INSERT);
    }

    virtual ~DRTupleStreamTest() {
        delete m_tuple;
        if (m_schema)
            TupleSchema::freeTupleSchema(m_schema);
    }

protected:
    DRTupleStream m_wrapper;
    TupleSchema* m_schema;
    char m_tupleMemory[(COLUMN_COUNT + 1) * 8];
    TableTuple* m_tuple;
    DummyTopend m_topend;
    boost::scoped_ptr<ExecutorContext> m_context;
    char tableHandle[20];
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
TEST_F(DRTupleStreamTest, DoOneTuple)
{

    // write a new tuple and then flush the buffer
    appendTuple(1, 2);
    m_wrapper.periodicFlush(-1, 2);

    // we should only have one tuple in the buffer
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_PLUS_TRANSACTION_SIZE);
}

/**
 * Test the really basic operation order
 */
TEST_F(DRTupleStreamTest, BasicOps)
{
    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper.periodicFlush(-1, 9);

    for (int i = 10; i < 20; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper.periodicFlush(-1, 19);

    // get the first buffer flushed
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 9));

    // now get the second
    ASSERT_FALSE(m_topend.blocks.empty());
    results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 9));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 10));
}

/**
 * Verify that a periodicFlush with distant TXN IDs works properly
 */
TEST_F(DRTupleStreamTest, FarFutureFlush)
{
    for (int i = 1; i < 10; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper.periodicFlush(-1, 99);

    for (int i = 100; i < 110; i++)
    {
        appendTuple(i-1, i);
    }
    m_wrapper.periodicFlush(-1, 130);

    // get the first buffer flushed
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 9));

    // now get the second
    ASSERT_FALSE(m_topend.blocks.empty());
    results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 9));
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 10));
}

/**
 * Fill a buffer by appending tuples that advance the last committed TXN
 */
TEST_F(DRTupleStreamTest, Fill) {

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_PLUS_TRANSACTION_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(i-1, i);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // now, drop in one more
    appendTuple(tuples_to_fill, tuples_to_fill + 1);

    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * tuples_to_fill));
}

/**
 * Fill a buffer with a single TXN, and then finally close it in the next
 * buffer.
 */
TEST_F(DRTupleStreamTest, FillSingleTxnAndAppend) {

    int tuples_to_fill = (BUFFER_SIZE - MAGIC_TRANSACTION_SIZE) / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // now, drop in one more on the same TXN ID
    appendTuple(0, 1);

    // We shouldn't yet get a buffer because we haven't closed the current
    // transaction
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // now, finally drop in a tuple that closes the first TXN
    appendTuple(1, 2);

    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_BEGIN_SIZE + (MAGIC_TUPLE_SIZE * tuples_to_fill));
}

/**
 * Fill a buffer with a single TXN, and then finally close it in the next
 * buffer using periodicFlush
 */
TEST_F(DRTupleStreamTest, FillSingleTxnAndFlush) {

    int tuples_to_fill = (BUFFER_SIZE - MAGIC_TRANSACTION_SIZE) / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // now, drop in one more on the same TXN ID
    appendTuple(0, 1);

    // We shouldn't yet get a buffer because we haven't closed the current
    // transaction
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // Now, flush the buffer with the tick
    m_wrapper.periodicFlush(-1, 1);

    // should be able to get 2 buffers, one full and one with one tuple
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_BEGIN_SIZE + (MAGIC_TUPLE_SIZE * tuples_to_fill));

    results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), MAGIC_BEGIN_SIZE + (MAGIC_TUPLE_SIZE * tuples_to_fill));
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE + MAGIC_END_SIZE);
}

/**
 * Fill a buffer with a single TXN, close it with the first tuple in
 * the next buffer, and then roll back that tuple, and verify that our
 * committed buffer is still there.
 */
TEST_F(DRTupleStreamTest, FillSingleTxnAndCommitWithRollback) {

    int tuples_to_fill = (BUFFER_SIZE - MAGIC_TRANSACTION_SIZE) / MAGIC_TUPLE_SIZE;
    // fill with just enough tuples to avoid exceeding buffer
    size_t mark = 0;
    for (int i = 1; i <= tuples_to_fill; i++)
    {
        mark = appendTuple(0, 1);
    }
    // We shouldn't yet get a buffer because we haven't forced the
    // generation of a new one by exceeding the current one.
    ASSERT_FALSE(m_topend.receivedDRBuffer);

    // now, drop in one more on a new TXN ID.  This should commit
    // the whole first buffer.  Roll back the new tuple and make sure
    // we have a good buffer
    appendTuple(1, 2);
    m_wrapper.rollbackTo(mark);

    // so flush and make sure we got something sane
    m_wrapper.periodicFlush(-1, 1);
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), MAGIC_TRANSACTION_SIZE + (MAGIC_TUPLE_SIZE * tuples_to_fill));
}

/**
 * Verify that several filled buffers all with one open transaction returns
 * nada.
 */
TEST_F(DRTupleStreamTest, FillWithOneTxn) {

    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    // fill several buffers
    for (int i = 0; i <= (tuples_to_fill + 10) * 3; i++)
    {
        appendTuple(1, 2);
    }
    // We shouldn't yet get a buffer even though we've filled a bunch because
    // the transaction is still open.
    ASSERT_FALSE(m_topend.receivedDRBuffer);
}

/**
 * Simple rollback test, verify that we can rollback the first tuple,
 * append another tuple, and only get one tuple in the output buffer.
 */
TEST_F(DRTupleStreamTest, RollbackFirstTuple)
{

    appendTuple(1, 2);
    // rollback the first tuple
    m_wrapper.rollbackTo(0);

    // write a new tuple and then flush the buffer
    appendTuple(2, 3);
    m_wrapper.periodicFlush(-1, 3);

    // we should only have one tuple in the buffer
    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    //The rollback emits an end transaction record spuriously, we'll just ignore it
    EXPECT_EQ(results->offset(), MAGIC_TUPLE_PLUS_TRANSACTION_SIZE + MAGIC_END_SIZE);
}


/**
 * Another simple rollback test, verify that a tuple in the middle of
 * a buffer can get rolled back and leave the committed transaction
 * untouched.
 */
TEST_F(DRTupleStreamTest, RollbackMiddleTuple)
{
    // append a bunch of tuples
    for (int i = 1; i <= 10; i++)
    {
         appendTuple(i-1, i);
    }

    // add another and roll it back and flush
    size_t mark = appendTuple(10, 11);
    m_wrapper.rollbackTo(mark);
    m_wrapper.periodicFlush(-1, 11);

    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 10) + MAGIC_TRANSACTION_SIZE);
}

/**
 * Verify that a transaction can generate entire buffers, they can all
 * be rolled back, and the original committed bytes are untouched.
 */
TEST_F(DRTupleStreamTest, RollbackWholeBuffer)
{
    // append a bunch of tuples
    for (int i = 1; i <= 10; i++)
    {
        appendTuple(i-1, i);
    }

    // now, fill a couple of buffers with tuples from a single transaction
    size_t mark = 0;
    int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE;
    for (int i = 0; i < (tuples_to_fill + 10) * 2; i++)
    {
        if (mark != 0) {
            appendTuple(10, 11);
        } else {
            mark = appendTuple(10, 11);
        }
    }
    m_wrapper.rollbackTo(mark);
    m_wrapper.periodicFlush(-1, 11);

    ASSERT_TRUE(m_topend.receivedDRBuffer);
    boost::shared_ptr<StreamBlock> results = m_topend.blocks.front();
    m_topend.blocks.pop_front();
    EXPECT_EQ(results->uso(), 0);
    EXPECT_EQ(results->offset(), (MAGIC_TUPLE_PLUS_TRANSACTION_SIZE * 10) + MAGIC_TRANSACTION_SIZE);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
