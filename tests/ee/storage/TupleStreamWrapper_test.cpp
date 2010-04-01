/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
#include "common/Topend.h"
#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "storage/TupleStreamWrapper.h"

using namespace voltdb;

const int COLUMN_COUNT = 5;
// 5 kilobytes of buffer
const int BUFFER_SIZE = 1024 * 5;

class MockTopend : public Topend {
  public:
    MockTopend() {
        m_handoffcount = 0;
        m_bytesHandedOff = 0;
    }

    void handoffReadyELBuffer(
        char* bufferPtr, int32_t bytesUsed, CatalogId tableId)
    {
        m_handoffcount++;
        m_bytesHandedOff += bytesUsed;
        delete[] bufferPtr;
    }

    virtual char* claimManagedBuffer(int32_t desiredSizeInBytes)
    {
        return new char[desiredSizeInBytes];
    }

    virtual void releaseManagedBuffer(char* bufferPtr)
    {
        delete[] bufferPtr;
    }

    virtual int loadNextDependency(
        int32_t dependencyId, Pool *pool, Table* destination)
    {
        return 0;
    }

    virtual void crashVoltDB(FatalException e) {

    }

    int m_handoffcount;
    int m_bytesHandedOff;
};

class TupleStreamWrapperTest : public Test {
public:
    TupleStreamWrapperTest() : m_wrapper(NULL), m_schema(NULL), m_tuple(NULL) {
        srand(0);

        m_topend = new MockTopend();

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
        m_wrapper = new TupleStreamWrapper(1, 1, 1, m_topend, 1);

        // excercise a smaller buffer capacity
        m_wrapper->setDefaultCapacity(1024);

        // set up the tuple we're going to use to fill the buffer
        // set the tuple's memory to zero
        ::memset(m_tupleMemory, 0, 8 * (COLUMN_COUNT + 1));

        // deal with the horrible hack that needs to set the first
        // value to true (rtb?? what is this horrible hack?)
        *(reinterpret_cast<bool*>(m_tupleMemory)) = true;
        m_tuple = new TableTuple(m_schema);
        m_tuple->move(m_tupleMemory);
    }

    virtual ~TupleStreamWrapperTest() {
        m_wrapper->cleanupManagedBuffers(NULL);
        delete m_wrapper;
        delete m_tuple;
        if (m_schema)
            TupleSchema::freeTupleSchema(m_schema);
        delete m_topend;
    }

protected:
    TupleStreamWrapper* m_wrapper;
    TupleSchema* m_schema;
    char m_tupleMemory[(COLUMN_COUNT + 1) * 8];
    TableTuple* m_tuple;
    MockTopend *m_topend;
};

/**
 * The goal of this test is simply to run through the mechanics.
 * Fill a buffer repeatedly and make sure nothing breaks.
 */
TEST_F(TupleStreamWrapperTest, Fill) {
    int flushcount = m_topend->m_handoffcount;

    // repeat for more tuples than fit in the default buffer
    for (int i = 0; i < 100; i++) {

        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
        }

        // append into the buffer, switching buffers if full
        m_wrapper->appendTuple(i, 1, 1, *m_tuple, TupleStreamWrapper::INSERT);
    }
    // make sure we used more than one buffer
    EXPECT_GT(m_topend->m_handoffcount, flushcount);

    // cleanup for valgrind with a txnid > than the max value of i, above.
    m_wrapper->flushOldTuples(1000, 1000);
}

TEST_F(TupleStreamWrapperTest, Flush) {
    int64_t txnid = 1024;
    int64_t seqno = 10;
    int64_t tmstmp = 1000;

    // verify the initial state of the stream
    EXPECT_EQ(0, m_wrapper->bytesUsed());

    // insert a tuple.
    for (int col = 0; col < COLUMN_COUNT; col++) {
        int value = rand();
        m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
    }
    m_wrapper->appendTuple(
        txnid++, seqno, tmstmp,
        *m_tuple, TupleStreamWrapper::INSERT);

    // verify content was pushed in to the stream
    // 6 col metadata. 5 col ints. 2 bytes nullhdr. 4 byte row length
    EXPECT_EQ((40 + 1) + (COLUMN_COUNT*8) + 2 + 4, m_wrapper->bytesUsed());

    // append a second tuple and reverify offset
    size_t mark = m_wrapper->appendTuple(
        txnid++, seqno, tmstmp,
        *m_tuple, TupleStreamWrapper::INSERT);

    // the first mark should be equal to the bytesUsed after the first insert
    EXPECT_EQ(mark, (40 + 1) + (COLUMN_COUNT*8) + 2 + 4);

    // should have doubled the content of the stream with this second insert
    EXPECT_EQ(2*((40 + 1) + (COLUMN_COUNT*8) + 2 + 4),
              m_wrapper->bytesUsed());

    // flush - this is the function being tested, purportedly
    m_wrapper->flushOldTuples(txnid, seqno);

    // old buffer flushed bytes. (see size calc above)
    EXPECT_EQ(87 * 2, m_topend->m_bytesHandedOff);

    m_wrapper->flushOldTuples(txnid, seqno);

    // old buffer flushed bytes. (see size calc above)
    EXPECT_EQ(87 * 2, m_topend->m_bytesHandedOff);

}

TEST_F(TupleStreamWrapperTest, RollbackFirstTuple) {
    int64_t txnid = 1;
    int64_t seqno = 0;
    int64_t tmstmp = 1000;
    size_t mark = 0;

    mark = m_wrapper->appendTuple(
        txnid++, seqno, tmstmp, *m_tuple, TupleStreamWrapper::INSERT);

    m_wrapper->rollbackTo(mark);
    m_wrapper->flushOldTuples(txnid++, tmstmp);

    // stream should have no content
    EXPECT_EQ(0, m_topend->m_bytesHandedOff);

    mark = m_wrapper->appendTuple(
        txnid++, seqno, tmstmp, *m_tuple, TupleStreamWrapper::INSERT);
    m_wrapper->flushOldTuples(txnid++, tmstmp);

    // stream should have a single tuple
    EXPECT_EQ(87, m_topend->m_bytesHandedOff);
}

TEST_F(TupleStreamWrapperTest, RollbackLastTuple) {
    int64_t txnid = 1;
    int64_t seqno = 0;
    int64_t tmstmp = 1000;
    size_t mark = 0;

    for (int i=0; i < 5; i++) {
        mark = m_wrapper->appendTuple(
            txnid++, seqno, tmstmp, *m_tuple, TupleStreamWrapper::INSERT);
        EXPECT_EQ(mark, i * 87);
    }

    m_wrapper->rollbackTo(mark);
    m_wrapper->flushOldTuples(txnid++, tmstmp);

    // stream should have 4 tuples of content
    EXPECT_EQ(4 * 87, m_topend->m_bytesHandedOff);

    for (int i=0; i < 5; i++) {
        mark = m_wrapper->appendTuple(
            txnid++, seqno, tmstmp, *m_tuple, TupleStreamWrapper::INSERT);
        EXPECT_EQ(mark, (i + 4) * 87);
    }
    m_wrapper->flushOldTuples(txnid++, tmstmp);

    // 9 total tuples
    EXPECT_EQ(9 * 87, m_topend->m_bytesHandedOff);
}


int main() {
    return TestSuite::globalInstance()->runAll();
}
