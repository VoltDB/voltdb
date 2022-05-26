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
#include <vector>
#include <deque>
#include "harness.h"

#include "common/executorcontext.hpp"
#include "common/Pool.hpp"
#include "common/UndoQuantum.h"
#include "common/Topend.h"
#include "common/FatalException.hpp"

#include "common/types.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "common/TupleSchema.h"
#include "common/tabletuple.h"
#include "common/StreamBlock.h"
#include "storage/streamedtable.h"

#include "boost/smart_ptr.hpp"

using namespace std;
using namespace voltdb;

const int COLUMN_COUNT = 5;
class MockVoltDBEngine : public VoltDBEngine {
public:
    MockVoltDBEngine()
    {
        m_enginesOldest = NULL;
        m_enginesNewest = NULL;
    }

    ~MockVoltDBEngine() { }


    virtual ExportTupleStream** getNewestExportStreamWithPendingRowsForAssignment() {
        return &m_enginesNewest;
    }

    virtual ExportTupleStream** getOldestExportStreamWithPendingRowsForAssignment() {
        return &m_enginesOldest;
    }


private:
    ExportTupleStream* m_enginesOldest;
    ExportTupleStream* m_enginesNewest;
};

class StreamedTableTest : public Test {
public:
    StreamedTableTest() {
        srand(0);
        m_topend = new DummyTopend();
        m_engine = new MockVoltDBEngine();
        m_pool = new Pool();
        m_quantum = createInstanceFromPool<UndoQuantum>(*m_pool, 0, m_pool);
        m_context = new ExecutorContext(0, 0, m_quantum, m_topend, m_pool,
                                        m_engine, "", 0, NULL, NULL, 0);

        // set up the schema used to fill the new buffer
        std::vector<ValueType> columnTypes;
        std::vector<int32_t> columnLengths;
        std::vector<bool> columnAllowNull;
        //Five columns
        m_columnNames = new std::vector<std::string>();
        m_columnNames->push_back("one");
        m_columnNames->push_back("two");
        m_columnNames->push_back("three");
        m_columnNames->push_back("four");
        m_columnNames->push_back("five");
        for (int i = 0; i < COLUMN_COUNT; i++) {
            columnTypes.push_back(ValueType::tINTEGER);
            columnLengths.push_back(NValue::getTupleStorageSize(ValueType::tINTEGER));
            columnAllowNull.push_back(false);
        }
        m_schema =
          TupleSchema::createTupleSchemaForTest(columnTypes,
                                         columnLengths,
                                         columnAllowNull);

        // set up the tuple we're going to use to fill the buffer
        // set the tuple's memory to zero
        ::memset(m_tupleMemory, 0, 8 * (COLUMN_COUNT + 1));

        // deal with the horrible hack that needs to set the first
        // value to true (rtb?? what is this horrible hack?)
        *(reinterpret_cast<bool*>(m_tupleMemory)) = true;
        m_tuple = new TableTuple(m_schema);
        m_tuple->move(m_tupleMemory);

        // a simple helper around the constructor that sets the
        // wrapper buffer size to the specified value
        m_table = StreamedTable::createForTest(1024, m_context, m_schema, "test", *m_columnNames);
    }

    void nextQuantum(int i, int64_t tokenOffset)
    {
        // Takes advantage of "grey box test" friend privileges on UndoQuantum.
       UndoQuantum::release(std::move(*m_quantum));
        m_quantum = createInstanceFromPool<UndoQuantum>(*m_pool, i + tokenOffset, m_pool);
        // quant, currTxnId, committedTxnId
        m_context->setupForPlanFragments(m_quantum, i, i, i - 1, 0, false);
    }

    virtual ~StreamedTableTest() {
        delete m_tuple;
        if (m_schema)
            TupleSchema::freeTupleSchema(m_schema);
        delete m_table;
        delete m_context;
        delete m_pool;
        delete m_engine;
        delete m_topend;
        delete m_columnNames;
        voltdb::globalDestroyOncePerProcess();
    }

protected:
    DummyTopend *m_topend;
    MockVoltDBEngine *m_engine;
    Pool *m_pool;
    UndoQuantum *m_quantum;
    ExecutorContext *m_context;

    StreamedTable *m_table;
    TupleSchema* m_schema;
    char m_tupleMemory[(COLUMN_COUNT + 1) * 8];
    TableTuple* m_tuple;
    std::vector<std::string>* m_columnNames;
};

/**
 * The goal of this test is simply to run through the mechanics.
 * Fill a buffer repeatedly and make sure nothing breaks.
 */
TEST_F(StreamedTableTest, BaseCase) {
    int64_t tokenOffset = 2000; // just so tokens != txnIds

    // repeat for more tuples than fit in the default buffer
    for (int i = 1; i < 1000; i++) {

        // pretend to be a plan fragment execution
        nextQuantum(i, tokenOffset);

        // fill a tuple
        for (int col = 0; col < COLUMN_COUNT; col++) {
            int value = rand();
            m_tuple->setNValue(col, ValueFactory::getIntegerValue(value));
        }

        m_table->insertTuple(*m_tuple);
    }
    UndoQuantum::release(std::move(*m_quantum));
    // a negative flush implies "now". this helps valgrind heap block test
    m_table->flushOldTuples(-1);

    // poll from the table and make sure we get "stuff", releasing as
    // we go.  This just makes sure we don't fail catastrophically and
    // that things are basically as we expect.
    deque<boost::shared_ptr<ExportStreamBlock> >::iterator begin = m_topend->exportBlocks.begin();
    int64_t uso = (*begin)->uso();
    EXPECT_EQ(uso, 0);
    size_t offset = (*begin)->offset();
    EXPECT_TRUE(offset != 0);
    while (begin != m_topend->exportBlocks.end()) {
        begin++;
        if (begin == m_topend->exportBlocks.end()) {
            break;
        }

        boost::shared_ptr<ExportStreamBlock> block = *begin;
        uso = block->uso();
        EXPECT_EQ(uso, offset);
        offset += block->offset();
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
