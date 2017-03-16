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

#include "common/TupleSchemaBuilder.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "storage/LargeTableIterator.h"
#include "storage/LargeTempTable.h"
#include "storage/tablefactory.h"

using namespace voltdb;

class LargeTempTableTest : public Test {

};

template<typename T>
NValue nvalueFromNative(T val);

template<>
NValue nvalueFromNative(int64_t val) {
    return ValueFactory::getBigIntValue(val);
}

template<>
NValue nvalueFromNative(int val) {
    return nvalueFromNative(static_cast<int64_t>(val));
}


template<>
NValue nvalueFromNative(const char* val) {
    return ValueFactory::getTempStringValue(val, strlen(val));
}

template<>
NValue nvalueFromNative(double val) {
    return ValueFactory::getDoubleValue(val);
}

void setTupleValuesHelper(TableTuple* tuple, int index) {
    assert(tuple->getSchema()->columnCount() == index);
}

template<typename T, typename ... Args>
void setTupleValuesHelper(TableTuple* tuple, int index, T arg, Args... args) {
    tuple->setNValue(index, nvalueFromNative(arg));
    setTupleValuesHelper(tuple, index + 1, args...);
}

template<typename ... Args>
void setTupleValues(TableTuple* tuple, Args... args) {
    setTupleValuesHelper(tuple, 0, args...);
}

TEST_F(LargeTempTableTest, Basic) {
    TupleSchemaBuilder schemaBuilder(3);

    schemaBuilder.setColumnAtIndex(0, VALUE_TYPE_BIGINT);
    schemaBuilder.setColumnAtIndex(1, VALUE_TYPE_DOUBLE);
    schemaBuilder.setColumnAtIndex(2, VALUE_TYPE_VARCHAR, 128);

    TupleSchema* schema = schemaBuilder.build();

    std::vector<std::string> names{"pk", "val", "text"};

    voltdb::LargeTempTable *ltt = TableFactory::buildLargeTempTable(
        "ltmp",
        schema,
        names);

    ltt->incrementRefcount();

    StandAloneTupleStorage tupleWrapper(schema);

    TableTuple tuple = tupleWrapper.tuple();

    setTupleValues(&tuple, 66, 3.14, "foo");
    ltt->insertTuple(tuple);

    setTupleValues(&tuple, 67, 6.28, "bar");
    ltt->insertTuple(tuple);

    setTupleValues(&tuple, 68, 7.77, "baz");
    ltt->insertTuple(tuple);

    LargeTableIterator iter = ltt->largeIterator();
    (void)iter;
    // TableTuple iterTuple(ltt->schema());

    // while (iter.hasNext(iterTuple)) {

    // }

    // tuple.setNValue(0, ValueFactory::getBigIntValue(66));
    // tuple.setNValue(1, ValueFactory::getDoubleValue(3.14));
    // tuple.setNValue(2, ValueFactory::getTempStringValue("foo"));


    ltt->decrementRefcount();
}

int main() {

    assert (voltdb::ExecutorContext::getExecutorContext() == NULL);

    boost::scoped_ptr<voltdb::Pool> testPool(new voltdb::Pool());
    voltdb::UndoQuantum* wantNoQuantum = NULL;
    voltdb::Topend* topless = NULL;
    boost::scoped_ptr<voltdb::ExecutorContext>
        executorContext(new voltdb::ExecutorContext(0,              // siteId
                                                    0,              // partitionId
                                                    wantNoQuantum,  // undoQuantum
                                                    topless,        // topend
                                                    testPool.get(), // tempStringPool
                                                    NULL,           // engine
                                                    "",             // hostname
                                                    0,              // hostId
                                                    NULL,           // drTupleStream
                                                    NULL,           // drReplicatedStream
                                                    0));            // drClusterId

    return TestSuite::globalInstance()->runAll();
}
