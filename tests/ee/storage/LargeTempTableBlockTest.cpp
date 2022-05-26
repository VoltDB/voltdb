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

#include <tuple>
#include <utility>

#include "boost/foreach.hpp"

#include "common/tabletuple.h"
#include "common/TupleSchema.h"

#include "storage/LargeTempTableBlock.h"

#include "harness.h"

#include "test_utils/ScopedTupleSchema.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/TupleComparingTest.hpp"
#include "test_utils/UniqueEngine.hpp"

using namespace voltdb;

class LargeTempTableBlockTest : public TupleComparingTest {
};

TEST_F(LargeTempTableBlockTest, iterator) {
    UniqueEngine engine = UniqueEngineBuilder().build();

    typedef std::tuple<int64_t, std::string, boost::optional<int32_t>> Tuple;
    ScopedTupleSchema schema{Tools::buildSchema<Tuple>()};

    LargeTempTableBlock block{LargeTempTableBlockId{0,0}, schema.get()};
    LargeTempTableBlock::iterator it = block.begin();
    LargeTempTableBlock::iterator itEnd = block.end();
    ASSERT_EQ(it, itEnd);

    // Insert some tuples into the block
    std::vector<Tuple> stdTuples{
        Tuple{0, "foo", boost::none},
        Tuple{1, "bar", 37},
        Tuple{2, "baz", 49},
        Tuple{3, "bugs", 96},
    };

    StandAloneTupleStorage tupleStorage{schema.get()};
    TableTuple tupleToInsert = tupleStorage.tuple();
    BOOST_FOREACH(auto stdTuple, stdTuples) {
        Tools::initTuple(&tupleToInsert, stdTuple);
        block.insertTuple(tupleToInsert);
    }

    // Use the iterator to access inserted tuples
    it = block.begin();
    itEnd = block.end();
    int i = 0;
    while (it != itEnd) {
        TableTuple tuple = it->toTableTuple(schema.get());
        ASSERT_TUPLES_EQ(stdTuples[i], tuple);
        ++it;
        ++i;
    }

    ASSERT_EQ(stdTuples.size(), i);

    // This also works with BOOST_FOREACH
    i = 0;
    BOOST_FOREACH(LargeTempTableBlock::Tuple& lttTuple, block) {
        char* storage = reinterpret_cast<char*>(&lttTuple);
        TableTuple tuple{storage, schema.get()};
        ASSERT_TUPLES_EQ(stdTuples[i], tuple);
        ++i;
    }

    // Test *it++, which the standard says should work.
    {
        it = block.begin();
        LargeTempTableBlock::Tuple& lttTuple = *it++;
        ASSERT_TUPLES_EQ(stdTuples[0], lttTuple.toTableTuple(schema.get()));
        ASSERT_TUPLES_EQ(stdTuples[1], it->toTableTuple(schema.get()));
    }

    // Decrement should also work.
    {
        // post-decrement
        LargeTempTableBlock::Tuple& lttTuple = *it--;
        ASSERT_TUPLES_EQ(stdTuples[1], lttTuple.toTableTuple(schema.get()));
        ASSERT_TUPLES_EQ(stdTuples[0], it->toTableTuple(schema.get()));

        ++it;
        // pre-decrement
        --it;
        ASSERT_TUPLES_EQ(stdTuples[0], it->toTableTuple(schema.get()));
    }

    // assign-add and assign-subtract
    it = block.begin();
    it += 3;
    ASSERT_TUPLES_EQ(stdTuples[3], it->toTableTuple(schema.get()));

    it -= 2;
    ASSERT_TUPLES_EQ(stdTuples[1], it->toTableTuple(schema.get()));

    // binary add and subtract
    it = block.begin();
    LargeTempTableBlock::iterator it2 = it + 3;
    ASSERT_TUPLES_EQ(stdTuples[3], it2->toTableTuple(schema.get()));
    ASSERT_TUPLES_EQ(stdTuples[0], it->toTableTuple(schema.get()));

    it = it2 - 2;
    ASSERT_TUPLES_EQ(stdTuples[1], it->toTableTuple(schema.get()));
    ASSERT_TUPLES_EQ(stdTuples[3], it2->toTableTuple(schema.get()));

    // constant LHS operand uses non-member function
    it2 = 1 + it;
    ASSERT_TUPLES_EQ(stdTuples[2], it2->toTableTuple(schema.get()));

    // iterator subtraction
    ASSERT_EQ(stdTuples.size(), block.end() - block.begin());

    // operator[]
    it = block.begin();
    ASSERT_TUPLES_EQ(stdTuples[0], it[0].toTableTuple(schema.get()));
    ASSERT_TUPLES_EQ(stdTuples[3], it[3].toTableTuple(schema.get()));

    // relational operators
    ASSERT_TRUE(block.end() > block.begin());
    ASSERT_TRUE(block.end() >= block.begin());
    ASSERT_TRUE(block.end() >= block.end());
    ASSERT_TRUE(block.begin() < block.end());
    ASSERT_TRUE(block.begin() <= block.end());
    ASSERT_TRUE(block.begin() <= block.begin());

    // const_iterator
    LargeTempTableBlock::const_iterator itc = block.cbegin();
    ASSERT_TUPLES_EQ(stdTuples[0], itc[0].toTableTuple(schema.get()));
    // This does not compile; can't convert const_iterator to iterator
    // LargeTempTableBlock::iterator nonConstIt = block.cbegin();
    //
    // This compiles, since you can convert an iterator to a const_iterator:
    itc = block.begin();

    const LargeTempTableBlock* constBlock = &block;
    auto anotherConstIt = constBlock->begin();
    // This is also a const iterator, so this does not compile
    // anotherConstIt->toTableTuple(schema.get()).setNValue(0, Tools::nvalueFromNative(int64_t(77)));
    ASSERT_TUPLES_EQ(stdTuples[0], anotherConstIt->toTableTuple(schema.get()));
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
