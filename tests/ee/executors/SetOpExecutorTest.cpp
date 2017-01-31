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

#include "harness.h"

#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "executors/setoperator.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"

#include "boost/scoped_ptr.hpp"

#include <vector>

namespace voltdb {
static TupleSchema* createTupleScmema(int columnCnt) {
    std::vector<ValueType> all_types;
    std::vector<bool> column_allow_null(columnCnt, true);
    std::vector<int32_t> all_inline_lengths;

    for (int i = 0; i < columnCnt; ++i) {
        all_types.push_back(VALUE_TYPE_BIGINT);
        all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(VALUE_TYPE_BIGINT));
    }
    return TupleSchema::createTupleSchemaForTest(all_types,
                                       all_inline_lengths,
                                       column_allow_null);
}

static TempTable* createTempTable(int columnCnt) {

    const std::string tableName = "a_table";

    TupleSchema*  schema = createTupleScmema(columnCnt);

    std::vector<std::string> names(columnCnt);
    return TableFactory::buildTempTable(tableName, schema, names, NULL);
}

class SetOpExecutorTest : public Test
{
public:
    SetOpExecutorTest()
    { }

    ~SetOpExecutorTest()
    { }

    void addTuples(TempTable* table, int count, int value) {
        TableTuple out_tuple = table->tempTuple();
        for (int i = 0; i < count; ++i) {
            out_tuple.setNValue(0, ValueFactory::getBigIntValue(value));
            table->insertTempTuple(out_tuple);
        }
    }

    void verifyResults(std::map<std::pair<int, int>, int>& expected_results, TempTable* table) {
        ASSERT_EQ(2, table->columnCount());
        TableTuple tuple(table->schema());
        TableIterator iterator = table->iterator();
        while (iterator.next(tuple)) {
            int first = ValuePeeker::peekAsInteger(tuple.getNValue(0));
            int second = ValuePeeker::peekAsInteger(tuple.getNValue(1));
            std::pair<int, int> tuple_pair = std::make_pair(first, second);
            std::map<std::pair<int, int>, int>::iterator it = expected_results.find(tuple_pair);
            ASSERT_TRUE(it != expected_results.end());
            if (it->second == 1) {
                expected_results.erase(it);
            } else {
                --it->second;
            }
        }
        ASSERT_TRUE(expected_results.empty());
    }
};


TEST_F(SetOpExecutorTest, passThroughSetOpTest)
{
    // PassThroughSetOperator simply sends all tuples from all input tables to the output table
    // tagging each row with the table index
    int column_cnt = 1;
    boost::scoped_ptr<TempTable> output_table(createTempTable(column_cnt + 1));
    boost::scoped_ptr<TempTable> input_table1(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table2(createTempTable(column_cnt));
    std::vector<Table*> input_tables;
    input_tables.push_back(input_table1.get());
    input_tables.push_back(input_table2.get());

    int cnt1 = 2;
    int cnt2 = 1;
    int total_cnt = cnt1 + cnt2;
    int val1 = 99;
    int val2 = 100;
    addTuples(input_table1.get(), cnt1, val1);
    addTuples(input_table2.get(), cnt2, val2);

    PassThroughSetOperator passThroughSetOp(input_tables, output_table.get());
    SetOperator* setOp = &passThroughSetOp;
    bool result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(total_cnt, output_table->activeTupleCount());

    int idx = 0;
    TableTuple tuple(output_table->schema());
    int tuple_size = tuple.sizeInValues();
    TableIterator iterator = output_table->iterator();
    while (iterator.next(tuple)) {
        int child_idx = ValuePeeker::peekAsInteger(tuple.getNValue(tuple_size - 1));
        // Check the TAG column value
        // The first two tuples are from the first table (idx == 0) and the last one is from the second
        if (idx < 2) {
            ASSERT_EQ(0, child_idx);
        } else {
            ASSERT_EQ(1, child_idx);
        }
        ++idx;
    }
}

TEST_F(SetOpExecutorTest, unionSetOpTest)
{
    int column_cnt = 1;
    boost::scoped_ptr<TempTable> output_table(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table1(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table2(createTempTable(column_cnt));
    std::vector<Table*> input_tables;
    input_tables.push_back(input_table1.get());
    input_tables.push_back(input_table2.get());

    int cnt1 = 2;
    int cnt2 = 1;
    int val1 = 99;
    int val2 = 100;
    addTuples(input_table1.get(), cnt1, val1);
    addTuples(input_table2.get(), cnt2, val2);

    // table 1 - 99, 99
    // table 2 - 100
    // UNION - 99, 100
    UnionSetOperator unionSetOp(input_tables, output_table.get(), false);
    SetOperator* setOp = &unionSetOp;
    bool result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(2, output_table->activeTupleCount());

    // Clean-up
    output_table->deleteAllTempTuples();

    // table 1 - 99, 99
    // table 2 - 100
    // UNION ALL - 99, 99, 100
    UnionSetOperator unionAllSetOp(input_tables, output_table.get(), true);
    setOp = &unionAllSetOp;
    result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(3, output_table->activeTupleCount());
}

TEST_F(SetOpExecutorTest, intersectSetOpTest)
{
    int column_cnt = 1;
    boost::scoped_ptr<TempTable> output_table(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table1(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table2(createTempTable(column_cnt));
    std::vector<Table*> input_tables;
    input_tables.push_back(input_table1.get());
    input_tables.push_back(input_table2.get());

    int val1 = 99;
    int val2 = 100;
    int val3 = 101;
    int val4 = 102;
    int val5 = 103;
    addTuples(input_table1.get(), 2, val1);
    addTuples(input_table1.get(), 3, val2);
    addTuples(input_table1.get(), 1, val3);
    addTuples(input_table1.get(), 1, val4);

    addTuples(input_table2.get(), 2, val1);
    addTuples(input_table2.get(), 2, val2);
    addTuples(input_table2.get(), 3, val3);
    addTuples(input_table2.get(), 1, val5);

    // table 1 - 99 x 2, 100 x 3, 101 x 1, 102 x 1
    // table 2 - 99 x 2, 100 x 2, 101 x 3, 103 x 1
    // INTERSECT - 99, 100, 101
    ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker> intersectSetOp(input_tables, output_table.get(), false, false);
    SetOperator* setOp = &intersectSetOp;
    bool result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(3, output_table->activeTupleCount());

    // Clean-up
    output_table->deleteAllTempTuples();

    // table 1 - 99 x 2, 100 x 3, 101 x 1, 102 x 1
    // table 2 - 99 x 2, 100 x 2, 101 x 3, 103 x 1
    // INTERSECT ALL- 99 x 2, 100 x 2, 101 x 1
    ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker> intersectAllSetOp(input_tables, output_table.get(), true, false);
    setOp = &intersectAllSetOp;
    result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(5, output_table->activeTupleCount());
}

TEST_F(SetOpExecutorTest, exceptSetOpTest)
{
    int column_cnt = 1;
    boost::scoped_ptr<TempTable> output_table(createTempTable(column_cnt + 1));
    boost::scoped_ptr<TempTable> input_table1(createTempTable(column_cnt));
    boost::scoped_ptr<TempTable> input_table2(createTempTable(column_cnt));
    std::vector<Table*> input_tables;
    input_tables.push_back(input_table1.get());
    input_tables.push_back(input_table2.get());

    int val1 = 99;
    int val2 = 100;
    int val3 = 101;
    int val4 = 102;
    int val5 = 103;
    addTuples(input_table1.get(), 2, val1);
    addTuples(input_table1.get(), 4, val2);
    addTuples(input_table1.get(), 1, val3);
    addTuples(input_table1.get(), 3, val4);

    addTuples(input_table2.get(), 2, val1);
    addTuples(input_table2.get(), 2, val2);
    addTuples(input_table2.get(), 3, val3);
    addTuples(input_table2.get(), 2, val5);

    //
    // table 1 - 99 x 2, 100 x 4, 101 x 1, 102 x 3
    // table 2 - 99 x 2, 100 x 2, 101 x 3, 103 x 1
    // Output from EXCEPT ALL (table 1)- (100, 0) x 2, (102, 0) x 3
    // Output from table 2 - (101, 1) x 2, (103, 1) x 2
    ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>
        exceptAllSetOp(input_tables, output_table.get(), true, true, true);
    SetOperator* setOp = &exceptAllSetOp;
    bool result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(9, output_table->activeTupleCount());

    std::map<std::pair<int, int>, int> expected_all_results;
    expected_all_results.insert(std::make_pair(std::make_pair(100, 0), 2)); //(100, 0) x 2
    expected_all_results.insert(std::make_pair(std::make_pair(102, 0), 3)); //(102, 0) x 3
    expected_all_results.insert(std::make_pair(std::make_pair(101, 1), 2)); //(101, 1) x 2
    expected_all_results.insert(std::make_pair(std::make_pair(103, 1), 2)); //(103, 1) x 2
    verifyResults(expected_all_results, output_table.get());

    // Clean-up
    output_table->deleteAllTempTuples();

    // table 1 - 99 x 2, 100 x 4, 101 x 1, 102 x 3
    // table 2 - 99 x 2, 100 x 2, 101 x 3, 103 x 1
    // Output from EXCEPT (table 1)- (102, 0) x 1
    // Output from table 2 - (99, 1) x 1, (100, 1) x 1, (101, 1) x 1, (103, 1) x 1
    ExceptIntersectSetOperator<TableTupleHasher, TableTupleEqualityChecker>
        exceptSetOp(input_tables, output_table.get(), false, true, true);
    setOp = &exceptSetOp;
    result = setOp->processTuples();

    ASSERT_TRUE(result);
    ASSERT_EQ(5, output_table->activeTupleCount());

    std::map<std::pair<int, int>, int> expected_results;
    expected_results.insert(std::make_pair(std::make_pair(102, 0), 1)); //(102, 0) x 1
    expected_results.insert(std::make_pair(std::make_pair(99, 1), 1)); //(99, 1) x 1
    expected_results.insert(std::make_pair(std::make_pair(100, 1), 1)); //(101, 1) x 1
    expected_results.insert(std::make_pair(std::make_pair(101, 1), 1)); //(101, 1) x 1
    expected_results.insert(std::make_pair(std::make_pair(103, 1), 1)); //(103, 1) x 1
    verifyResults(expected_results, output_table.get());

}


} // namespace voltdb

int main()
{
    return TestSuite::globalInstance()->runAll();
}
