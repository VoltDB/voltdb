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

#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
#include "executors/executorutil.h"
#include "executors/mergereceiveexecutor.h"
#include "expressions/tuplevalueexpression.h"
#include "storage/tablefactory.h"
#include "storage/temptable.h"

#include "boost/foreach.hpp"
#include "boost/scoped_ptr.hpp"
#include "boost/timer.hpp"

#include <vector>
#include <string>
#include <algorithm>

namespace voltdb {
static TupleSchema* createTupleScmema() {
    std::vector<ValueType> all_types;
    all_types.push_back(ValueType::tBIGINT);
    std::vector<bool> column_allow_null(1, true);
    std::vector<int32_t> all_inline_lengths;
    all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(ValueType::tBIGINT));
    return TupleSchema::createTupleSchemaForTest(all_types,
                                       all_inline_lengths,
                                       column_allow_null);
}

static TempTable* createTempTable() {

    const std::string tableName = "a_table";

    std::vector<ValueType> all_types;
    all_types.push_back(ValueType::tBIGINT);
    std::vector<bool> column_allow_null(1, true);
    std::vector<int32_t> all_inline_lengths;
    all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(ValueType::tBIGINT));
    TupleSchema*  schema = createTupleScmema();

    std::vector<std::string> names(1);
    return TableFactory::buildTempTable(tableName, schema, names, NULL);
}

class MergeReceiveExecutorTest : public Test
{
public:
    MergeReceiveExecutorTest()
        : m_tempDstTable(createTempTable())
        , m_key(0, 0)
    {
        m_keys.push_back(&m_key);
    }

    ~MergeReceiveExecutorTest()
    { }

    TempTable* getDstTempTable() {
        return m_tempDstTable.get();
    }

    std::vector<AbstractExpression*>& getSortKeys() {
        return m_keys;
    }

    void validateResults(AbstractExecutor::TupleComparer comp,
        std::vector<TableTuple>& srcTuples, int limit = -1, int offset = 0) {

        std::size_t size = (limit == -1) ? srcTuples.size() : limit;
        ASSERT_EQ(size, m_tempDstTable->activeTupleCount());

        // Sort the source
        std::sort(srcTuples.begin(), srcTuples.end(), comp);
        int i = 0;
        TableIterator iterator = m_tempDstTable->iterator();
        TableTuple tuple(m_tempDstTable->schema());
        while(iterator.next(tuple)) {
            ASSERT_TRUE(srcTuples[offset + i++].getNValue(0).op_equals(tuple.getNValue(0)).isTrue());
        }
        // Clean-up
        m_tempDstTable->deleteAllTuples();
    }

    char* addPartitionData(std::vector<int>& partitionValues,
        std::vector<TableTuple>& tuples,
        std::vector<int64_t>& partitionTupleCounts)
    {
        TableTuple tuple(m_tempDstTable->schema());
        size_t nValues = partitionValues.size();
        char* block = new char[nValues*tuple.tupleLength()];
        for (size_t i = 0; i < nValues; ++i) {
            NValue value = ValueFactory::getIntegerValue(partitionValues[i]);
            tuple.move(block+i*tuple.tupleLength());
            tuple.setNValue(0, value);
            tuples.push_back(tuple);
        }
        if (!partitionValues.empty()) {
            partitionTupleCounts.push_back(partitionValues.size());
        }
        return block;
    }

private:
    boost::scoped_ptr<TempTable> m_tempDstTable;
    TupleValueExpression m_key;
    std::vector<AbstractExpression*> m_keys;
};


TEST_F(MergeReceiveExecutorTest, emptyResultSetTest)
{
    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;
    std::vector<AbstractExpression*> keys;
    std::vector<SortDirectionType> dirs;
    AbstractExecutor::TupleComparer comp(keys, dirs);
    int limit = -1;
    int offset = 0;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);
    ASSERT_EQ(0, getDstTempTable()->activeTupleCount());
}

TEST_F(MergeReceiveExecutorTest, singlePartitionTest)
{
    std::vector<int> values;
    values.push_back(0);
    values.push_back(1);
    values.push_back(1);
    values.push_back(2);

    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;

    boost::scoped_array<char> cleaner(
        addPartitionData(values, tuples, partitionTupleCounts));

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);

    validateResults(comp, tuples);
}

TEST_F(MergeReceiveExecutorTest, singlePartitionLimitOffsetTest)
{
    std::vector<int> values;
    values.push_back(0);
    values.push_back(1);
    values.push_back(1);
    values.push_back(2);

    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;

    boost::scoped_array<char> cleaner(
        addPartitionData(values, tuples, partitionTupleCounts));

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = 2;
    int offset = 1;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);

    validateResults(comp, tuples, limit, offset);
}

TEST_F(MergeReceiveExecutorTest, singlePartitionBigOffsetTest)
{
    std::vector<int> values;
    values.push_back(0);
    values.push_back(1);
    values.push_back(1);
    values.push_back(2);

    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;

    boost::scoped_array<char> cleaner(
        addPartitionData(values, tuples, partitionTupleCounts));

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 10;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);
    ASSERT_EQ(0, getDstTempTable()->activeTupleCount());
}

TEST_F(MergeReceiveExecutorTest, twoNonOverlapPartitionsTest)
{
    std::vector<int> values1;
    values1.push_back(10);
    values1.push_back(11);
    values1.push_back(11);
    values1.push_back(12);

    std::vector<int> values2;
    values2.push_back(1);
    values2.push_back(1);
    values2.push_back(1);
    values2.push_back(2);

    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;

    boost::scoped_array<char> cleaner1(
        addPartitionData(values1, tuples, partitionTupleCounts));
    boost::scoped_array<char> cleaner2(
        addPartitionData(values2, tuples, partitionTupleCounts));

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);

    validateResults(comp, tuples);
}

TEST_F(MergeReceiveExecutorTest, multipleOverlapPartitionsTest)
{
    std::vector<int> values1;
    values1.push_back(10);
    values1.push_back(11);
    values1.push_back(11);
    values1.push_back(12);

    std::vector<int> values2;
    values2.push_back(1);
    values2.push_back(3);
    values2.push_back(4);
    values2.push_back(10);
    values2.push_back(11);
    values2.push_back(15);
    values2.push_back(20);
    values2.push_back(21);
    values2.push_back(25);

    std::vector<int> values3;
    values3.push_back(2);
    values3.push_back(4);
    values3.push_back(10);
    values3.push_back(12);
    values3.push_back(13);
    values3.push_back(15);

    std::vector<TableTuple> tuples;
    std::vector<int64_t> partitionTupleCounts;

    boost::scoped_array<char> cleaner1(
        addPartitionData(values1, tuples, partitionTupleCounts));
    boost::scoped_array<char> cleaner2(
        addPartitionData(values2, tuples, partitionTupleCounts));
    boost::scoped_array<char> cleaner3(
        addPartitionData(values3, tuples, partitionTupleCounts));

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    // Init the postfilter to evaluate LIMIT/OFFSET conditions
    CountingPostfilter postfilter(getDstTempTable(), NULL, limit, offset);
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               postfilter,
                               agg_exec,
                               getDstTempTable(),
                               pmp);
    validateResults(comp, tuples);
}

} // namespace voltdb

int main()
{
    return TestSuite::globalInstance()->runAll();
}
