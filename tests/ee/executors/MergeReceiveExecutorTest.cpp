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

#include "harness.h"

#include "common/TupleSchema.h"
#include "common/NValue.hpp"
#include "common/ValueFactory.hpp"
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
static const CatalogId DATABASE_ID = 100;


static TupleSchema* createTupleScmema() {
    std::vector<ValueType> all_types;
    all_types.push_back(VALUE_TYPE_BIGINT);
    std::vector<bool> column_allow_null(1, true);
    std::vector<int32_t> all_inline_lengths;
    all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(VALUE_TYPE_BIGINT));
    return TupleSchema::createTupleSchemaForTest(all_types,
                                       all_inline_lengths,
                                       column_allow_null);
}

static TempTable* createTempTable() {

    const std::string tableName = "a_table";

    std::vector<ValueType> all_types;
    all_types.push_back(VALUE_TYPE_BIGINT);
    std::vector<bool> column_allow_null(1, true);
    std::vector<int32_t> all_inline_lengths;
    all_inline_lengths.push_back(NValue::
                                 getTupleStorageSize(VALUE_TYPE_BIGINT));
    TupleSchema*  schema = createTupleScmema();

    std::vector<std::string> names(1);
    return voltdb::TableFactory::getTempTable(DATABASE_ID,
                                                 tableName,
                                                 schema,
                                                 names,
                                                 NULL);
}

class MergeReceiveExecutorTest : public Test
{
public:
    MergeReceiveExecutorTest() :
        Test(), m_tempDstTable() ,m_keys()
    {
        m_tempDstTable.reset(createTempTable());

        AbstractExpression* key = new TupleValueExpression(0, 0);
        m_keys.push_back(key);
    }

    ~MergeReceiveExecutorTest()
    {
        BOOST_FOREACH(AbstractExpression* key, m_keys) {
            delete key;
        }
    }

    voltdb::TempTable* getDstTempTable() {
        return m_tempDstTable.get();
    }

    std::vector<AbstractExpression*>& getSortKeys() {
        return m_keys;
    }

    void validateResults(AbstractExecutor::TupleComparer comp,
        std::vector<TableTuple>& srcTuples, int limit = -1, int offset = 0) {

        int size = (limit == -1) ? srcTuples.size() : limit;
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
        m_tempDstTable->deleteAllTuplesNonVirtual(true);
    }

    void addPartitionData(std::vector<int>& partitionValues,
        std::vector<TableTuple>& tuples,
        std::vector<int64_t>& partitionTupleCounts) {

        for (size_t i = 0; i < partitionValues.size(); ++i) {
            NValue value = ValueFactory::getIntegerValue(partitionValues[i]);
            TableTuple tuple(m_tempDstTable->schema());
            tuple.move(new char[tuple.tupleLength()]);
            tuple.setNValue(0, value);
            tuples.push_back(tuple);
        }
        if (!partitionValues.empty()) {
            partitionTupleCounts.push_back(partitionValues.size());
        }
    }

private:
    boost::scoped_ptr<voltdb::TempTable> m_tempDstTable;
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
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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

    addPartitionData(values, tuples, partitionTupleCounts);

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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

    addPartitionData(values, tuples, partitionTupleCounts);

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = 2;
    int offset = 1;
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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

    addPartitionData(values, tuples, partitionTupleCounts);

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 10;
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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

    addPartitionData(values1, tuples, partitionTupleCounts);
    addPartitionData(values2, tuples, partitionTupleCounts);

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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

    addPartitionData(values1, tuples, partitionTupleCounts);
    addPartitionData(values2, tuples, partitionTupleCounts);
    addPartitionData(values3, tuples, partitionTupleCounts);

    std::vector<SortDirectionType> dirs(1, SORT_DIRECTION_TYPE_ASC);
    AbstractExecutor::TupleComparer comp(getSortKeys(), dirs);
    int limit = -1;
    int offset = 0;
    AggregateExecutorBase* agg_exec = NULL;
    ProgressMonitorProxy* pmp = NULL;
    MergeReceiveExecutor::merge_sort(tuples,
                               partitionTupleCounts,
                               comp,
                               limit,
                               offset,
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
