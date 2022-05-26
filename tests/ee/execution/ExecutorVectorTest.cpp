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

#include <string>

#include "harness.h"

#include "test_utils/LargeTempTableTopend.hpp"
#include "test_utils/Tools.hpp"
#include "test_utils/UniqueEngine.hpp"

#include "common/executorcontext.hpp"
#include "executors/abstractexecutor.h"
#include "storage/temptable.h"

using namespace voltdb;

/**
 * Catalog for a very simple database with just one table:
 *  create table t (i           integer not null,
 *                  inline_vc00 varchar(63 bytes),
 *                  val         varchar(500000));
 */
static const std::string catalogPayload =
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 1199145600\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno -1\n"
    "set $PREV jsonapi true\n"
    "set $PREV networkpartition false\n"
    "set $PREV heartbeatTimeout 90\n"
    "set $PREV useddlschema false\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled true\n"
    "set $PREV drRole \"master\"\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 5555\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drFlushInterval 1000\n"
    "set $PREV exportFlushInterval 4000\n"
    "set $PREV preferredSource 0\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"sQFUNjM3MjY1NjE3NDY1MjA3NDYxNjI2QwkMLDIwMjg2OTIwNjk2RQEgNDY3NjU3MjIwNkU2Rjc0AQgcNzU2QzZDMkMJJHQ2QzY5NkU2NTVGNzY2MzMwMzAyMDc2NjE3MjYzNjgBCCwyODM2MzMyMDYyNzkBUgw3MzI5AT4BJgg2QzIFCDIuAAA1AUYwMzAzMDMwMjkyOTNCCg==\"\n"
    "set $PREV isActiveActiveDRed false\n"
    "set $PREV securityprovider \"hash\"\n"
    "add /clusters#cluster/databases#database groups administrator\n"
    "set /clusters#cluster/databases#database/groups#administrator admin true\n"
    "set $PREV defaultproc true\n"
    "set $PREV defaultprocread true\n"
    "set $PREV sql true\n"
    "set $PREV sqlread true\n"
    "set $PREV allproc true\n"
    "add /clusters#cluster/databases#database groups user\n"
    "set /clusters#cluster/databases#database/groups#user admin false\n"
    "set $PREV defaultproc true\n"
    "set $PREV defaultprocread true\n"
    "set $PREV sql true\n"
    "set $PREV sqlread true\n"
    "set $PREV allproc true\n"
    "add /clusters#cluster/databases#database tables T\n"
    "set /clusters#cluster/databases#database/tables#T isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"T|ivv\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#T columns I\n"
    "set /clusters#cluster/databases#database/tables#T/columns#I index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"I\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#T columns INLINE_VC00\n"
    "set /clusters#cluster/databases#database/tables#T/columns#INLINE_VC00 index 1\n"
    "set $PREV type 9\n"
    "set $PREV size 63\n"
    "set $PREV nullable true\n"
    "set $PREV name \"INLINE_VC00\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes true\n"
    "add /clusters#cluster/databases#database/tables#T columns VAL\n"
    "set /clusters#cluster/databases#database/tables#T/columns#VAL index 2\n"
    "set $PREV type 9\n"
    "set $PREV size 500000\n"
    "set $PREV nullable true\n"
    "set $PREV name \"VAL\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes true\n"
    "add /clusters#cluster/databases#database snapshotSchedule default\n"
    "set /clusters#cluster/databases#database/snapshotSchedule#default enabled false\n"
    "set $PREV frequencyUnit \"h\"\n"
    "set $PREV frequencyValue 24\n"
    "set $PREV retain 2\n"
    "set $PREV prefix \"AUTOSNAP\"\n"
    "add /clusters#cluster deployment deployment\n"
    "set /clusters#cluster/deployment#deployment kfactor 0\n"
    "add /clusters#cluster/deployment#deployment systemsettings systemsettings\n"
    "set /clusters#cluster/deployment#deployment/systemsettings#systemsettings temptablemaxsize 100\n"
    "set $PREV snapshotpriority 6\n"
    "set $PREV elasticduration 50\n"
    "set $PREV elasticthroughput 2\n"
    "set $PREV querytimeout 300000\n"
    "add /clusters#cluster logconfig log\n"
    "set /clusters#cluster/logconfig#log enabled false\n"
    "set $PREV synchronous false\n"
    "set $PREV fsyncInterval 200\n"
    "set $PREV maxTxns 2147483647\n"
    "set $PREV logSize 1024";

// This is the "large" query produced by this invocation:
//     exec @AdHocLarge
//         select count(*), max(dtbl.theval) from (select *, t2.val as theval from t as t1, t  as t2) as dtbl
// (Note the IS_LARGE_QUERY field at the bottom.)
const std::string jsonPlan =
    "{  \n"
    "   \"PLAN_NODES\":[  \n"
    "      {  \n"
    "         \"ID\":1,\n"
    "         \"PLAN_NODE_TYPE\":\"SEND\",\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            2\n"
    "         ]\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":2,\n"
    "         \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "         \"INLINE_NODES\":[  \n"
    "            {  \n"
    "               \"ID\":3,\n"
    "               \"PLAN_NODE_TYPE\":\"AGGREGATE\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"C1\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":6,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  },\n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"C2\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":500000,\n"
    "                        \"COLUMN_IDX\":1\n"
    "                     }\n"
    "                  }\n"
    "               ],\n"
    "               \"AGGREGATE_COLUMNS\":[  \n"
    "                  {  \n"
    "                     \"AGGREGATE_TYPE\":\"AGGREGATE_COUNT_STAR\",\n"
    "                     \"AGGREGATE_DISTINCT\":0,\n"
    "                     \"AGGREGATE_OUTPUT_COLUMN\":0\n"
    "                  },\n"
    "                  {  \n"
    "                     \"AGGREGATE_TYPE\":\"AGGREGATE_MIN\",\n"
    "                     \"AGGREGATE_DISTINCT\":0,\n"
    "                     \"AGGREGATE_OUTPUT_COLUMN\":1,\n"
    "                     \"AGGREGATE_EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":500000,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            },\n"
    "            {  \n"
    "               \"ID\":4,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"THEVAL\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":500000,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":6\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            }\n"
    "         ],\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            5\n"
    "         ],\n"
    "         \"OUTPUT_SCHEMA\":[  \n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"C1\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":6,\n"
    "                  \"COLUMN_IDX\":0\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"C2\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"COLUMN_IDX\":1\n"
    "               }\n"
    "            }\n"
    "         ],\n"
    "         \"TARGET_TABLE_NAME\":\"DTBL\",\n"
    "         \"TARGET_TABLE_ALIAS\":\"DTBL\",\n"
    "         \"SUBQUERY_INDICATOR\":\"TRUE\"\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":5,\n"
    "         \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            6\n"
    "         ],\n"
    "         \"OUTPUT_SCHEMA\":[  \n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"I\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":5,\n"
    "                  \"COLUMN_IDX\":0\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":63,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":1\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"VAL\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":2\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"I\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":5,\n"
    "                  \"COLUMN_IDX\":3\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":63,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":4\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"VAL\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":5\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"THEVAL\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":5\n"
    "               }\n"
    "            }\n"
    "         ]\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":6,\n"
    "         \"PLAN_NODE_TYPE\":\"NESTLOOP\",\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            7,\n"
    "            9\n"
    "         ],\n"
    "         \"OUTPUT_SCHEMA\":[  \n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"I\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":5,\n"
    "                  \"COLUMN_IDX\":0\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":63,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":1\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"VAL\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":2\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"I\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":5,\n"
    "                  \"COLUMN_IDX\":3\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":63,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":4\n"
    "               }\n"
    "            },\n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"VAL\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":9,\n"
    "                  \"VALUE_SIZE\":500000,\n"
    "                  \"IN_BYTES\":true,\n"
    "                  \"COLUMN_IDX\":5\n"
    "               }\n"
    "            }\n"
    "         ],\n"
    "         \"JOIN_TYPE\":\"INNER\",\n"
    "         \"PRE_JOIN_PREDICATE\":null,\n"
    "         \"JOIN_PREDICATE\":null,\n"
    "         \"WHERE_PREDICATE\":null\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":7,\n"
    "         \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "         \"INLINE_NODES\":[  \n"
    "            {  \n"
    "               \"ID\":8,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"I\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  },\n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":63,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":1\n"
    "                     }\n"
    "                  },\n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"VAL\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":500000,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":2\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            }\n"
    "         ],\n"
    "         \"TARGET_TABLE_NAME\":\"T\",\n"
    "         \"TARGET_TABLE_ALIAS\":\"T1\"\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":9,\n"
    "         \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "         \"INLINE_NODES\":[  \n"
    "            {  \n"
    "               \"ID\":10,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"I\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  },\n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"INLINE_VC00\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":63,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":1\n"
    "                     }\n"
    "                  },\n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"VAL\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":500000,\n"
    "                        \"IN_BYTES\":true,\n"
    "                        \"COLUMN_IDX\":2\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            }\n"
    "         ],\n"
    "         \"TARGET_TABLE_NAME\":\"T\",\n"
    "         \"TARGET_TABLE_ALIAS\":\"T2\"\n"
    "      }\n"
    "   ],\n"
    "   \"EXECUTE_LIST\":[  \n"
    "      7,\n"
    "      9,\n"
    "      6,\n"
    "      5,\n"
    "      2,\n"
    "      1\n"
    "   ],\n"
    "   \"IS_LARGE_QUERY\":true\n"
    "}\n";


class ExecutorVectorTest : public Test {
public:
    ~ExecutorVectorTest() {
        voltdb::globalDestroyOncePerProcess();
    }
};

TEST_F(ExecutorVectorTest, Large) {
    std::unique_ptr<Topend> topend{new LargeTempTableTopend()};

    // Define an LTT block cache that can hold only three blocks:
    int64_t tempTableMemoryLimitInBytes = 24 * 1024 * 1024;
    UniqueEngine engine = UniqueEngineBuilder()
        .setTopend(std::move(topend))
        .setTempTableMemoryLimit(tempTableMemoryLimitInBytes)
        .build();

    bool rc = engine->loadCatalog(0, catalogPayload);
    ASSERT_TRUE(rc);

    auto ev = ExecutorVector::fromJsonPlan(engine.get(), jsonPlan, 0);
    BOOST_FOREACH(auto executor, ev->getExecutorList()) {
        auto nodeType = executor->getPlanNode()->getPlanNodeType();
        if (nodeType == PlanNodeType::Send) {
            // send nodes do not have output temp tables
            ASSERT_EQ(NULL, executor->getTempOutputTable());
        }
        else {
            // Verify that the output temp table for each node
            // is a large temp table
            auto table = executor->getTempOutputTable();
            ASSERT_NE(NULL, table);
            ASSERT_EQ("LargeTempTable", table->tableType());
        }
    }

    // Make sure we can execute without crashing
    // (answer is verified in RegressionSuite JUnit test)
    UniqueTempTableResult tbl = engine->executePlanFragment(ev.get(), NULL);
    // Send node at top of plan produces no result table.
    ASSERT_EQ(NULL, tbl.get());

    // Now execute the fragment with some data in there.
    Table* persTbl = engine->getTableByName("T");
    const TupleSchema* schema = persTbl->schema();
    StandAloneTupleStorage tupleWrapper(schema);
    TableTuple tuple = tupleWrapper.tuple();

    SynchronizedThreadLock::debugSimulateSingleThreadMode(true);
    SynchronizedThreadLock::assumeMpMemoryContext();
    for (int i = 0; i < 750; ++i) {
        std::ostringstream ossShort, ossLong;
        ossShort << "short " << i;
        ossLong << "long " << i;

        Tools::setTupleValues(&tuple, i, ossShort.str(), ossLong.str());
        persTbl->insertTuple(tuple);
    }
    SynchronizedThreadLock::assumeLowestSiteContext();
    SynchronizedThreadLock::debugSimulateSingleThreadMode(false);

    tbl = engine->executePlanFragment(ev.get(), NULL);
    // Again send node has no output table.
    ASSERT_EQ(NULL, tbl.get());

    LargeTempTableBlockCache& lttBlockCache = ExecutorContext::getExecutorContext()->lttBlockCache();

    ASSERT_EQ(0, lttBlockCache.allocatedMemory());
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
