/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
#include "execution/VoltDBEngine.h"
#include "common/executorcontext.hpp"

using namespace voltdb;



class AutoEngine {
public:
    AutoEngine()
        : m_engine(new VoltDBEngine())
    {
        m_engine->initialize(1,     // clusterIndex
                             1,     // siteId
                             0,     // partitionId
                             0,     // hostId
                             "",    // hostname
                             0,     // drClusterId
                             1024,  // defaultDrBufferSize
                             voltdb::DEFAULT_TEMP_TABLE_MEMORY,
                             false, // don't create DR replicated stream
                             95);   // compaction threshold
        m_engine->setUndoToken(0);
    }

    const VoltDBEngine* operator->() const {
        return m_engine.get();
    }

    VoltDBEngine* operator->() {
        return m_engine.get();
    }

private:
    std::unique_ptr<VoltDBEngine> m_engine;
};

const std::string initialCatalogCmds =
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 1199145600\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno -1\n"
    "set $PREV jsonapi true\n"
    "set $PREV networkpartition false\n"
    "set $PREV heartbeatTimeout 90\n"
    "set $PREV useddlschema true\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled true\n"
    "set $PREV drRole \"master\"\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 5555\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drFlushInterval 1000\n"
    "set $PREV preferredSource 0\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"eJwDAAAAAAE=\"\n"
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
    "set $PREV querytimeout 10000\n"
    "add /clusters#cluster logconfig log\n"
    "set /clusters#cluster/logconfig#log enabled false\n"
    "set $PREV synchronous false\n"
    "set $PREV fsyncInterval 200\n"
    "set $PREV maxTxns 2147483647\n"
    "set $PREV logSize 1024\n";

const std::string updateCatalogCmds=
    "set /clusters#cluster/databases#database schema \"eJwlijEOwCAMA/e+JhhyaVYQ/f+TCOpi2Xemh3BaDFxW2RDr7zK9pIxkX0/godqb79qiXt+l7PM5MywPTw==\"\n"
    "add /clusters#cluster/databases#database tables T\n"
    "set /clusters#cluster/databases#database/tables#T isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"T|i\"\n"
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
    "set $PREV inbytes false\n";

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
    "         \"PLAN_NODE_TYPE\":\"TABLECOUNT\",\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            3\n"
    "         ],\n"
    "         \"OUTPUT_SCHEMA\":[  \n"
    "            {  \n"
    "               \"COLUMN_NAME\":\"C1\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":6,\n"
    "                  \"COLUMN_IDX\":0\n"
    "               }\n"
    "            }\n"
    "         ],\n"
    "         \"TARGET_TABLE_NAME\":\"DTBL\",\n"
    "         \"TARGET_TABLE_ALIAS\":\"DTBL\",\n"
    "         \"SUBQUERY_INDICATOR\":\"TRUE\"\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":3,\n"
    "         \"PLAN_NODE_TYPE\":\"NESTLOOP\",\n"
    "         \"CHILDREN_IDS\":[  \n"
    "            4,\n"
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
    "               \"COLUMN_NAME\":\"I\",\n"
    "               \"EXPRESSION\":{  \n"
    "                  \"TYPE\":32,\n"
    "                  \"VALUE_TYPE\":5,\n"
    "                  \"COLUMN_IDX\":1\n"
    "               }\n"
    "            }\n"
    "         ],\n"
    "         \"JOIN_TYPE\":\"INNER\",\n"
    "         \"PRE_JOIN_PREDICATE\":null,\n"
    "         \"JOIN_PREDICATE\":null,\n"
    "         \"WHERE_PREDICATE\":null\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":4,\n"
    "         \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "         \"INLINE_NODES\":[  \n"
    "            {  \n"
    "               \"ID\":5,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"I\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            }\n"
    "         ],\n"
    "         \"TARGET_TABLE_NAME\":\"T\",\n"
    "         \"TARGET_TABLE_ALIAS\":\"T1\"\n"
    "      },\n"
    "      {  \n"
    "         \"ID\":6,\n"
    "         \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "         \"INLINE_NODES\":[  \n"
    "            {  \n"
    "               \"ID\":7,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"OUTPUT_SCHEMA\":[  \n"
    "                  {  \n"
    "                     \"COLUMN_NAME\":\"I\",\n"
    "                     \"EXPRESSION\":{  \n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":0\n"
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
    "      4,\n"
    "      6,\n"
    "      3,\n"
    "      2,\n"
    "      1\n"
    "   ],\n"
    "   \"IS_LARGE_QUERY\":true\n"
    "}\n";


class ExecutorVectorTest : public Test {
public:
    ExecutorVectorTest()
        : Test()
        , m_engine()
    {
    }

protected:
    AutoEngine m_engine;
};

TEST_F(ExecutorVectorTest, Basic) {
    VoltDBEngine *engine = ExecutorContext::getEngine();

    ASSERT_NE(NULL, engine);

    bool rc = engine->loadCatalog(0, initialCatalogCmds);
    ASSERT_TRUE(rc);

    rc = engine->loadCatalog(1, updateCatalogCmds);
    ASSERT_TRUE(rc);

    auto ev = ExecutorVector::fromJsonPlan(engine, jsonPlan, 0);
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
