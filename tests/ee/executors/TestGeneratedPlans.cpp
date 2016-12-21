/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
/******************************************************************************************
 *
 * NOTA BENE: This file is automagically generated from the source class named
 *                org.voltdb.planner.EEPlanTestGenerator.
 *            Please do not edit it unless you abandon all hope of regenerating it.
 *
 ******************************************************************************************/
#include "harness.h"

#include "catalog/cluster.h"
#include "catalog/table.h"
#include "plannodes/abstractplannode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "test_utils/plan_testing_config.h"
#include "test_utils/LoadTableFrom.hpp"
#include "test_utils/plan_testing_baseclass.h"


namespace {
extern TestConfig allTests[];
};

class TestGeneratedPlans : public PlanTestingBaseClass<EngineTestTopend> {
public:
    /*
     * This constructor lets us set the global random seed for the
     * random number generator.  It would be better to have a seed
     * just for this test.  But that is not easily done.
     */
    TestGeneratedPlans(uint32_t randomSeed = (unsigned int)time(NULL)) {
        initialize(m_PartitionByExecutorDB, randomSeed);
    }

    ~TestGeneratedPlans() { }
protected:
    static DBConfig         m_PartitionByExecutorDB;
};

TEST_F(TestGeneratedPlans, test_order_by) {
    static int testIndex = 0;
    executeTest(allTests[testIndex]);
}
TEST_F(TestGeneratedPlans, test_join) {
    static int testIndex = 1;
    executeTest(allTests[testIndex]);
}


namespace {
const char *AAA_ColumnNames[] = {
    "A"
    "B",
    "C",
};
const char *BBB_ColumnNames[] = {
    "A"
    "B",
    "C",
};


const int NUM_TABLE_ROWS_AAA = 15;
const int NUM_TABLE_COLS_AAA = 3;
const int AAAData[NUM_TABLE_ROWS_AAA * NUM_TABLE_COLS_AAA] = {
      1, 10,101,
      1, 10,102,
      1, 20,201,
      1, 20,202,
      1, 30,301,
      2, 10,101,
      2, 10,102,
      2, 20,201,
      2, 20,202,
      2, 30,301,
      3, 10,101,
      3, 10,102,
      3, 20,201,
      3, 20,202,
      3, 30,301,
};

const int NUM_TABLE_ROWS_BBB = 15;
const int NUM_TABLE_COLS_BBB = 3;
const int BBBData[NUM_TABLE_ROWS_BBB * NUM_TABLE_COLS_BBB] = {
      1, 10,101,
      1, 10,102,
      1, 20,201,
      1, 20,202,
      1, 30,301,
      2, 10,101,
      2, 10,102,
      2, 20,201,
      2, 20,202,
      2, 30,301,
      3, 10,101,
      3, 10,102,
      3, 20,201,
      3, 20,202,
      3, 30,301,
};



const TableConfig AAAConfig = {
    "AAA",
    AAA_ColumnNames,
    NUM_TABLE_ROWS_AAA,
    NUM_TABLE_COLS_AAA,
    AAAData
};
const TableConfig BBBConfig = {
    "BBB",
    BBB_ColumnNames,
    NUM_TABLE_ROWS_BBB,
    NUM_TABLE_COLS_BBB,
    BBBData
};


const TableConfig *allTables[] = {
    &AAAConfig,
    &BBBConfig,

};

const int NUM_OUTPUT_ROWS_TEST_ORDER_BY = 15;
const int NUM_OUTPUT_COLS_TEST_ORDER_BY = 2;
const int outputTable_test_order_by[NUM_OUTPUT_ROWS_TEST_ORDER_BY * NUM_OUTPUT_COLS_TEST_ORDER_BY] = {
      1, 10,
      1, 10,
      1, 20,
      1, 20,
      1, 30,
      2, 10,
      2, 10,
      2, 20,
      2, 20,
      2, 30,
      3, 10,
      3, 10,
      3, 20,
      3, 20,
      3, 30,
};

const int NUM_OUTPUT_ROWS_TEST_JOIN = 45;
const int NUM_OUTPUT_COLS_TEST_JOIN = 3;
const int outputTable_test_join[NUM_OUTPUT_ROWS_TEST_JOIN * NUM_OUTPUT_COLS_TEST_JOIN] = {
      1, 10,101,
      1, 10,101,
      1, 10,101,
      1, 10,102,
      1, 10,102,
      1, 10,102,
      1, 20,201,
      1, 20,201,
      1, 20,201,
      1, 20,202,
      1, 20,202,
      1, 20,202,
      1, 30,301,
      1, 30,301,
      1, 30,301,
      2, 10,101,
      2, 10,101,
      2, 10,101,
      2, 10,102,
      2, 10,102,
      2, 10,102,
      2, 20,201,
      2, 20,201,
      2, 20,201,
      2, 20,202,
      2, 20,202,
      2, 20,202,
      2, 30,301,
      2, 30,301,
      2, 30,301,
      3, 10,101,
      3, 10,101,
      3, 10,101,
      3, 10,102,
      3, 10,102,
      3, 10,102,
      3, 20,201,
      3, 20,201,
      3, 20,201,
      3, 20,202,
      3, 20,202,
      3, 20,202,
      3, 30,301,
      3, 30,301,
      3, 30,301,
};



TestConfig allTests[2] = {
    {
        // SQL Statement
        "select A, B from AAA order by A, B;",
        // Plan String
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        4,\n"
        "        3,\n"
        "        2,\n"
        "        1\n"
        "    ],\n"
        "    \"PLAN_NODES\": [\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [2],\n"
        "            \"ID\": 1,\n"
        "            \"PLAN_NODE_TYPE\": \"SEND\"\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [3],\n"
        "            \"ID\": 2,\n"
        "            \"OUTPUT_SCHEMA\": [\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"A\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"B\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 1,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ],\n"
        "            \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [4],\n"
        "            \"ID\": 3,\n"
        "            \"PLAN_NODE_TYPE\": \"ORDERBY\",\n"
        "            \"SORT_COLUMNS\": [\n"
        "                {\n"
        "                    \"SORT_DIRECTION\": \"ASC\",\n"
        "                    \"SORT_EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"SORT_DIRECTION\": \"ASC\",\n"
        "                    \"SORT_EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 1,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ]\n"
        "        },\n"
        "        {\n"
        "            \"ID\": 4,\n"
        "            \"INLINE_NODES\": [{\n"
        "                \"ID\": 5,\n"
        "                \"OUTPUT_SCHEMA\": [\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"A\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 0,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"B\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 1,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    }\n"
        "                ],\n"
        "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"SEQSCAN\",\n"
        "            \"TARGET_TABLE_ALIAS\": \"AAA\",\n"
        "            \"TARGET_TABLE_NAME\": \"AAA\"\n"
        "        }\n"
        "    ]\n"
        "}",
        NUM_OUTPUT_ROWS_TEST_ORDER_BY,
        NUM_OUTPUT_COLS_TEST_ORDER_BY,
        outputTable_test_order_by
    },
    {
        // SQL Statement
        "select AAA.A, AAA.B, BBB.C from AAA join BBB on AAA.C = BBB.C order by AAA.A, AAA.B, AAA.C;",
        // Plan String
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        5,\n"
        "        7,\n"
        "        4,\n"
        "        3,\n"
        "        2,\n"
        "        1\n"
        "    ],\n"
        "    \"PLAN_NODES\": [\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [2],\n"
        "            \"ID\": 1,\n"
        "            \"PLAN_NODE_TYPE\": \"SEND\"\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [3],\n"
        "            \"ID\": 2,\n"
        "            \"OUTPUT_SCHEMA\": [\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"A\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"B\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 1,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"C\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 3,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ],\n"
        "            \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [4],\n"
        "            \"ID\": 3,\n"
        "            \"PLAN_NODE_TYPE\": \"ORDERBY\",\n"
        "            \"SORT_COLUMNS\": [\n"
        "                {\n"
        "                    \"SORT_DIRECTION\": \"ASC\",\n"
        "                    \"SORT_EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"SORT_DIRECTION\": \"ASC\",\n"
        "                    \"SORT_EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 1,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"SORT_DIRECTION\": \"ASC\",\n"
        "                    \"SORT_EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 2,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ]\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [\n"
        "                5,\n"
        "                7\n"
        "            ],\n"
        "            \"ID\": 4,\n"
        "            \"JOIN_PREDICATE\": {\n"
        "                \"LEFT\": {\n"
        "                    \"COLUMN_IDX\": 0,\n"
        "                    \"TABLE_IDX\": 1,\n"
        "                    \"TYPE\": 32,\n"
        "                    \"VALUE_TYPE\": 5\n"
        "                },\n"
        "                \"RIGHT\": {\n"
        "                    \"COLUMN_IDX\": 2,\n"
        "                    \"TYPE\": 32,\n"
        "                    \"VALUE_TYPE\": 5\n"
        "                },\n"
        "                \"TYPE\": 10,\n"
        "                \"VALUE_TYPE\": 23\n"
        "            },\n"
        "            \"JOIN_TYPE\": \"INNER\",\n"
        "            \"OUTPUT_SCHEMA\": [\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"A\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"B\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 1,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"C\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 2,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"C\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 0,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ],\n"
        "            \"PLAN_NODE_TYPE\": \"NESTLOOP\",\n"
        "            \"PRE_JOIN_PREDICATE\": null,\n"
        "            \"WHERE_PREDICATE\": null\n"
        "        },\n"
        "        {\n"
        "            \"ID\": 5,\n"
        "            \"INLINE_NODES\": [{\n"
        "                \"ID\": 6,\n"
        "                \"OUTPUT_SCHEMA\": [\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"A\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 0,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"B\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 1,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"C\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 2,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    }\n"
        "                ],\n"
        "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"SEQSCAN\",\n"
        "            \"TARGET_TABLE_ALIAS\": \"AAA\",\n"
        "            \"TARGET_TABLE_NAME\": \"AAA\"\n"
        "        },\n"
        "        {\n"
        "            \"ID\": 7,\n"
        "            \"INLINE_NODES\": [{\n"
        "                \"ID\": 8,\n"
        "                \"OUTPUT_SCHEMA\": [{\n"
        "                    \"COLUMN_NAME\": \"C\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 2,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }],\n"
        "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"SEQSCAN\",\n"
        "            \"TARGET_TABLE_ALIAS\": \"BBB\",\n"
        "            \"TARGET_TABLE_NAME\": \"BBB\"\n"
        "        }\n"
        "    ]\n"
        "}",
        NUM_OUTPUT_ROWS_TEST_JOIN,
        NUM_OUTPUT_COLS_TEST_JOIN,
        outputTable_test_join
    },
};

}

DBConfig TestGeneratedPlans::m_PartitionByExecutorDB =

{
    //
    // DDL.
    //
    "drop table AAA if exists;\n"
    "drop table BBB if exists;\n"
    "\n"
    "create table AAA (\n"
    "  A integer,\n"
    "  B integer,\n"
    "  C integer\n"
    " );\n"
    " \n"
    " create table BBB (\n"
    "  A integer,\n"
    "  B integer,\n"
    "  C integer\n"
    " );\n"
    " ",
    //
    // Catalog String
    //
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 0\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno 0\n"
    "set $PREV jsonapi false\n"
    "set $PREV networkpartition false\n"
    "set $PREV adminport 0\n"
    "set $PREV adminstartup false\n"
    "set $PREV heartbeatTimeout 0\n"
    "set $PREV useddlschema false\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled false\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 0\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drFlushInterval 0\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"eJy1TkEOgDAIu/saVljZrhr9/5MEs5ubN9NAAqUtNAcvF4gbC8GDFWIlAWEno1dv7K5urrpvnEuQWEk0JJUlBHWehBYlOT8WZ17SwwY4BoMloy8m9/07ePz7U/ANeEhGWQ==\"\n"
    "set $PREV isActiveActiveDRed false\n"
    "set $PREV securityprovider \"\"\n"
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
    "add /clusters#cluster/databases#database tables AAA\n"
    "set /clusters#cluster/databases#database/tables#AAA isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"AAA|iii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#AAA columns A\n"
    "set /clusters#cluster/databases#database/tables#AAA/columns#A index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"A\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#AAA columns B\n"
    "set /clusters#cluster/databases#database/tables#AAA/columns#B index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"B\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#AAA columns C\n"
    "set /clusters#cluster/databases#database/tables#AAA/columns#C index 2\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"C\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database tables BBB\n"
    "set /clusters#cluster/databases#database/tables#BBB isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"BBB|iii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#BBB columns A\n"
    "set /clusters#cluster/databases#database/tables#BBB/columns#A index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"A\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#BBB columns B\n"
    "set /clusters#cluster/databases#database/tables#BBB/columns#B index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"B\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#BBB columns C\n"
    "set /clusters#cluster/databases#database/tables#BBB/columns#C index 2\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"C\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database procedures testplanseegenerator\n"
    "set /clusters#cluster/databases#database/procedures#testplanseegenerator classname \"\"\n"
    "set $PREV readonly false\n"
    "set $PREV singlepartition false\n"
    "set $PREV everysite false\n"
    "set $PREV systemproc false\n"
    "set $PREV defaultproc false\n"
    "set $PREV hasjava false\n"
    "set $PREV hasseqscans false\n"
    "set $PREV language \"\"\n"
    "set $PREV partitiontable null\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV partitionparameter 0\n"
    "set $PREV allowedInShutdown false\n"
    "",
    2,
    allTables
};


int main() {
     return TestSuite::globalInstance()->runAll();
}
