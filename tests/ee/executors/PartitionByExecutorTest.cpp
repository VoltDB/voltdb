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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "harness.h"

#include "catalog/cluster.h"
#include "catalog/table.h"
#include "plannodes/abstractplannode.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "test_utils/plan_testing_baseclass.h"
#include "test_utils/LoadTableFrom.hpp"


namespace {
const char *plan_strings[] = {
    //  Plan for this query:
    //      select ID, A, B, RANK() OVER ( PARTITION BY A ORDER BY B ) from AAA;
    "{\n"
    "    \"EXECUTE_LIST\": [\n"
    "        1,\n"
    "        3,\n"
    "        2,\n"
    "        4,\n"
    "        5\n"
    "    ],\n"
    "    \"PLAN_NODES\": [\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [4],\n"
    "            \"ID\": 5,\n"
    "            \"PLAN_NODE_TYPE\": \"SEND\"\n"
    "        },\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [2],\n"
    "            \"ID\": 4,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"ID\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 1,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 3,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"B\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 5,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"C4\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
    "        },\n"
    "        {\n"
    "            \"AGGREGATE_COLUMNS\": [{\n"
    "                \"AGGREGATE_DISTINCT\": 0,\n"
    "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
    "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_RANK\"\n"
    "            }],\n"
    "            \"CHILDREN_IDS\": [3],\n"
    "            \"GROUPBY_EXPRESSIONS\": [{\n"
    "                \"COLUMN_IDX\": 1,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }],\n"
    "            \"ID\": 2,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"C4\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"ID\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"ID\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 1,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 2,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 3,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"B\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 4,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"PARTITIONBY\",\n"
    "            \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 3,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }}]\n"
    "        },\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [1],\n"
    "            \"ID\": 3,\n"
    "            \"PLAN_NODE_TYPE\": \"ORDERBY\",\n"
    "            \"SORT_COLUMNS\": [\n"
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
    "                        \"COLUMN_IDX\": 3,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                }\n"
    "            ]\n"
    "        },\n"
    "        {\n"
    "            \"ID\": 1,\n"
    "            \"INLINE_NODES\": [{\n"
    "                \"ID\": 6,\n"
    "                \"OUTPUT_SCHEMA\": [\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"ID\",\n"
    "                        \"EXPRESSION\": {\n"
    "                            \"COLUMN_IDX\": 0,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_TYPE\": 5\n"
    "                        }\n"
    "                    },\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"ID\",\n"
    "                        \"EXPRESSION\": {\n"
    "                            \"COLUMN_IDX\": 0,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_TYPE\": 5\n"
    "                        }\n"
    "                    },\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"A\",\n"
    "                        \"EXPRESSION\": {\n"
    "                            \"COLUMN_IDX\": 1,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_TYPE\": 5\n"
    "                        }\n"
    "                    },\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"A\",\n"
    "                        \"EXPRESSION\": {\n"
    "                            \"COLUMN_IDX\": 1,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_TYPE\": 5\n"
    "                        }\n"
    "                    },\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"B\",\n"
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
    "        }\n"
    "    ]\n"
    "}\n",
    "{\n"
    "    \"EXECUTE_LIST\": [\n"
    "        1,\n"
    "        3,\n"
    "        2,\n"
    "        4,\n"
    "        5,\n"
    "        6\n"
    "    ],\n"
    "    \"PLAN_NODES\": [\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [5],\n"
    "            \"ID\": 6,\n"
    "            \"PLAN_NODE_TYPE\": \"SEND\"\n"
    "        },\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [4],\n"
    "            \"ID\": 5,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 1,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"B\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 2,\n"
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
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"R\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
    "        },\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [2],\n"
    "            \"ID\": 4,\n"
    "            \"PLAN_NODE_TYPE\": \"ORDERBY\",\n"
    "            \"SORT_COLUMNS\": [\n"
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
    "                },\n"
    "                {\n"
    "                    \"SORT_DIRECTION\": \"ASC\",\n"
    "                    \"SORT_EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 3,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"SORT_DIRECTION\": \"ASC\",\n"
    "                    \"SORT_EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                }\n"
    "            ]\n"
    "        },\n"
    "        {\n"
    "            \"AGGREGATE_COLUMNS\": [{\n"
    "                \"AGGREGATE_DISTINCT\": 0,\n"
    "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
    "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_RANK\"\n"
    "            }],\n"
    "            \"CHILDREN_IDS\": [3],\n"
    "            \"GROUPBY_EXPRESSIONS\": [{\n"
    "                \"COLUMN_IDX\": 0,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }],\n"
    "            \"ID\": 2,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"R\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 6\n"
    "                    }\n"
    "                },\n"
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
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"PARTITIONBY\",\n"
    "            \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
    "                \"COLUMN_IDX\": 1,\n"
    "                \"TYPE\": 32,\n"
    "                \"VALUE_TYPE\": 5\n"
    "            }}]\n"
    "        },\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [1],\n"
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
    "            \"ID\": 1,\n"
    "            \"INLINE_NODES\": [{\n"
    "                \"ID\": 7,\n"
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
    "        }\n"
    "    ]\n"
    "}\n",
    (const char *)0
};

/**
 * The catalog string below reflects this DDL.
 *
 * CREATE TABLE AAA (
 *    A  INTEGER,
 *    B  INTEGER,
 *    C  INTEGER
 * );
 */
const char *catalog_string =
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 1199145600\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno 0\n"
    "set $PREV jsonapi false\n"
    "set $PREV networkpartition false\n"
    "set $PREV voltRoot \"\"\n"
    "set $PREV exportOverflow \"\"\n"
    "set $PREV drOverflow \"\"\n"
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
    "set /clusters#cluster/databases#database schema \"eJx9ikEKgEAMA+++pptO0L26+P8v2YIHQZCQkDAhLcwwWFE5EKt7tZJCR/lRbyZXv9mxpfWi+qX5oaGZ53YD0G8aZw==\"\n"
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
    "set $PREV matview null\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#AAA columns B\n"
    "set /clusters#cluster/databases#database/tables#AAA/columns#B index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"B\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV matview null\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#AAA columns C\n"
    "set /clusters#cluster/databases#database/tables#AAA/columns#C index 2\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"C\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV matview null\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV inbytes false\n";
}

class PartitionByExecutorTest : public PlanTestingBaseClass<EngineTestTopend> {
public:
    /*
     * This constructor lets us set the global random seed for the
     * random number generator.  It would be better to have a seed
     * just for this test.  But that is not easily done.
     */
    PartitionByExecutorTest(uint32_t random_seed = (unsigned int)time(NULL))
        : m_AAA(NULL),
          m_AAA_id(-1) {
        initialize(catalog_string, random_seed);
    }

    void initialize(const char *catalog_string,
                    uint32_t    random_seed = (uint32_t)time(NULL)) {
        PlanTestingBaseClass<EngineTestTopend>::initialize(catalog_string, random_seed);
        //
        // Get the tables we need.
        //
        const int NUM_ROWS_AAA   = 15;
        const int NUM_COLS_AAA   = 3;

        int32_t input_AAA[NUM_ROWS_AAA][NUM_COLS_AAA] = {
            {  1,  10,  101},
            {  1,  10,  102},
            {  1,  20,  201},
            {  1,  20,  202},
            {  1,  30,  301},
            {  2,  10,  101},
            {  2,  10,  102},
            {  2,  20,  201},
            {  2,  20,  202},
            {  2,  30,  301},
            {  3,  10,  101},
            {  3,  10,  102},
            {  3,  20,  201},
            {  3,  20,  202},
            {  3,  30,  301}
        };
        initializeTableOfInt("AAA", &m_AAA, &m_AAA_id, NUM_ROWS_AAA, NUM_COLS_AAA, (int32_t *)input_AAA);
    }

    ~PartitionByExecutorTest() { }
protected:
    voltdb::PersistentTable *m_AAA;
    int                      m_AAA_id;
};

TEST_F(PartitionByExecutorTest, testPartitionBy) {
    const int NUM_ROWS = 15;
    const int NUM_COLS =  4;

    int32_t output[NUM_ROWS][NUM_COLS] = {
            {  1,  10,  101,   1},
            {  1,  10,  102,   1},
            {  1,  20,  201,   3},
            {  1,  20,  202,   3},
            {  1,  30,  301,   5},
            {  2,  10,  101,   1},
            {  2,  10,  102,   1},
            {  2,  20,  201,   3},
            {  2,  20,  202,   3},
            {  2,  30,  301,   5},
            {  3,  10,  101,   1},
            {  3,  10,  102,   1},
            {  3,  20,  201,   3},
            {  3,  20,  202,   3},
            {  3,  30,  301,   5}
    };
    executeFragment(100, plan_strings[0]);
    validateResult((int32_t *)output, NUM_ROWS, NUM_COLS);
}

TEST_F(PartitionByExecutorTest, testOrderedPartitionBy) {
    const int NUM_ROWS = 15;
    const int NUM_COLS =  4;

    int32_t output[NUM_ROWS][NUM_COLS] = {
            {  1,  10,  101,   1},
            {  1,  10,  102,   1},
            {  1,  20,  201,   3},
            {  1,  20,  202,   3},
            {  1,  30,  301,   5},
            {  2,  10,  101,   1},
            {  2,  10,  102,   1},
            {  2,  20,  201,   3},
            {  2,  20,  202,   3},
            {  2,  30,  301,   5},
            {  3,  10,  101,   1},
            {  3,  10,  102,   1},
            {  3,  20,  201,   3},
            {  3,  20,  202,   3},
            {  3,  30,  301,   5}
    };
    executeFragment(100, plan_strings[1]);
    validateResult((int32_t *)output, NUM_ROWS, NUM_COLS);
}

int main() {
     return TestSuite::globalInstance()->runAll();
}
