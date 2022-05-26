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
#include <tuple>
#include <vector>

#include <boost/foreach.hpp>
#include <boost/optional.hpp>

#include "harness.h"

#include "test_utils/Tools.hpp"
#include "test_utils/TupleComparingTest.hpp"
#include "test_utils/UniqueEngine.hpp"

#include "common/tabletuple.h"
#include "execution/ExecutorVector.h"
#include "executors/abstractexecutor.h"
#include "plannodes/abstractjoinnode.h"
#include "plannodes/commontablenode.h"
#include "plannodes/orderbynode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/seqscannode.h"
#include "storage/AbstractTempTable.hpp"
#include "storage/table.h"
#include "storage/tableiterator.h"

using namespace voltdb;

class CommonTableExpressionTest : public TupleComparingTest {
};

// Catalog for the following DDL:
//
// CREATE TABLE EMPLOYEES (
//     LAST_NAME VARCHAR(20) NOT NULL,
//     EMP_ID INTEGER NOT NULL,
//     MANAGER_ID INTEGER
// );
// PARTITION TABLE EMPLOYEES ON LAST_NAME;

const std::string catalogPayload =
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
    "set /clusters#cluster/databases#database schema \"qgRUNDM1MjQ1NDE1NDQ1MjA1NDQxNDI0QwEMWDQ1NEQ1MDRDNEY1OTQ1NDU1MzIwMjgyARIwMTUzNTQ1RjRFNDE0RAEsJDU2NDE1MjQzNDgBCDwyODMyMzAyOTIwNEU0RjU0AQgkNTU0QzRDMkMyMAlYEDVGNDk0ARoIOTRFAXwUNDc0NTUyASpKMgAIRDQxBWwFJF46ABAyOTNCCmrPAAA0AWEQNDk1NjQBcABGEYcENTAF/QA4/t0A/t0Adt0AUkkBCEM0NQXOIVWKRwEZ6kKvAQgxMzAJAlK1ARQwMjkzQgo=\"\n"
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
    "add /clusters#cluster/databases#database tables EMPLOYEES\n"
    "set /clusters#cluster/databases#database/tables#EMPLOYEES isreplicated false\n"
    "set $PREV partitioncolumn /clusters#cluster/databases#database/tables#EMPLOYEES/columns#LAST_NAME\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"EMPLOYEES|vii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#EMPLOYEES columns EMP_ID\n"
    "set /clusters#cluster/databases#database/tables#EMPLOYEES/columns#EMP_ID index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"EMP_ID\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#EMPLOYEES columns LAST_NAME\n"
    "set /clusters#cluster/databases#database/tables#EMPLOYEES/columns#LAST_NAME index 0\n"
    "set $PREV type 9\n"
    "set $PREV size 20\n"
    "set $PREV nullable false\n"
    "set $PREV name \"LAST_NAME\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#EMPLOYEES columns MANAGER_ID\n"
    "set /clusters#cluster/databases#database/tables#EMPLOYEES/columns#MANAGER_ID index 2\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"MANAGER_ID\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
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

// This JSON is hopefully similar to what the planner will produce for
// the following SQL:
//
// WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS (
//     SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME
//       FROM EMPLOYEES
//       WHERE MANAGER_ID IS NULL
//     UNION ALL
//     SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || ‘/’ || E.LAST_NAME
//       FROM EMPLOYEES E JOIN EMP_PATH EP ON E.MANAGER_ID = EP.EMP_ID
// )
// SELECT * FROM EMP_PATH;
const std::string jsonPlan =
    "{\n"
    "   \"PLAN_NODES_LISTS\":[\n"
    // The main query
    "      {\n"
    "         \"STATEMENT_ID\":0,\n"
    "         \"PLAN_NODES\":[\n"
    "            {\n"
    "               \"ID\":2,\n"
    "               \"PLAN_NODE_TYPE\":\"ORDERBY\",\n"
    "               \"CHILDREN_IDS\":[\n"
    "                  3\n"
    "               ],\n"
    "               \"SORT_COLUMNS\":[\n"
    "                  {\n"
    "                     \"SORT_EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":3\n"
    "                     },\n"
    "                     \"SORT_DIRECTION\":\"ASC\"\n"
    "                  },\n"
    "                  {\n"
    "                     \"SORT_EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":2048,\n"
    "                        \"COLUMN_IDX\":4\n"
    "                     },\n"
    "                     \"SORT_DIRECTION\":\"ASC\"\n"
    "                  }\n"
    "               ]\n"
    "            },\n"
    "            {\n"
    "               \"ID\":3,\n"
    "               \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "               \"INLINE_NODES\":[\n"
    "                  {\n"
    "                     \"ID\":13,\n"
    "                     \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "                     \"OUTPUT_SCHEMA\":[\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":0\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":1\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"MANAGER_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":2\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LEVEL\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":6,\n"
    "                              \"COLUMN_IDX\":3\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"PATH\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":4\n"
    "                           }\n"
    "                        }\n"
    "                     ]\n"
    "                  }\n"
    "               ],\n"
    "               \"TARGET_TABLE_NAME\":\"EMP_PATH\",\n"
    "               \"TARGET_TABLE_ALIAS\":\"EMP_PATH\",\n"
    "               \"IS_CTE_SCAN\":true,\n"
    "               \"CTE_STMT_ID\":1\n"
    "            }\n"
    "         ]\n"
    "      },\n"
    // The base query
    "      {\n"
    "         \"STATEMENT_ID\":1,\n"
    "         \"PLAN_NODES\":[\n"
    "            {\n"
    "               \"ID\":4,\n"
    "               \"PLAN_NODE_TYPE\":\"COMMONTABLE\",\n"
    "               \"CHILDREN_IDS\":[\n"
    "                  5\n"
    "               ],\n"
    "               \"COMMON_TABLE_NAME\":\"EMP_PATH\",\n"
    "               \"RECURSIVE_STATEMENT_ID\":2\n"
    "            },\n"
    "            {\n"
    "               \"ID\":5,\n"
    "               \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "               \"INLINE_NODES\":[\n"
    "                  {\n"
    "                     \"ID\":6,\n"
    "                     \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "                     \"OUTPUT_SCHEMA\":[\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":0\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":1\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"MANAGER_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":2\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"C4\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"ISNULL\": false,\n"
    "                              \"VALUE\": \"1\",\n"
    "                              \"TYPE\": 30,\n"
    "                              \"VALUE_TYPE\": 6\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":1048576,\n"
    "                              \"COLUMN_IDX\":0\n"
    "                           }\n"
    "                        }\n"
    "                     ]\n"
    "                  }\n"
    "               ],\n"
    "               \"PREDICATE\":{\n"
    "                  \"TYPE\":9,\n"
    "                  \"VALUE_TYPE\":23,\n"
    "                  \"LEFT\":{\n"
    "                     \"TYPE\":32,\n"
    "                     \"VALUE_TYPE\":5,\n"
    "                     \"COLUMN_IDX\":2\n"
    "                  }\n"
    "               },\n"
    "               \"TARGET_TABLE_NAME\":\"EMPLOYEES\",\n"
    "               \"TARGET_TABLE_ALIAS\":\"EMPLOYEES\"\n"
    "            }\n"
    "         ]\n"
    "      },\n"
    // The recursive query
    "      {\n"
    "         \"STATEMENT_ID\":2,\n"
    "         \"PLAN_NODES\":[\n"
    "            {\n"
    "               \"ID\":7,\n"
    "               \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "               \"CHILDREN_IDS\":[\n"
    "                  8\n"
    "               ],\n"
    "               \"OUTPUT_SCHEMA\":[\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":2048,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":1\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"MANAGER_ID\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":2\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"C4\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":1,\n"
    "                        \"VALUE_TYPE\":6,\n"
    "                        \"LEFT\":{\n"
    "                           \"TYPE\":32,\n"
    "                           \"VALUE_TYPE\":5,\n"
    "                           \"COLUMN_IDX\":4\n"
    "                        },\n"
    "                        \"RIGHT\":{\n"
    "                           \"ISNULL\": false,\n"
    "                           \"VALUE\": \"1\",\n"
    "                           \"TYPE\": 30,\n"
    "                           \"VALUE_TYPE\": 5\n"
    "                        }\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"C5\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":100,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":1048576,\n"
    "                        \"ARGS\":[\n"
    "                           {\n"
    "                              \"TYPE\":100,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":1048576,\n"
    "                              \"ARGS\":[\n"
    "                                 {\n"
    "                                    \"TYPE\":32,\n"
    "                                    \"VALUE_TYPE\":9,\n"
    "                                    \"VALUE_SIZE\":2048,\n"
    "                                    \"COLUMN_IDX\":5\n"
    "                                 },\n"
    "                                 {\n"
    "                                    \"ISNULL\": false,\n"
    "                                    \"VALUE\": \"/\",\n"
    "                                    \"TYPE\": 30,\n"
    "                                    \"VALUE_TYPE\": 9\n"
    "                                 }\n"
    "                              ],\n"
    "                              \"NAME\":\"concat\",\n"
    "                              \"FUNCTION_ID\":124\n"
    "                           },\n"
    "                           {\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":0\n"
    "                           }\n"
    "                        ],\n"
    "                        \"NAME\":\"concat\",\n"
    "                        \"FUNCTION_ID\":124\n"
    "                     }\n"
    "                  }\n"
    "               ]\n"
    "            },\n"
    "            {\n"
    "               \"ID\":8,\n"
    "               \"PLAN_NODE_TYPE\":\"NESTLOOP\",\n"
    "               \"CHILDREN_IDS\":[\n"
    "                  9,\n"
    "                  11\n"
    "               ],\n"
    "               \"OUTPUT_SCHEMA\":[\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":2048,\n"
    "                        \"COLUMN_IDX\":0\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":1\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"MANAGER_ID\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":2\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":3\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"LEVEL\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":5,\n"
    "                        \"COLUMN_IDX\":4\n"
    "                     }\n"
    "                  },\n"
    "                  {\n"
    "                     \"COLUMN_NAME\":\"PATH\",\n"
    "                     \"EXPRESSION\":{\n"
    "                        \"TYPE\":32,\n"
    "                        \"VALUE_TYPE\":9,\n"
    "                        \"VALUE_SIZE\":2048,\n"
    "                        \"COLUMN_IDX\":5\n"
    "                     }\n"
    "                  }\n"
    "               ],\n"
    "               \"JOIN_TYPE\":\"INNER\",\n"
    "               \"PRE_JOIN_PREDICATE\":null,\n"
    "               \"JOIN_PREDICATE\":{\n"
    "                  \"TYPE\":10,\n"
    "                  \"VALUE_TYPE\":23,\n"
    "                  \"LEFT\":{\n"
    "                     \"TYPE\":32,\n"
    "                     \"VALUE_TYPE\":5,\n"
    "                     \"COLUMN_IDX\":0,\n"
    "                     \"TABLE_IDX\":1\n"
    "                  },\n"
    "                  \"RIGHT\":{\n"
    "                     \"TYPE\":32,\n"
    "                     \"VALUE_TYPE\":5,\n"
    "                     \"COLUMN_IDX\":2\n"
    "                  }\n"
    "               },\n"
    "               \"WHERE_PREDICATE\":null\n"
    "            },\n"
    "            {\n"
    "               \"ID\":9,\n"
    "               \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "               \"INLINE_NODES\":[\n"
    "                  {\n"
    "                     \"ID\":10,\n"
    "                     \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "                     \"OUTPUT_SCHEMA\":[\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LAST_NAME\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":0\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":1\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"MANAGER_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":2\n"
    "                           }\n"
    "                        }\n"
    "                     ]\n"
    "                  }\n"
    "               ],\n"
    "               \"TARGET_TABLE_NAME\":\"EMPLOYEES\",\n"
    "               \"TARGET_TABLE_ALIAS\":\"E\"\n"
    "            },\n"
    "            {\n"
    "               \"ID\":11,\n"
    "               \"PLAN_NODE_TYPE\":\"SEQSCAN\",\n"
    "               \"INLINE_NODES\":[\n"
    "                  {\n"
    "                     \"ID\":12,\n"
    "                     \"PLAN_NODE_TYPE\":\"PROJECTION\",\n"
    "                     \"OUTPUT_SCHEMA\":[\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"EMP_ID\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":1\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"LEVEL\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":5,\n"
    "                              \"COLUMN_IDX\":3\n"
    "                           }\n"
    "                        },\n"
    "                        {\n"
    "                           \"COLUMN_NAME\":\"PATH\",\n"
    "                           \"EXPRESSION\":{\n"
    "                              \"TYPE\":32,\n"
    "                              \"VALUE_TYPE\":9,\n"
    "                              \"VALUE_SIZE\":2048,\n"
    "                              \"COLUMN_IDX\":4\n"
    "                           }\n"
    "                        }\n"
    "                     ]\n"
    "                  }\n"
    "               ],\n"
    "               \"TARGET_TABLE_NAME\":\"EMP_PATH\",\n"
    "               \"TARGET_TABLE_ALIAS\":\"EP\",\n"
    "               \"IS_CTE_SCAN\":true,\n"
    "               \"CTE_STMT_ID\":1\n"
    "            }\n"
    "         ]\n"
    "      }\n"
    "   ],\n"
    "   \"EXECUTE_LISTS\":[\n"
    "      {\n"
    "         \"EXECUTE_LIST\":[\n"
    "            3,\n"
    "            2\n"
    "         ]\n"
    "      },\n"
    "      {\n"
    "         \"EXECUTE_LIST\":[\n"
    "            5,\n"
    "            4\n"
    "         ]\n"
    "      },\n"
    "      {\n"
    "         \"EXECUTE_LIST\":[\n"
    "            9,\n"
    "            11,\n"
    "            8,\n"
    "            7\n"
    "         ]\n"
    "      }\n"
    "   ],\n"
    "   \"IS_LARGE_QUERY\":false\n"
    "}\n";

TEST_F(CommonTableExpressionTest, verifyPlan) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    bool success = engine->loadCatalog(0, catalogPayload);
    ASSERT_TRUE(success);

    auto ev = ExecutorVector::fromJsonPlan(engine.get(), jsonPlan, 0);
    ASSERT_NE(NULL, ev.get());

    // Verify the outer query
    auto execList = ev->getExecutorList(0);
    ASSERT_EQ(2, execList.size());

    SeqScanPlanNode* seqScanNode = dynamic_cast<SeqScanPlanNode*>(execList[0]->getPlanNode());
    ASSERT_NE(NULL, seqScanNode);
    ASSERT_TRUE(seqScanNode->isCteScan());
    ASSERT_EQ(1, seqScanNode->getCteStmtId());
    ASSERT_EQ("EMP_PATH", seqScanNode->getTargetTableName());
    ASSERT_NE(std::string::npos, seqScanNode->debugInfo("").find("TargetTable[EMP_PATH], scanType[CTE_SCAN]"));

    OrderByPlanNode* obNode = dynamic_cast<OrderByPlanNode*>(execList[1]->getPlanNode());
    ASSERT_NE(NULL, obNode);

    // verify the common table executor node and the base case
    execList = ev->getExecutorList(1);
    ASSERT_EQ(2, execList.size());

    seqScanNode = dynamic_cast<SeqScanPlanNode*>(execList[0]->getPlanNode());
    ASSERT_NE(NULL, seqScanNode);
    ASSERT_TRUE(seqScanNode->isPersistentTableScan());
    ASSERT_EQ(-1, seqScanNode->getCteStmtId());
    ASSERT_EQ("EMPLOYEES", seqScanNode->getTargetTableName());
    ASSERT_NE(std::string::npos,
              seqScanNode->debugInfo("").find("TargetTable[EMPLOYEES], scanType[PERSISTENT_TABLE_SCAN]"));

    CommonTablePlanNode* ctPlanNode = dynamic_cast<CommonTablePlanNode*>(execList[1]->getPlanNode());
    ASSERT_NE(NULL, ctPlanNode);
    ASSERT_EQ(2, ctPlanNode->getRecursiveStmtId());
    ASSERT_EQ("EMP_PATH", ctPlanNode->getCommonTableName());
    ASSERT_NE(std::string::npos,
              ctPlanNode->debugInfo("").find("CommonTable[EMP_PATH], with recursive stmt id[2]"));

    // verify the recursive query
    execList = ev->getExecutorList(2);
    ASSERT_EQ(4, execList.size());

    // LHS of join is a normal scan of EMPLOYEES
    seqScanNode = dynamic_cast<SeqScanPlanNode*>(execList[0]->getPlanNode());
    ASSERT_NE(NULL, seqScanNode);
    ASSERT_TRUE(seqScanNode->isPersistentTableScan());
    ASSERT_EQ(-1, seqScanNode->getCteStmtId());
    ASSERT_EQ("EMPLOYEES", seqScanNode->getTargetTableName());

    // RHS of join is the intermediate result of the recursive CTE
    seqScanNode = dynamic_cast<SeqScanPlanNode*>(execList[1]->getPlanNode());
    ASSERT_NE(NULL, seqScanNode);
    ASSERT_TRUE(seqScanNode->isCteScan());
    ASSERT_EQ(1, seqScanNode->getCteStmtId());
    ASSERT_EQ("EMP_PATH", seqScanNode->getTargetTableName());
    ASSERT_NE(std::string::npos, seqScanNode->debugInfo("").find("TargetTable[EMP_PATH], scanType[CTE_SCAN]"));

    AbstractJoinPlanNode* joinNode = dynamic_cast<AbstractJoinPlanNode*>(execList[2]->getPlanNode());
    ASSERT_NE(NULL, joinNode);

    ProjectionPlanNode* projNode = dynamic_cast<ProjectionPlanNode*>(execList[3]->getPlanNode());
    ASSERT_NE(NULL, projNode);
}


TEST_F(CommonTableExpressionTest, execute) {
    UniqueEngine engine = UniqueEngineBuilder().build();
    bool success = engine->loadCatalog(0, catalogPayload);
    ASSERT_TRUE(success);

    // Initialize the EMPLOYEES table
    Table* employeesTable = engine->getTableByName("EMPLOYEES");
    typedef std::tuple<std::string, int, boost::optional<int>> InRow;
    std::vector<InRow> persistentTuples{
        InRow{"King",      100, boost::none},
        InRow{"Cambrault", 148, 100},
        InRow{"Bates",     172, 148},
        InRow{"Bloom",     169, 148},
        InRow{"Fox",       170, 148},
        InRow{"Kumar",     173, 148},
        InRow{"Ozer",      168, 148},
        InRow{"Smith",     171, 148},
        InRow{"De Haan",   102, 100},
        InRow{"Hunold",    103, 102},
        InRow{"Austin",    105, 103},
        InRow{"Ernst",     104, 103},
        InRow{"Lorentz",   107, 103},
        InRow{"Pataballa", 106, 103},
        InRow{"Errazuriz", 147, 100},
        InRow{"Ande",      166, 147},
        InRow{"Banda",     167, 147}
    };

    StandAloneTupleStorage storage{employeesTable->schema()};
    TableTuple tupleToInsert = storage.tuple();
    BOOST_FOREACH(auto initValues, persistentTuples) {
        Tools::initTuple(&tupleToInsert, initValues);
        employeesTable->insertTuple(tupleToInsert);
    }

    // Create the executor vector from the hand-coded JSON
    auto ev = ExecutorVector::fromJsonPlan(engine.get(), jsonPlan, 0);
    ASSERT_NE(NULL, ev.get());

    // Execute the fragment and verify the result.
    UniqueTempTableResult result = engine->executePlanFragment(ev.get(), NULL);
    ASSERT_NE(NULL, result.get());

    typedef std::tuple<std::string, int, boost::optional<int>, int64_t, std::string> OutRow;
    std::vector<OutRow> expectedTuples{
        OutRow{"King",      100, boost::none, 1, "King"},
        OutRow{"Cambrault", 148, 100,         2, "King/Cambrault"},
        OutRow{"De Haan",   102, 100,         2, "King/De Haan"},
        OutRow{"Errazuriz", 147, 100,         2, "King/Errazuriz"},
        OutRow{"Bates",     172, 148,         3, "King/Cambrault/Bates"},
        OutRow{"Bloom",     169, 148,         3, "King/Cambrault/Bloom"},
        OutRow{"Fox",       170, 148,         3, "King/Cambrault/Fox"},
        OutRow{"Kumar",     173, 148,         3, "King/Cambrault/Kumar"},
        OutRow{"Ozer",      168, 148,         3, "King/Cambrault/Ozer"},
        OutRow{"Smith",     171, 148,         3, "King/Cambrault/Smith"},
        OutRow{"Hunold",    103, 102,         3, "King/De Haan/Hunold"},
        OutRow{"Ande",      166, 147,         3, "King/Errazuriz/Ande"},
        OutRow{"Banda",     167, 147,         3, "King/Errazuriz/Banda"},
        OutRow{"Austin",    105, 103,         4, "King/De Haan/Hunold/Austin"},
        OutRow{"Ernst",     104, 103,         4, "King/De Haan/Hunold/Ernst"},
        OutRow{"Lorentz",   107, 103,         4, "King/De Haan/Hunold/Lorentz"},
        OutRow{"Pataballa", 106, 103,         4, "King/De Haan/Hunold/Pataballa"}
    };

    int i = 0;
    TableTuple iterTuple{result->schema()};
    TableIterator iter = result->iterator();
    while (iter.next(iterTuple)) {
        ASSERT_TUPLES_EQ(expectedTuples[i], iterTuple);
        ++i;
    }

    // Try executing again, to make sure we clean up intermediate temp tables.
    ExecutorContext::getExecutorContext()->cleanupAllExecutors();
    result = engine->executePlanFragment(ev.get(), NULL);
    ASSERT_NE(NULL, result.get());

    i = 0;
    iter = result->iterator();
    while (iter.next(iterTuple)) {
        ASSERT_TUPLES_EQ(expectedTuples[i], iterTuple);
        ++i;
    }
}

int main() {
    return TestSuite::globalInstance()->runAll();
}
