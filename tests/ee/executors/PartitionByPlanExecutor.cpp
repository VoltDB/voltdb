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

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/constraint.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "common/tabletuple.h"
#include "common/valuevector.h"
#include "execution/VoltDBEngine.h"
#include "expressions/abstractexpression.h"
#include "indexes/tableindex.h"
#include "plannodes/abstractplannode.h"
#include "storage/persistenttable.h"
#include "storage/tablefactory.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/tableutil.h"
#include "common/Topend.h"

#include "test_utils/LoadTableFrom.hpp"

#include <cstdlib>
#include <ctime>
#include <unistd.h>
#include <boost/shared_ptr.hpp>

using namespace std;
using namespace voltdb;

const char *plan =
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
        "                        \"COLUMN_IDX\": 3,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                },\n"
        "                {\n"
        "                    \"COLUMN_NAME\": \"C3\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"PARTITION_BY_EXPRESSIONS\": [{\n"
        "                            \"COLUMN_IDX\": 1,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }],\n"
        "                        \"TYPE\": 70,\n"
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
        "                    \"COLUMN_NAME\": \"C3\",\n"
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
        "                    \"COLUMN_NAME\": \"B\",\n"
        "                    \"EXPRESSION\": {\n"
        "                        \"COLUMN_IDX\": 3,\n"
        "                        \"TYPE\": 32,\n"
        "                        \"VALUE_TYPE\": 5\n"
        "                    }\n"
        "                }\n"
        "            ],\n"
        "            \"PLAN_NODE_TYPE\": \"PARTITIONBY\"\n"
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
        "                        \"COLUMN_NAME\": \"A\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 0,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
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
        "}\n";

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
                "set /clusters#cluster/databases#database schema \"eJx9jlEKwDAIQ/93mi5GXX9Xev8rTVcYhW0lKJEnJBQFlbuSihJ7J9jShwuh4Ii5lZdV605Tc1MH2sOwYPJiBVXOjf/hSM3hI56VPb/pVJ0iRoEFlU+aJS58KjRJ\"\n"
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
                "set $PREV matview null\n"
                "set $PREV aggregatetype 0\n"
                "set $PREV matviewsource null\n"
                "set $PREV inbytes false\n"
                "add /clusters#cluster/databases#database/tables#BBB columns B\n"
                "set /clusters#cluster/databases#database/tables#BBB/columns#B index 1\n"
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
                "add /clusters#cluster/databases#database/tables#BBB columns C\n"
                "set /clusters#cluster/databases#database/tables#BBB/columns#C index 2\n"
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
#define NUM_OF_COLUMNS 4
#define NUM_OF_INDEXES 3
#define NUM_OF_TUPLES 10 //must be multiples of 2 for Update test.

//
// Define the column information for the main test table
// This is useful because it will allow us to check different types and other
// configurations without having to dig down into the code
//
ValueType COLUMN_TYPES[NUM_OF_COLUMNS]  = { VALUE_TYPE_INTEGER,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_VARCHAR,
                                                    VALUE_TYPE_INTEGER };
int COLUMN_SIZES[NUM_OF_COLUMNS]                = { 4, 8, 8, 4};
bool COLUMN_ALLOW_NULLS[NUM_OF_COLUMNS]         = { false, true, true, false };

typedef int64_t fragmentId_t;

/**
 * This Topend allows us to get fragments by fragment id.  Other
 * than that, this is just a DummyTopend.
 */
class EngineTestTopend : public DummyTopend {
    typedef std::map<fragmentId_t, std::string> fragmentMap;
    fragmentMap m_fragments;
public:
    void addPlan(fragmentId_t fragmentId, const std::string &planStr) {
        m_fragments[fragmentId] = planStr;
    }
    std::string planForFragmentId(fragmentId_t fragmentId) {
        fragmentMap::iterator it = m_fragments.find(fragmentId);
        if (it == m_fragments.end()) {
            return "";
        } else {
            return it->second;
        }
    }
};

class PartitionByExecutorTest : public Test {
public:
    /**
     * This constructor lets us set the global random seed for the
     * random number generator.  It would be better to have a seed
     * just for this test.  But that is not easily done.
     */
    PartitionByExecutorTest(uint32_t random_seed = (unsigned int)time(NULL))
            : m_cluster_id(1),
              m_site_id(1),
              m_constraint(NULL)
    {
        srand(random_seed);
        /*
         * The schema for this catalog can be found in the
         * sql file voltdb/tests/frontend/org/voltdb/planner/testplans-eng10022.sql.
         * To generate it, start voltdb and load that schema into the database.
         * The file voltdbroot/config_log/catalog.jar will contain a file
         * named catalog.txt, whose contents are this string.  It will need some
         * cleanup in emacs to make it suitable to be a C++ string.
         * All that will be needed is to escape double quotes with a backslash,
         * and surround each line with unescaped double quotes.  But you
         * knew that already.
         */
        m_catalog_string =
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
                "set /clusters#cluster/databases#database schema \"eJx9jlEKwDAIQ/93mi5GXX9Xev8rTVcYhW0lKJEnJBQFlbuSihJ7J9jShwuh4Ii5lZdV605Tc1MH2sOwYPJiBVXOjf/hSM3hI56VPb/pVJ0iRoEFlU+aJS58KjRJ\"\n"
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
                "set $PREV matview null\n"
                "set $PREV aggregatetype 0\n"
                "set $PREV matviewsource null\n"
                "set $PREV inbytes false\n"
                "add /clusters#cluster/databases#database/tables#BBB columns B\n"
                "set /clusters#cluster/databases#database/tables#BBB/columns#B index 1\n"
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
                "add /clusters#cluster/databases#database/tables#BBB columns C\n"
                "set /clusters#cluster/databases#database/tables#BBB/columns#C index 2\n"
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

        /*
         * Initialize the engine.  We create our own
         * topend, to make sure we can supply fragments
         * by id, and then make sure we know where the
         * shared buffers are.  Note that calling setBuffers
         * sets the shared buffer pointers, and calling
         * resetReusedResultOutputBuffer causes the engine to
         * use them.
         */
        m_topend.reset(new EngineTestTopend());
        m_engine.reset(new VoltDBEngine(m_topend.get()));
        m_parameter_buffer.reset(new char [4 * 1024]);
        m_result_buffer.reset(new char [1024 * 1024 * 2]);
        m_exception_buffer.reset(new char [4 * 1024]);
        m_engine->setBuffers(m_parameter_buffer.get(), 4 * 1024,
                             m_result_buffer.get(), 1024 * 1024 * 2,
                             m_exception_buffer.get(), 4096);
        m_engine->resetReusedResultOutputBuffer();
        int partitionCount = 3;
        ASSERT_TRUE(m_engine->initialize(this->m_cluster_id, this->m_site_id, 0, 0, "", 0, 1024, DEFAULT_TEMP_TABLE_MEMORY, false));
        m_engine->updateHashinator( HASHINATOR_LEGACY, (char*)&partitionCount, NULL, 0);
        ASSERT_TRUE(m_engine->loadCatalog( -2, m_catalog_string));

        /*
         * Get a link to the catalog and pull out information about it
         */
        m_catalog = m_engine->getCatalog();
        m_cluster = m_catalog->clusters().get("cluster");
        m_database = m_cluster->databases().get("database");
        m_database_id = m_database->relativeIndex();
        catalog::Table *partitioned_catalog_table_customer = m_database->tables().get("D_CUSTOMER");
        m_partitioned_customer_table_id = partitioned_catalog_table_customer->relativeIndex();
        m_partitioned_customer_table = dynamic_cast<PersistentTable*>(m_engine->getTable(m_partitioned_customer_table_id));
        catalog::Table *replicated_catalog_table_customer = m_database->tables().get("R_CUSTOMER");
        m_replicated_customer_table_id = replicated_catalog_table_customer->relativeIndex();
        m_replicated_customer_table = m_engine->getTable(m_replicated_customer_table_id);

        //
        // Fill in tuples.  The IndexOrder test does not use
        // the contents of the tables.  The ExecutionEngineTest does
        // use them, in a somewhat trivial way.  It would be good if
        // there was some way to fill these in deterministically.
        // Random tuples make debugging unnecessarily difficult,
        // especially if the random see is the time the test
        // started.
        //
        ASSERT_TRUE(m_partitioned_customer_table);
        ASSERT_TRUE(tableutil::addRandomTuples(m_partitioned_customer_table, NUM_OF_TUPLES));
        ASSERT_TRUE(m_replicated_customer_table);
        ASSERT_TRUE(tableutil::addRandomTuples(m_replicated_customer_table, NUM_OF_TUPLES));
    }

    ~PartitionByExecutorTest() { }

    protected:
        CatalogId m_cluster_id;
        CatalogId m_database_id;
        CatalogId m_site_id;
        string m_catalog_string;
        catalog::Catalog *m_catalog; //This is not the real catalog that the VoltDBEngine uses. It is a duplicate made locally to get GUIDs
        catalog::Cluster *m_cluster;
        catalog::Database *m_database;
        catalog::Constraint *m_constraint;
        boost::scoped_ptr<VoltDBEngine>     m_engine;
        boost::scoped_ptr<EngineTestTopend> m_topend;
        PersistentTable* m_partitioned_customer_table;
        int m_partitioned_customer_table_id;

        Table* m_replicated_customer_table;
        int m_replicated_customer_table_id;
        void compareTables(Table *first, Table* second);
        boost::shared_array<char>m_result_buffer;
        boost::shared_array<char>m_exception_buffer;
        boost::shared_array<char>m_parameter_buffer;
};

// ------------------------------------------------------------------
// Execute_PlanFragmentInfo
// ------------------------------------------------------------------
// This plan is the plan generated from the query in the test
// org.voltdb.planner.TestPlansENG10022.testPlanENG10022.  It
// can be generated with this onerously complicated procedure.
// 1. Run the test with the JVM command line parameter -Dmumble=compilerdebug.
//    It doesn't matter what property name you use, mumble in this
//    case.  But the property value must be compilerdebug.  An
//    easy way to do this in Eclipse is to create a JRE configuration
//    with -Dmumble=compilerdebug and run the testENG10022 test with
//    this configuration.  Perhaps setting VOLTDB_OPTS when running
//    the ant target for the test works as well.
// 2. When the test runs there will be a folder called debugoutput
//    wherever the test is run.  In the Eclipse method above this will
//    be the project folder, which is the root of the source folder.
//    There you will find a file with name something like
//           debugoutput/statement_plans/ENG-10022-stmt-0_json.txt.
//    The contents of this file should be this string.
// 3. Some emacs cleanup will be required to paste this into
//    the source code as a string.  Escape double quotes and add
//    initial double quotes, '\n' lines and terminal double quotes
//    as usual.
namespace {
std::string plan =
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
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
        "            \"ID\": 2,\n"
        "            \"INLINE_NODES\": [{\n"
        "                \"ID\": 3,\n"
        "                \"OUTPUT_SCHEMA\": [\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"CID\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"COLUMN_IDX\": 0,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        }\n"
        "                    },\n"
        "                    {\n"
        "                        \"COLUMN_NAME\": \"CID2\",\n"
        "                        \"EXPRESSION\": {\n"
        "                            \"LEFT\": {\n"
        "                                \"ISNULL\": false,\n"
        "                                \"TYPE\": 30,\n"
        "                                \"VALUE\": 2,\n"
        "                                \"VALUE_TYPE\": 5\n"
        "                            },\n"
        "                            \"RIGHT\": {\n"
        "                                \"COLUMN_IDX\": 0,\n"
        "                                \"TYPE\": 32,\n"
        "                                \"VALUE_TYPE\": 5\n"
        "                            },\n"
        "                            \"TYPE\": 3,\n"
        "                            \"VALUE_TYPE\": 6\n"
        "                        }\n"
        "                    }\n"
        "                ],\n"
        "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
        "            }],\n"
        "            \"LOOKUP_TYPE\": \"GTE\",\n"
        "            \"PLAN_NODE_TYPE\": \"INDEXSCAN\",\n"
        "            \"PURPOSE\": 3,\n"
        "            \"SORT_DIRECTION\": \"ASC\",\n"
        "            \"TARGET_INDEX_NAME\": \"VOLTDB_AUTOGEN_IDX_PK_R_CUSTOMER_R_CUSTOMERID\",\n"
        "            \"TARGET_TABLE_ALIAS\": \"R_CUSTOMER\",\n"
        "            \"TARGET_TABLE_NAME\": \"R_CUSTOMER\"\n"
        "        }\n"
        "    ]\n"
        "}\n"
        "\n";
}

};

TEST_F(PartitionByExecutorTest, testPartitionBy) {

}

int main() {
     return TestSuite::globalInstance()->runAll();
}
