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

class TestWindowedMin : public PlanTestingBaseClass<EngineTestTopend> {
public:
    /*
     * This constructor lets us set the global random seed for the
     * random number generator.  It would be better to have a seed
     * just for this test.  But that is not easily done.
     */
    TestWindowedMin(uint32_t randomSeed = (unsigned int)time(NULL)) {
        initialize(m_PartitionByExecutorDB, randomSeed);
    }

    ~TestWindowedMin() { }
protected:
    static DBConfig         m_PartitionByExecutorDB;
};

TEST_F(TestWindowedMin, test_min_last_row) {
    static int testIndex = 0;
    executeTest(allTests[testIndex]);
}
TEST_F(TestWindowedMin, test_min_middle_row) {
    static int testIndex = 1;
    executeTest(allTests[testIndex]);
}
TEST_F(TestWindowedMin, test_min_first_row) {
    static int testIndex = 2;
    executeTest(allTests[testIndex]);
}


namespace {
const char *T_ColumnNames[] = {
    "A"
    "B",
    "C",
};


const int NUM_TABLE_ROWS_T = 30;
const int NUM_TABLE_COLS_T = 3;
const int TData[NUM_TABLE_ROWS_T * NUM_TABLE_COLS_T] = {
      1,  1,  1,
      1,  1,  2,
      1,  1,  3,
      1,  1,  4,
      1,  1,  5,
      1,  2,  1,
      1,  2,  2,
      1,  2,  3,
      1,  2,  4,
      1,  2,  5,
      1,  3,  1,
      1,  3,  2,
      1,  3,  3,
      1,  3,  4,
      1,  3,  5,
      2,  1,  1,
      2,  1,  2,
      2,  1,  3,
      2,  1,  4,
      2,  1,  5,
      2,  2,  1,
      2,  2,  2,
      2,  2,  3,
      2,  2,  4,
      2,  2,  5,
      2,  3,  1,
      2,  3,  2,
      2,  3,  3,
      2,  3,  4,
      2,  3,  5,
};



const TableConfig TConfig = {
    "T",
    T_ColumnNames,
    NUM_TABLE_ROWS_T,
    NUM_TABLE_COLS_T,
    TData
};


const TableConfig *allTables[] = {
    &TConfig,

};

const int NUM_OUTPUT_ROWS_TEST_MIN_LAST_ROW = 30;
const int NUM_OUTPUT_COLS_TEST_MIN_LAST_ROW = 3;
const int outputTable_test_min_last_row[NUM_OUTPUT_ROWS_TEST_MIN_LAST_ROW * NUM_OUTPUT_COLS_TEST_MIN_LAST_ROW] = {
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
};

const int NUM_OUTPUT_ROWS_TEST_MIN_MIDDLE_ROW = 30;
const int NUM_OUTPUT_COLS_TEST_MIN_MIDDLE_ROW = 3;
const int outputTable_test_min_middle_row[NUM_OUTPUT_ROWS_TEST_MIN_MIDDLE_ROW * NUM_OUTPUT_COLS_TEST_MIN_MIDDLE_ROW] = {
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
};

const int NUM_OUTPUT_ROWS_TEST_MIN_FIRST_ROW = 30;
const int NUM_OUTPUT_COLS_TEST_MIN_FIRST_ROW = 3;
const int outputTable_test_min_first_row[NUM_OUTPUT_ROWS_TEST_MIN_FIRST_ROW * NUM_OUTPUT_COLS_TEST_MIN_FIRST_ROW] = {
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  1,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  2,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      1,  3,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  1,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  2,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
      2,  3,  0,
};



TestConfig allTests[3] = {
    {
        // SQL Statement
        "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
        // Plan String
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        6,\n"
        "        5,\n"
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
        "            \"CHILDREN_IDS\": [4],\n"
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
        "                        \"COLUMN_IDX\": 2,\n"
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
        "                \"AGGREGATE_EXPRESSIONS\": [{\n"
        "                    \"ARGS\": [{\n"
        "                        \"LEFT\": {\n"
        "                            \"ISNULL\": false,\n"
        "                            \"TYPE\": 30,\n"
        "                            \"VALUE\": 5,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"RIGHT\": {\n"
        "                            \"COLUMN_IDX\": 2,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"TYPE\": 2,\n"
        "                        \"VALUE_TYPE\": 6\n"
        "                    }],\n"
        "                    \"FUNCTION_ID\": 10,\n"
        "                    \"NAME\": \"abs\",\n"
        "                    \"RESULT_TYPE_PARAM_IDX\": 0,\n"
        "                    \"TYPE\": 100,\n"
        "                    \"VALUE_TYPE\": 6\n"
        "                }],\n"
        "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
        "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_MIN\"\n"
        "            }],\n"
        "            \"CHILDREN_IDS\": [5],\n"
        "            \"ID\": 4,\n"
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
        "            \"PARTITIONBY_EXPRESSIONS\": [{\n"
        "                \"COLUMN_IDX\": 0,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\",\n"
        "            \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
        "                \"COLUMN_IDX\": 1,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }}]\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [6],\n"
        "            \"ID\": 5,\n"
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
        "            \"ID\": 6,\n"
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
        "            \"TARGET_TABLE_ALIAS\": \"T\",\n"
        "            \"TARGET_TABLE_NAME\": \"T\"\n"
        "        }\n"
        "    ]\n"
        "}",
        NUM_OUTPUT_ROWS_TEST_MIN_LAST_ROW,
        NUM_OUTPUT_COLS_TEST_MIN_LAST_ROW,
        outputTable_test_min_last_row
    },
    {
        // SQL Statement
        "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
        // Plan String
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        6,\n"
        "        5,\n"
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
        "            \"CHILDREN_IDS\": [4],\n"
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
        "                        \"COLUMN_IDX\": 2,\n"
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
        "                \"AGGREGATE_EXPRESSIONS\": [{\n"
        "                    \"ARGS\": [{\n"
        "                        \"LEFT\": {\n"
        "                            \"ISNULL\": false,\n"
        "                            \"TYPE\": 30,\n"
        "                            \"VALUE\": 3,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"RIGHT\": {\n"
        "                            \"COLUMN_IDX\": 2,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"TYPE\": 2,\n"
        "                        \"VALUE_TYPE\": 6\n"
        "                    }],\n"
        "                    \"FUNCTION_ID\": 10,\n"
        "                    \"NAME\": \"abs\",\n"
        "                    \"RESULT_TYPE_PARAM_IDX\": 0,\n"
        "                    \"TYPE\": 100,\n"
        "                    \"VALUE_TYPE\": 6\n"
        "                }],\n"
        "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
        "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_MIN\"\n"
        "            }],\n"
        "            \"CHILDREN_IDS\": [5],\n"
        "            \"ID\": 4,\n"
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
        "            \"PARTITIONBY_EXPRESSIONS\": [{\n"
        "                \"COLUMN_IDX\": 0,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\",\n"
        "            \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
        "                \"COLUMN_IDX\": 1,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }}]\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [6],\n"
        "            \"ID\": 5,\n"
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
        "            \"ID\": 6,\n"
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
        "            \"TARGET_TABLE_ALIAS\": \"T\",\n"
        "            \"TARGET_TABLE_NAME\": \"T\"\n"
        "        }\n"
        "    ]\n"
        "}",
        NUM_OUTPUT_ROWS_TEST_MIN_MIDDLE_ROW,
        NUM_OUTPUT_COLS_TEST_MIN_MIDDLE_ROW,
        outputTable_test_min_middle_row
    },
    {
        // SQL Statement
        "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
        // Plan String
        "{\n"
        "    \"EXECUTE_LIST\": [\n"
        "        6,\n"
        "        5,\n"
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
        "            \"CHILDREN_IDS\": [4],\n"
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
        "                        \"COLUMN_IDX\": 2,\n"
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
        "                \"AGGREGATE_EXPRESSIONS\": [{\n"
        "                    \"ARGS\": [{\n"
        "                        \"LEFT\": {\n"
        "                            \"ISNULL\": false,\n"
        "                            \"TYPE\": 30,\n"
        "                            \"VALUE\": 1,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"RIGHT\": {\n"
        "                            \"COLUMN_IDX\": 2,\n"
        "                            \"TYPE\": 32,\n"
        "                            \"VALUE_TYPE\": 5\n"
        "                        },\n"
        "                        \"TYPE\": 2,\n"
        "                        \"VALUE_TYPE\": 6\n"
        "                    }],\n"
        "                    \"FUNCTION_ID\": 10,\n"
        "                    \"NAME\": \"abs\",\n"
        "                    \"RESULT_TYPE_PARAM_IDX\": 0,\n"
        "                    \"TYPE\": 100,\n"
        "                    \"VALUE_TYPE\": 6\n"
        "                }],\n"
        "                \"AGGREGATE_OUTPUT_COLUMN\": 0,\n"
        "                \"AGGREGATE_TYPE\": \"AGGREGATE_WINDOWED_MIN\"\n"
        "            }],\n"
        "            \"CHILDREN_IDS\": [5],\n"
        "            \"ID\": 4,\n"
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
        "            \"PARTITIONBY_EXPRESSIONS\": [{\n"
        "                \"COLUMN_IDX\": 0,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }],\n"
        "            \"PLAN_NODE_TYPE\": \"WINDOWFUNCTION\",\n"
        "            \"SORT_COLUMNS\": [{\"SORT_EXPRESSION\": {\n"
        "                \"COLUMN_IDX\": 1,\n"
        "                \"TYPE\": 32,\n"
        "                \"VALUE_TYPE\": 5\n"
        "            }}]\n"
        "        },\n"
        "        {\n"
        "            \"CHILDREN_IDS\": [6],\n"
        "            \"ID\": 5,\n"
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
        "            \"ID\": 6,\n"
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
        "            \"TARGET_TABLE_ALIAS\": \"T\",\n"
        "            \"TARGET_TABLE_NAME\": \"T\"\n"
        "        }\n"
        "    ]\n"
        "}",
        NUM_OUTPUT_ROWS_TEST_MIN_FIRST_ROW,
        NUM_OUTPUT_COLS_TEST_MIN_FIRST_ROW,
        outputTable_test_min_first_row
    },
};

}

DBConfig TestWindowedMin::m_PartitionByExecutorDB =

{
    //
    // DDL.
    //
    "drop table T if exists;\n"
    "drop table AAA if exists;\n"
    "drop table BBB if exists;\n"
    "drop table R1 if exists;\n"
    "\n"
    "CREATE TABLE T (\n"
    "  A INTEGER,\n"
    "  B INTEGER,\n"
    "  C INTEGER\n"
    ");\n"
    "\n"
    "CREATE TABLE R1 (\n"
    "  ID INTEGER NOT NULL,\n"
    "  TINY INTEGER NOT NULL,\n"
    "  BIG INTEGER NOT NULL,\n"
    "  PRIMARY KEY (ID)\n"
    ");\n"
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
    " \n"
    "-- Order By Table, from the order by suite.\n"
    "--\n"
    "CREATE TABLE O1 (\n"
    " PKEY          INTEGER NOT NULL,\n"
    " A_INT         INTEGER,\n"
    " PRIMARY KEY (PKEY)\n"
    ");\n"
    "\n"
    "PARTITION TABLE O1 ON COLUMN PKEY;\n"
    "CREATE INDEX IDX_O1_A_INT_PKEY on O1 (A_INT, PKEY);\n"
    "",
    //
    // Catalog String
    //
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 0\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno 0\n"
    "set $PREV jsonapi false\n"
    "set $PREV networkpartition false\n"
    "set $PREV heartbeatTimeout 0\n"
    "set $PREV useddlschema false\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled false\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 0\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drFlushInterval 0\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"eJy1UkFyhDAMu/c1wZFtfN2U/P9JlVkKdIBd9tDJJMNgOZKsGFyse5HisMHEmqkUhRQLM57qo4VXh9f6+LJTOIZcn7VIro9aVOoVB6oKFIMCs3rKETQsTmTkLimTOzAlCg5VkbZU5LJSD5Ui8Zpy1rmSBnC8AhN6SuNfZVf7pSMmkXG/g6zBcd1noD5iv6lcZp4H8ZF6Z6Wx2LPKCNQGBqDnYe+nylCmRJozmD9TvajUQ6W8J16ezD8RXweKvgWq28AO614EZOhP5Nsb2uvAVi1xaiEvA790fL5CHUlMKzxXCdo3Q9Zt2szuZLad7aT5AeGp3Yc=\"\n"
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
    "add /clusters#cluster/databases#database tables O1\n"
    "set /clusters#cluster/databases#database/tables#O1 isreplicated false\n"
    "set $PREV partitioncolumn /clusters#cluster/databases#database/tables#O1/columns#PKEY\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"O1|ii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#O1 columns A_INT\n"
    "set /clusters#cluster/databases#database/tables#O1/columns#A_INT index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable true\n"
    "set $PREV name \"A_INT\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#O1 columns PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/columns#PKEY index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"PKEY\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#O1 indexes IDX_O1_A_INT_PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/indexes#IDX_O1_A_INT_PKEY unique false\n"
    "set $PREV assumeUnique false\n"
    "set $PREV countable true\n"
    "set $PREV type 1\n"
    "set $PREV expressionsjson \"\"\n"
    "set $PREV predicatejson \"\"\n"
    "add /clusters#cluster/databases#database/tables#O1/indexes#IDX_O1_A_INT_PKEY columns A_INT\n"
    "set /clusters#cluster/databases#database/tables#O1/indexes#IDX_O1_A_INT_PKEY/columns#A_INT index 0\n"
    "set $PREV column /clusters#cluster/databases#database/tables#O1/columns#A_INT\n"
    "add /clusters#cluster/databases#database/tables#O1/indexes#IDX_O1_A_INT_PKEY columns PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/indexes#IDX_O1_A_INT_PKEY/columns#PKEY index 1\n"
    "set $PREV column /clusters#cluster/databases#database/tables#O1/columns#PKEY\n"
    "add /clusters#cluster/databases#database/tables#O1 indexes VOLTDB_AUTOGEN_IDX_PK_O1_PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/indexes#VOLTDB_AUTOGEN_IDX_PK_O1_PKEY unique true\n"
    "set $PREV assumeUnique false\n"
    "set $PREV countable true\n"
    "set $PREV type 1\n"
    "set $PREV expressionsjson \"\"\n"
    "set $PREV predicatejson \"\"\n"
    "add /clusters#cluster/databases#database/tables#O1/indexes#VOLTDB_AUTOGEN_IDX_PK_O1_PKEY columns PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/indexes#VOLTDB_AUTOGEN_IDX_PK_O1_PKEY/columns#PKEY index 0\n"
    "set $PREV column /clusters#cluster/databases#database/tables#O1/columns#PKEY\n"
    "add /clusters#cluster/databases#database/tables#O1 constraints VOLTDB_AUTOGEN_IDX_PK_O1_PKEY\n"
    "set /clusters#cluster/databases#database/tables#O1/constraints#VOLTDB_AUTOGEN_IDX_PK_O1_PKEY type 4\n"
    "set $PREV oncommit \"\"\n"
    "set $PREV index /clusters#cluster/databases#database/tables#O1/indexes#VOLTDB_AUTOGEN_IDX_PK_O1_PKEY\n"
    "set $PREV foreignkeytable null\n"
    "add /clusters#cluster/databases#database tables R1\n"
    "set /clusters#cluster/databases#database/tables#R1 isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"R1|iii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#R1 columns BIG\n"
    "set /clusters#cluster/databases#database/tables#R1/columns#BIG index 2\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"BIG\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#R1 columns ID\n"
    "set /clusters#cluster/databases#database/tables#R1/columns#ID index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"ID\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#R1 columns TINY\n"
    "set /clusters#cluster/databases#database/tables#R1/columns#TINY index 1\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"TINY\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#R1 indexes VOLTDB_AUTOGEN_IDX_PK_R1_ID\n"
    "set /clusters#cluster/databases#database/tables#R1/indexes#VOLTDB_AUTOGEN_IDX_PK_R1_ID unique true\n"
    "set $PREV assumeUnique false\n"
    "set $PREV countable true\n"
    "set $PREV type 1\n"
    "set $PREV expressionsjson \"\"\n"
    "set $PREV predicatejson \"\"\n"
    "add /clusters#cluster/databases#database/tables#R1/indexes#VOLTDB_AUTOGEN_IDX_PK_R1_ID columns ID\n"
    "set /clusters#cluster/databases#database/tables#R1/indexes#VOLTDB_AUTOGEN_IDX_PK_R1_ID/columns#ID index 0\n"
    "set $PREV column /clusters#cluster/databases#database/tables#R1/columns#ID\n"
    "add /clusters#cluster/databases#database/tables#R1 constraints VOLTDB_AUTOGEN_IDX_PK_R1_ID\n"
    "set /clusters#cluster/databases#database/tables#R1/constraints#VOLTDB_AUTOGEN_IDX_PK_R1_ID type 4\n"
    "set $PREV oncommit \"\"\n"
    "set $PREV index /clusters#cluster/databases#database/tables#R1/indexes#VOLTDB_AUTOGEN_IDX_PK_R1_ID\n"
    "set $PREV foreignkeytable null\n"
    "add /clusters#cluster/databases#database tables T\n"
    "set /clusters#cluster/databases#database/tables#T isreplicated true\n"
    "set $PREV partitioncolumn null\n"
    "set $PREV estimatedtuplecount 0\n"
    "set $PREV materializer null\n"
    "set $PREV signature \"T|iii\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#T columns A\n"
    "set /clusters#cluster/databases#database/tables#T/columns#A index 0\n"
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
    "add /clusters#cluster/databases#database/tables#T columns B\n"
    "set /clusters#cluster/databases#database/tables#T/columns#B index 1\n"
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
    "add /clusters#cluster/databases#database/tables#T columns C\n"
    "set /clusters#cluster/databases#database/tables#T/columns#C index 2\n"
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
    1,
    allTables
};


int main() {
     return TestSuite::globalInstance()->runAll();
}
