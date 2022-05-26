/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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

#include <string>

/*
 * A way to generate this catalog payload string is to use Ethan's branch named
 * "CatalogPayload-sysproc-notest".
 * After executing all the DDLs, run "exec @CatalogPayload;".
 * To generate the JSON string for a query, run "exec @JSONPlan '<query>';".
 *
 * DDL:
 * CREATE TABLE (a INT NOT NULL, b FLOAT NOT NULL, c VARCHAR(10) NOT NULL);
 *
 * Note that table T is defined as a replicated table.
 * But the query plans below are planned with forceSP() because only
 * one partition will be initialized in the test.
 */
const char *catalogPayload =
    "add / clusters cluster\n"
    "set /clusters#cluster localepoch 1199145600\n"
    "set $PREV securityEnabled false\n"
    "set $PREV httpdportno 0\n"
    "set $PREV jsonapi true\n"
    "set $PREV networkpartition true\n"
    "set $PREV heartbeatTimeout 90\n"
    "set $PREV useddlschema true\n"
    "set $PREV drConsumerEnabled false\n"
    "set $PREV drProducerEnabled false\n"
    "set $PREV drRole \"none\"\n"
    "set $PREV drClusterId 0\n"
    "set $PREV drProducerPort 0\n"
    "set $PREV drMasterHost \"\"\n"
    "set $PREV drFlushInterval 1\n"
    "set $PREV exportFlushInterval 1\n"
    "add /clusters#cluster databases database\n"
    "set /clusters#cluster/databases#database schema \"eJx1jDkOwCAMBPu8xl6HAdog+P+TYkSbFN5DHi1RRcHrTZGlOmKcLFPDZXTmbkzW8VqSGRqZlUe2tTd+mMg18i+Ctl0tPEz9g+/xXC87nh+P\"\n"
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
    "set $PREV signature \"T|ifv\"\n"
    "set $PREV tuplelimit 2147483647\n"
    "set $PREV isDRed false\n"
    "add /clusters#cluster/databases#database/tables#T columns A\n"
    "set /clusters#cluster/databases#database/tables#T/columns#A index 0\n"
    "set $PREV type 5\n"
    "set $PREV size 4\n"
    "set $PREV nullable false\n"
    "set $PREV name \"A\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#T columns B\n"
    "set /clusters#cluster/databases#database/tables#T/columns#B index 1\n"
    "set $PREV type 8\n"
    "set $PREV size 8\n"
    "set $PREV nullable false\n"
    "set $PREV name \"B\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n"
    "add /clusters#cluster/databases#database/tables#T columns C\n"
    "set /clusters#cluster/databases#database/tables#T/columns#C index 2\n"
    "set $PREV type 9\n"
    "set $PREV size 10\n"
    "set $PREV nullable false\n"
    "set $PREV name \"C\"\n"
    "set $PREV defaultvalue null\n"
    "set $PREV defaulttype 0\n"
    "set $PREV aggregatetype 0\n"
    "set $PREV matviewsource null\n"
    "set $PREV matview null\n"
    "set $PREV inbytes false\n";

// INSERT INTO T VALUES (?, ?, ?);
std::string anInsertPlan =
    "{\n"
    "    \"EXECUTE_LIST\": [\n"
    "        2,\n"
    "        1\n"
    "    ],\n"
    "    \"PLAN_NODES\": [\n"
    "        {\n"
    "            \"CHILDREN_IDS\": [2],\n"
    "            \"FIELD_MAP\": [\n"
    "                0,\n"
    "                1,\n"
    "                2\n"
    "            ],\n"
    "            \"ID\": 1,\n"
    "            \"MULTI_PARTITION\": false,\n"
    "            \"PLAN_NODE_TYPE\": \"INSERT\",\n"
    "            \"TARGET_TABLE_NAME\": \"T\"\n"
    "        },\n"
    "        {\n"
    "            \"BATCHED\": false,\n"
    "            \"ID\": 2,\n"
    "            \"OUTPUT_SCHEMA\": [\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"A\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"PARAM_IDX\": 0,\n"
    "                        \"TYPE\": 31,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"B\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"PARAM_IDX\": 1,\n"
    "                        \"TYPE\": 31,\n"
    "                        \"VALUE_TYPE\": 8\n"
    "                    }\n"
    "                },\n"
    "                {\n"
    "                    \"COLUMN_NAME\": \"C\",\n"
    "                    \"EXPRESSION\": {\n"
    "                        \"PARAM_IDX\": 2,\n"
    "                        \"TYPE\": 31,\n"
    "                        \"VALUE_SIZE\": 10,\n"
    "                        \"VALUE_TYPE\": 9\n"
    "                    }\n"
    "                }\n"
    "            ],\n"
    "            \"PLAN_NODE_TYPE\": \"MATERIALIZE\"\n"
    "        }\n"
    "    ]\n"
    "}\n";

// SELECT * FROM T WHERE a = ? AND b >= ? AND C like ?;
std::string aSelectPlan =
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
    "                            \"VALUE_TYPE\": 8\n"
    "                        }\n"
    "                    },\n"
    "                    {\n"
    "                        \"COLUMN_NAME\": \"C\",\n"
    "                        \"EXPRESSION\": {\n"
    "                            \"COLUMN_IDX\": 2,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_SIZE\": 10,\n"
    "                            \"VALUE_TYPE\": 9\n"
    "                        }\n"
    "                    }\n"
    "                ],\n"
    "                \"PLAN_NODE_TYPE\": \"PROJECTION\"\n"
    "            }],\n"
    "            \"PLAN_NODE_TYPE\": \"SEQSCAN\",\n"
    "            \"PREDICATE\": {\n"
    "                \"LEFT\": {\n"
    "                    \"LEFT\": {\n"
    "                        \"LEFT\": {\n"
    "                            \"COLUMN_IDX\": 1,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_TYPE\": 8\n"
    "                        },\n"
    "                        \"RIGHT\": {\n"
    "                            \"PARAM_IDX\": 1,\n"
    "                            \"TYPE\": 31,\n"
    "                            \"VALUE_TYPE\": 8\n"
    "                        },\n"
    "                        \"TYPE\": 15,\n"
    "                        \"VALUE_TYPE\": 23\n"
    "                    },\n"
    "                    \"RIGHT\": {\n"
    "                        \"LEFT\": {\n"
    "                            \"COLUMN_IDX\": 2,\n"
    "                            \"TYPE\": 32,\n"
    "                            \"VALUE_SIZE\": 10,\n"
    "                            \"VALUE_TYPE\": 9\n"
    "                        },\n"
    "                        \"RIGHT\": {\n"
    "                            \"PARAM_IDX\": 2,\n"
    "                            \"TYPE\": 31,\n"
    "                            \"VALUE_SIZE\": 0,\n"
    "                            \"VALUE_TYPE\": 9\n"
    "                        },\n"
    "                        \"TYPE\": 16,\n"
    "                        \"VALUE_TYPE\": 23\n"
    "                    },\n"
    "                    \"TYPE\": 20,\n"
    "                    \"VALUE_TYPE\": 23\n"
    "                },\n"
    "                \"RIGHT\": {\n"
    "                    \"LEFT\": {\n"
    "                        \"COLUMN_IDX\": 0,\n"
    "                        \"TYPE\": 32,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    },\n"
    "                    \"RIGHT\": {\n"
    "                        \"PARAM_IDX\": 0,\n"
    "                        \"TYPE\": 31,\n"
    "                        \"VALUE_TYPE\": 5\n"
    "                    },\n"
    "                    \"TYPE\": 10,\n"
    "                    \"VALUE_TYPE\": 23\n"
    "                },\n"
    "                \"TYPE\": 20,\n"
    "                \"VALUE_TYPE\": 23\n"
    "            },\n"
    "            \"TARGET_TABLE_ALIAS\": \"T\",\n"
    "            \"TARGET_TABLE_NAME\": \"T\"\n"
    "        }\n"
    "    ]\n"
    "}\n";
