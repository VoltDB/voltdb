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

package org.voltdb.planner;

public class EEPlanTestGenerator extends PlannerTestCase {
    private static final String DDL_FILENAME = "testplans-ee-generators.sql";
    @Override
    protected void setUp() throws Exception {

        setupSchema(EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                    "testplanseegenerator",
                    false);
    }


    public void testGeneratedPlan() throws Exception {
        TableConfig AAAConfig = new TableConfig("AAA",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    { 1,  10,  101 },
                                                    { 1,  10,  102 },
                                                    { 1,  20,  201 },
                                                    { 1,  20,  202 },
                                                    { 1,  30,  301},
                                                    { 2,  10,  101},
                                                    { 2,  10,  102},
                                                    { 2,  20,  201},
                                                    { 2,  20,  202},
                                                    { 2,  30,  301},
                                                    { 3,  10,  101},
                                                    { 3,  10,  102},
                                                    { 3,  20,  201},
                                                    { 3,  20,  202},
                                                    { 3,  30,  301}});
        TableConfig BBBConfig = new TableConfig("BBB",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    { 1,  10,  101 },
                                                    { 1,  10,  102 },
                                                    { 1,  20,  201 },
                                                    { 1,  20,  202 },
                                                    { 1,  30,  301},
                                                    { 2,  10,  101},
                                                    { 2,  10,  102},
                                                    { 2,  20,  201},
                                                    { 2,  20,  202},
                                                    { 2,  30,  301},
                                                    { 3,  10,  101},
                                                    { 3,  10,  102},
                                                    { 3,  20,  201},
                                                    { 3,  20,  202},
                                                    { 3,  30,  301}});
        DBConfig db = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   AAAConfig,
                                   BBBConfig);
        String sqlStmt;
        sqlStmt = "select A, B from AAA order by A, B;";

        db.addTest(new TestConfig("test_order_by",
                                  sqlStmt,
                                  new int[][] {
                                       { 1,  10},
                                       { 1,  10},
                                       { 1,  20},
                                       { 1,  20},
                                       { 1,  30},
                                       { 2,  10},
                                       { 2,  10},
                                       { 2,  20},
                                       { 2,  20},
                                       { 2,  30},
                                       { 3,  10},
                                       { 3,  10},
                                       { 3,  20},
                                       { 3,  20},
                                       { 3,  30} } ));
        sqlStmt = "select AAA.A, AAA.B, BBB.C from AAA join BBB on AAA.C = BBB.C order by AAA.A, AAA.B, AAA.C;";
        db.addTest(new TestConfig("test_join",
                                  sqlStmt,
                                   new int[][] {
                                       { 1,  10,  101},
                                       { 1,  10,  101},
                                       { 1,  10,  101},
                                       { 1,  10,  102},
                                       { 1,  10,  102},
                                       { 1,  10,  102},
                                       { 1,  20,  201},
                                       { 1,  20,  201},
                                       { 1,  20,  201},
                                       { 1,  20,  202},
                                       { 1,  20,  202},
                                       { 1,  20,  202},
                                       { 1,  30,  301},
                                       { 1,  30,  301},
                                       { 1,  30,  301},
                                       { 2,  10,  101},
                                       { 2,  10,  101},
                                       { 2,  10,  101},
                                       { 2,  10,  102},
                                       { 2,  10,  102},
                                       { 2,  10,  102},
                                       { 2,  20,  201},
                                       { 2,  20,  201},
                                       { 2,  20,  201},
                                       { 2,  20,  202},
                                       { 2,  20,  202},
                                       { 2,  20,  202},
                                       { 2,  30,  301},
                                       { 2,  30,  301},
                                       { 2,  30,  301},
                                       { 3,  10,  101},
                                       { 3,  10,  101},
                                       { 3,  10,  101},
                                       { 3,  10,  102},
                                       { 3,  10,  102},
                                       { 3,  10,  102},
                                       { 3,  20,  201},
                                       { 3,  20,  201},
                                       { 3,  20,  201},
                                       { 3,  20,  202},
                                       { 3,  20,  202},
                                       { 3,  20,  202},
                                       { 3,  30,  301},
                                       { 3,  30,  301},
                                       { 3,  30,  301}} ) );
        generateTests("executors", "TestGeneratedPlans", db);
    }

    public void testGeneratedMaxPlan() throws Exception {
        TableConfig TConfig = new TableConfig("T",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    // A   B     C
                                                    //-------------
                                                    {  1,  1,    1},
                                                    {  1,  1,    2},
                                                    {  1,  1,    3},
                                                    {  1,  1,    4},
                                                    {  1,  1,    5},
                                                    //======================================
                                                    {  1,  2,    1},
                                                    {  1,  2,    2},
                                                    {  1,  2,    3},
                                                    {  1,  2,    4},
                                                    {  1,  2,    5},
                                                    //======================================
                                                    {  1,  3,    1},
                                                    {  1,  3,    2},
                                                    {  1,  3,    3},
                                                    {  1,  3,    4},
                                                    {  1,  3,    5},
                                                    //--------------------------------------
                                                    {  2,  1,    1},
                                                    {  2,  1,    2},
                                                    {  2,  1,    3},
                                                    {  2,  1,    4},
                                                    {  2,  1,    5},
                                                    //======================================
                                                    {  2,  2,    1},
                                                    {  2,  2,    2},
                                                    {  2,  2,    3},
                                                    {  2,  2,    4},
                                                    {  2,  2,    5},
                                                    //======================================
                                                    {  2,  3,    1},
                                                    {  2,  3,    2},
                                                    {  2,  3,    3},
                                                    {  2,  3,    4},
                                                    {  2,  3,    5}
                                                });
        DBConfig countDB = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        countDB.addTest(new TestConfig("test_max_last_row",
                                  "select A, B, max(-1 * abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        countDB.addTest(new TestConfig("test_max_middle_row",
                                  "select A, B, max(-1 * abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        countDB.addTest(new TestConfig("test_max_first_row",
                                  "select A, B, max(-1 * abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        generateTests("executors", "TestWindowedMax", countDB);
    }

    public void testGeneratedMinPlan() throws Exception {
        TableConfig TConfig = new TableConfig("T",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    // A   B     C
                                                    //-------------
                                                    {  1,  1,    1},
                                                    {  1,  1,    2},
                                                    {  1,  1,    3},
                                                    {  1,  1,    4},
                                                    {  1,  1,    5},
                                                    //======================================
                                                    {  1,  2,    1},
                                                    {  1,  2,    2},
                                                    {  1,  2,    3},
                                                    {  1,  2,    4},
                                                    {  1,  2,    5},
                                                    //======================================
                                                    {  1,  3,    1},
                                                    {  1,  3,    2},
                                                    {  1,  3,    3},
                                                    {  1,  3,    4},
                                                    {  1,  3,    5},
                                                    //--------------------------------------
                                                    {  2,  1,    1},
                                                    {  2,  1,    2},
                                                    {  2,  1,    3},
                                                    {  2,  1,    4},
                                                    {  2,  1,    5},
                                                    //======================================
                                                    {  2,  2,    1},
                                                    {  2,  2,    2},
                                                    {  2,  2,    3},
                                                    {  2,  2,    4},
                                                    {  2,  2,    5},
                                                    //======================================
                                                    {  2,  3,    1},
                                                    {  2,  3,    2},
                                                    {  2,  3,    3},
                                                    {  2,  3,    4},
                                                    {  2,  3,    5}
                                                });
        DBConfig countDB = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        countDB.addTest(new TestConfig("test_min_last_row",
                                  "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        countDB.addTest(new TestConfig("test_min_middle_row",
                                  "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        countDB.addTest(new TestConfig("test_min_first_row",
                                  "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               {  1,  1,    0},
                                               //======================================
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               {  1,  2,    0},
                                               //======================================
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               {  1,  3,    0},
                                               //--------------------------------------
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               {  2,  1,    0},
                                               //======================================
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               {  2,  2,    0},
                                               //======================================
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0},
                                               {  2,  3,    0}
        }));

        generateTests("executors", "TestWindowedMin", countDB);
    }

    public void testGeneratedSumPlan() throws Exception {
        TableConfig TConfig = new TableConfig("T",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    // A   B     C
                                                    //-------------
                                                    {  1,  1,    1},
                                                    {  1,  1,    2},
                                                    {  1,  1,    3},
                                                    {  1,  1,    4},
                                                    {  1,  1,    5},
                                                    //======================================
                                                    {  1,  2,    1},
                                                    {  1,  2,    2},
                                                    {  1,  2,    3},
                                                    {  1,  2,    4},
                                                    {  1,  2,    5},
                                                    //======================================
                                                    {  1,  3,    1},
                                                    {  1,  3,    2},
                                                    {  1,  3,    3},
                                                    {  1,  3,    4},
                                                    {  1,  3,    5},
                                                    //--------------------------------------
                                                    {  2,  1,    1},
                                                    {  2,  1,    2},
                                                    {  2,  1,    3},
                                                    {  2,  1,    4},
                                                    {  2,  1,    5},
                                                    //======================================
                                                    {  2,  2,    1},
                                                    {  2,  2,    2},
                                                    {  2,  2,    3},
                                                    {  2,  2,    4},
                                                    {  2,  2,    5},
                                                    //======================================
                                                    {  2,  3,    1},
                                                    {  2,  3,    2},
                                                    {  2,  3,    3},
                                                    {  2,  3,    4},
                                                    {  2,  3,    5}
                                                });
        DBConfig countDB = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        countDB.addTest(new TestConfig("test_min_last_row",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                  new int[][] {{  1,  1,   20},
                                               {  1,  1,   20},
                                               {  1,  1,   20},
                                               {  1,  1,   20},
                                               {  1,  1,   20},
                                               //======================================
                                               {  1,  2,   45},
                                               {  1,  2,   45},
                                               {  1,  2,   45},
                                               {  1,  2,   45},
                                               {  1,  2,   45},
                                               //======================================
                                               {  1,  3,   75},
                                               {  1,  3,   75},
                                               {  1,  3,   75},
                                               {  1,  3,   75},
                                               {  1,  3,   75},
                                               //--------------------------------------
                                               {  2,  1,   20},
                                               {  2,  1,   20},
                                               {  2,  1,   20},
                                               {  2,  1,   20},
                                               {  2,  1,   20},
                                               //======================================
                                               {  2,  2,   45},
                                               {  2,  2,   45},
                                               {  2,  2,   45},
                                               {  2,  2,   45},
                                               {  2,  2,   45},
                                               //======================================
                                               {  2,  3,   75},
                                               {  2,  3,   75},
                                               {  2,  3,   75},
                                               {  2,  3,   75},
                                               {  2,  3,   75}
        }));

        generateTests("executors", "TestWindowedSum", countDB);
    }

    public void testGeneratedCountPlan() throws Exception {
        TableConfig TConfig = new TableConfig("T",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    // A   B     C
                                                    //-------------
                                                    {  1,  1,  101},
                                                    {  1,  1,  102},
                                                    //======================================
                                                    {  1,  2,  201},
                                                    {  1,  2,  202},
                                                    //======================================
                                                    {  1,  3,  203},
                                                    //--------------------------------------
                                                    {  2,  1, 1101},
                                                    {  2,  1, 1102},
                                                    //======================================
                                                    {  2,  2, 1201},
                                                    {  2,  2, 1202},
                                                    //======================================
                                                    {  2,  3, 1203},
                                                    //--------------------------------------
                                                    { 20,  1, 2101},
                                                    { 20,  1, 2102},
                                                    //======================================
                                                    { 20,  2, 2201},
                                                    { 20,  2, 2202},
                                                    //======================================
                                                    { 20,  3, 2203},
                                                    //--------------------------------------
                                                });
        DBConfig countDB = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        String sqlStmt;
        sqlStmt = "select A, B, C, count(*) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count_star",
                                  sqlStmt,
                                  new int[][] {
                                  // A   B    C    count
                                  //--------------------------------------
                                  {  1,  1,  101, 2},
                                  {  1,  1,  102, 2},
                                  //======================================
                                  {  1,  2,  201, 4},
                                  {  1,  2,  202, 4},
                                  //======================================
                                  {  1,  3,  203, 5},
                                  //--------------------------------------
                                  {  2,  1, 1101, 2},
                                  {  2,  1, 1102, 2},
                                  //======================================
                                  {  2,  2, 1201, 4},
                                  {  2,  2, 1202, 4},
                                  //======================================
                                  {  2,  3, 1203, 5},
                                  //--------------------------------------
                                  { 20,  1, 2101, 2},
                                  { 20,  1, 2102, 2},
                                  //======================================
                                  { 20,  2, 2201, 4},
                                  { 20,  2, 2202, 4},
                                  //======================================
                                  { 20,  3, 2203, 5},
                                  //--------------------------------------
        }));;
        sqlStmt = "select A, B, C, count(A+B) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count",
                                  sqlStmt,
                                  new int[][] {
                                  // A   B    C    count
                                  //--------------------------------------
                                  {  1,  1,  101, 2},
                                  {  1,  1,  102, 2},
                                  //======================================
                                  {  1,  2,  201, 4},
                                  {  1,  2,  202, 4},
                                  //======================================
                                  {  1,  3,  203, 5},
                                  //--------------------------------------
                                  {  2,  1, 1101, 2},
                                  {  2,  1, 1102, 2},
                                  //=====================================
                                  {  2,  2, 1201, 4},
                                  {  2,  2, 1202, 4},
                                  //======================================
                                  {  2,  3, 1203, 5},
                                  //--------------------------------------
                                  { 20,  1, 2101, 2},
                                  { 20,  1, 2102, 2},
                                  //======================================
                                  { 20,  2, 2201, 4},
                                  { 20,  2, 2202, 4},
                                  //======================================
                                  { 20,  3, 2203, 5},
                                  //--------------------------------------
        }));;
        generateTests("executors", "TestWindowedCount", countDB);
    }

    public void testGeneratedRankPlan() throws Exception {
        TableConfig TConfig = new TableConfig("T",
                                                new String[] {"A", "B", "C"},
                                                new int[][] {
                                                    // A   B     C
                                                    //-------------
                                                    {  1,  1,  101},
                                                    {  1,  1,  102},
                                                    //======================================
                                                    {  1,  2,  201},
                                                    {  1,  2,  202},
                                                    //======================================
                                                    {  1,  3,  203},
                                                    //--------------------------------------
                                                    {  2,  1, 1101},
                                                    {  2,  1, 1102},
                                                    //======================================
                                                    {  2,  2, 1201},
                                                    {  2,  2, 1202},
                                                    //======================================
                                                    {  2,  3, 1203},
                                                    //--------------------------------------
                                                    { 20,  1, 2101},
                                                    { 20,  1, 2102},
                                                    //======================================
                                                    { 20,  2, 2201},
                                                    { 20,  2, 2202},
                                                    //======================================
                                                    { 20,  3, 2203},
                                                    //--------------------------------------
                                                });
        DBConfig rankDB = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        String sqlStmt;
        sqlStmt = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_rank",
                                  sqlStmt,
                                  new int[][] {
                                  // A   B    C    rank
                                  //--------------------------------------
                                  {  1,  1,  101, 1},
                                  {  1,  1,  102, 1},
                                  //======================================
                                  {  1,  2,  201, 3},
                                  {  1,  2,  202, 3},
                                  //======================================
                                  {  1,  3,  203, 5},
                                  //--------------------------------------
                                  {  2,  1, 1101, 1},
                                  {  2,  1, 1102, 1},
                                  //======================================
                                  {  2,  2, 1201, 3},
                                  {  2,  2, 1202, 3},
                                  //======================================
                                  {  2,  3, 1203, 5},
                                  //--------------------------------------
                                  { 20,  1, 2101, 1},
                                  { 20,  1, 2102, 1},
                                  //======================================
                                  { 20,  2, 2201, 3},
                                  { 20,  2, 2202, 3},
                                  //======================================
                                  { 20,  3, 2203, 5},
                                  //--------------------------------------
        }));;
        sqlStmt = "select A, B, C, dense_rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_dense_rank",
                                  sqlStmt,
                                  new int[][] {
                                  // A   B    C    rank
                                  //--------------------------------------
                                  {  1,  1,  101, 1},
                                  {  1,  1,  102, 1},
                                  //======================================
                                  {  1,  2,  201, 2},
                                  {  1,  2,  202, 2},
                                  //======================================
                                  {  1,  3,  203, 3},
                                  //--------------------------------------
                                  {  2,  1, 1101, 1},
                                  {  2,  1, 1102, 1},
                                  //======================================
                                  {  2,  2, 1201, 2},
                                  {  2,  2, 1202, 2},
                                  //======================================
                                  {  2,  3, 1203, 3},
                                  //--------------------------------------
                                  { 20,  1, 2101, 1},
                                  { 20,  1, 2102, 1},
                                  //======================================
                                  { 20,  2, 2201, 2},
                                  { 20,  2, 2202, 2},
                                  //======================================
                                  { 20,  3, 2203, 3},
                                  //--------------------------------------
        }));;
        generateTests("executors", "TestWindowedRank", rankDB);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(EEPlanTestGenerator.class);
    }

}
