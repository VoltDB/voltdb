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

package org.voltdb.planner;

import org.voltdb.VoltType;

/**
 * This is not a jUnit test, though it looks a little bit like one.  It's
 * an example of a Java application which generates EE Unit tests.  To
 * run this run the Java class EEPlanTestGenerator as a java application.
 *
 * The idea is pretty simple.  You set up for a jUnit test as usual, loading
 * a schema from a file or from Java code.  But instead of running the SQL
 * compiler and looking for plans or error messages, follow these steps.
 * <ul>
 *   <li>
 *     Create a database.  This is a set of tables and their contents.
 *     A table schema is an array of column names, and a table is an
 *     array of arrays if ints, where the inner arrays all have the same
 *     number of elements as the array of column names.  So, each outer
 *     array is a row.  Right now we can only create tables with int values,
 *     though we intend to expand that.
 *   </li>
 *   <li>
 *     Create some tests.  Each of these are given by
 *     <ul>
 *       <li>
 *         A test name.  This will be the name of the unit test function,
 *         so it should be a legal C identifier.
 *       </li>
 *       <li>
 *         A SQL query string.
 *       </li>
 *       <li>
 *         An expected output table.  This will be a two dimensional
 *         array of ints, where the outer dimensions are rows.  Again,
 *         we can only have tables of integers here.
 *       </li>
 *     </ul>
 *     Each of these tests are added to the database created in the first
 *     step.
 *   </li>
 *   <li>
 *     Create the test by calling generateTests(category, testname, db).
 *     This will create a C++ file named category/testname.cpp which will
 *     execute the plan for the test and compare the result with the expected
 *     result.  Note that non-deterministic tests will be problematic here.
 *     So, use order by and limit judiciously.
 *   </li>
 * </ul>
 * Follow the example below.
 *
 * @author bwhite
 *
 */
public class EEPlanTestGenerator extends PlannerTestCase {
    private static final String DDL_FILENAME = "testplans-ee-generators.sql";
    @Override
    protected void setUp() throws Exception {

        setupSchema(EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                    "testplanseegenerator",
                    false);
    }


    public void generatedPlannerTest() throws Exception {
        TableConfig AAAConfig = new TableConfig("AAA",
                                                new SchemaConfig(
                                                        new ColumnConfig("A", VoltType.INTEGER),
                                                        new ColumnConfig("B", VoltType.INTEGER),
                                                        new ColumnConfig("C", VoltType.INTEGER)),
                                                new Object[][] {
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
                                                new SchemaConfig(
                                                        new ColumnConfig("A", VoltType.INTEGER),
                                                        new ColumnConfig("B", VoltType.INTEGER),
                                                        new ColumnConfig("C", VoltType.INTEGER)),
                                                new Object[][] {
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
        TableConfig XXXConfig = new TableConfig("XXX",
                                                new SchemaConfig(
                                                        new ColumnConfig("id", VoltType.INTEGER),
                                                        new ColumnConfig("name", VoltType.STRING, 32),
                                                        new ColumnConfig("data", VoltType.STRING, 1024)),
                                                new Object[][] {
                                                  { 1, "alpha", "beta" },
                                                  { 2, "gamma", "delta" }
                                                });
        //
        // This is a kind of table config which generates
        // random data.  It's better to make this kind of
        // a table when there is a lot of data, if it's possible.
        //
        TableConfig CCCConfig = new TableConfig("CCC",
                                                new SchemaConfig(
                                                        new ColumnConfig("id",   VoltType.INTEGER),
                                                        new ColumnConfig("name", VoltType.STRING,  32),
                                                        new ColumnConfig("data", VoltType.STRING,  1024)),
                                                10000000);
        DBConfig db = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   AAAConfig,
                                   BBBConfig,
                                   CCCConfig,
                                   XXXConfig);
        String sqlStmt;
        sqlStmt = "select A, B from AAA order by A, B;";

        db.addTest(new TestConfig("test_order_by",
                                  sqlStmt,
                                  new TableConfig("order_by",
                                                  new SchemaConfig(
                                                          new ColumnConfig("A", VoltType.INTEGER),
                                                          new ColumnConfig("B", VoltType.INTEGER)),
                                                  new Object[][] {
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
                                                   { 3,  30} } )));
        sqlStmt = "select AAA.A, AAA.B, BBB.C from AAA join BBB on AAA.C = BBB.C order by AAA.A, AAA.B, AAA.C;";
        db.addTest(new TestConfig("test_join",
                                  sqlStmt,
                                  new TableConfig("test_join",
                                                  new SchemaConfig(
                                                          new ColumnConfig("A", VoltType.INTEGER),
                                                          new ColumnConfig("B", VoltType.INTEGER),
                                                          new ColumnConfig("C", VoltType.INTEGER)),
                                                  new Object[][] {
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
                                                      { 3,  30,  301}} )));
        // In this case we don't care about the output
        // at all.  We just want to run the test.  This could
        // be used under gdb, where the output is long or
        // non-deterministic, or to do profiling, where we don't
        // care about specifying the output.
        sqlStmt = "select * from CCC;";
        db.addTest(new TestConfig("test_cache", sqlStmt));
        generateTests("executors", "TestGeneratedPlans", db);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static void main(String args[]) {
        EEPlanTestGenerator tg = new EEPlanTestGenerator();
        try {
            tg.setUp();
            tg.generatedPlannerTest();
        } catch (Exception e) {
            System.err.printf("Unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

}
