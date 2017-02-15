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
 * Follow the example below.  All the possibilities are in the test
 * generatedPlannerTest.
 *
 * @author bwhite
 *
 */
public class EEPlanTestGenerator extends EEPlanGenerator {
    private static final String DDL_FILENAME = "testplans-ee-generators.sql";

    @Override
    protected void setUp() throws Exception {
        setupSchema(EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                    "testplanseegenerator",
                    false);
    }


    /*
     * Define some schemas for use later on.
     */
    public static SchemaConfig ABIntSchema = new SchemaConfig(new ColumnConfig("A", VoltType.INTEGER),
                                                              new ColumnConfig("B", VoltType.INTEGER));

    public static SchemaConfig ABCIntSchema = new SchemaConfig(new ColumnConfig("A", VoltType.INTEGER),
                                                               new ColumnConfig("B", VoltType.INTEGER),
                                                               new ColumnConfig("C", VoltType.INTEGER));

    public static SchemaConfig ABCDIntSchema = new SchemaConfig(new ColumnConfig("A", VoltType.INTEGER),
                                                                new ColumnConfig("B", VoltType.INTEGER),
                                                                new ColumnConfig("C", VoltType.INTEGER),
                                                                new ColumnConfig("D", VoltType.INTEGER));

    public static SchemaConfig NameDataSchema = new SchemaConfig(new ColumnConfig("id", VoltType.INTEGER),
                                                                 new ColumnConfig("name", VoltType.STRING, 32),
                                                                 new ColumnConfig("data", VoltType.STRING, 1024));

    public final static TableConfig AAAConfig = new TableConfig("AAA",
                                                                ABCIntSchema,
                                                                new Integer[][] {
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
     public final static TableConfig BBBConfig = new TableConfig("BBB",
                                                                 ABCIntSchema,
                                                                 new Integer[][] {
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
    // Tables with heterogeneous data need to have data whose
    // type is Object[][].
    public final static TableConfig XXXConfig = new TableConfig("XXX",
                                                                NameDataSchema,
                                                                new Object[][] {
                                                                    { 1, "alpha", "beta" },
                                                                    { 2, "gamma", "delta" }
                                                                });
    //
    // This is a kind of table config which generates
    // random data.  It's better to make this kind of
    // a table when there is a lot of data, if it's possible.
    //
    public final static TableConfig CCCConfig = new TableConfig("CCC",
                                                                NameDataSchema,
                                                                10000000);
    public final static TableConfig orderByOutput = new TableConfig("order_by",
                                                                    ABIntSchema,
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
                                                                        { 3,  30} } );
    public final static TableConfig joinOutput = new TableConfig("test_join",
                                                     ABCIntSchema,
                                                     new Integer[][] {
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
                                                         { 3,  30,  301}} );

    public void generatedPlannerTest() throws Exception {
        // Create a DB config, which contains all the tables.
        // Note that result tables, like orderByOutput or
        // joinOutput don't need to be added.  Only the
        // tables we expect a query to use needs to be added here.
        DBConfig db = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   AAAConfig,
                                   BBBConfig,
                                   CCCConfig,
                                   XXXConfig);
        // Add a test.  This test runs the select statement
        // and expects the result to be orderByOutput.
        db.addTest(new TestConfig("test_order_by",
                                  "select A, B from AAA order by A, B;",
                                  orderByOutput));
        db.addTest(new TestConfig("test_join",
                                  "select AAA.A, AAA.B, BBB.C from AAA join BBB on AAA.C = BBB.C order by AAA.A, AAA.B, AAA.C;",
                                  joinOutput));
        // In this case we don't care about the output
        // at all.  We just want to run the test.  This could
        // be used under gdb, where the output is long or
        // non-deterministic, or to do profiling, where we don't
        // care about specifying the output, and it will not
        // be validated at all.
        db.addTest(new TestConfig("test_cache", "select * from CCC;"));
        // Now, write the tests in the file executors/TestGeneratedPlans.cpp.
        generateTests("executors", "TestGeneratedPlans", db);
    }

    public static final TableConfig TConfig = new TableConfig("T",
                                                              ABCIntSchema,
                                                              new Integer[][] {
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

        public static final TableConfig testOutput = new TableConfig("test_output",
                                                                     ABCIntSchema,
                                                                     new Integer[][] {
                                                                         {  1,  1,    0},
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
                                                                         {  2,  3,    0}});

    public void generatedMaxPlan() throws Exception {

        DBConfig maxDB = new DBConfig(getClass(),
                                      EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig);
        maxDB.addTest(new TestConfig("test_max_first_row",
                                     "select A, B, max(-1 * abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        maxDB.addTest(new TestConfig("test_max_middle_row",
                                     "select A, B, max(-1 * abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        maxDB.addTest(new TestConfig("test_max_last_row",
                                     "select A, B, max(-1 * abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        generateTests("executors", "TestWindowedMax", maxDB);
    }

    public void generatedMinPlan() throws Exception {
        DBConfig minDB = new DBConfig(getClass(),
                                      EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig);

        minDB.addTest(new TestConfig("test_min_last_row",
                                     "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        minDB.addTest(new TestConfig("test_min_middle_row",
                                     "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        minDB.addTest(new TestConfig("test_min_first_row",
                                     "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     testOutput));

        generateTests("executors", "TestWindowedMin", minDB);
    }

    public final static TableConfig sumOutput = new TableConfig("test_sum_output",
                                                                ABCIntSchema,
                                                                new Integer[][] {
                                                                    {  1,  1,   20},
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
                                                                    {  2,  3,   75}});

    public void generatedSumPlan() throws Exception {
        DBConfig sumDB = new DBConfig(getClass(),
                                      EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig);
        sumDB.addTest(new TestConfig("test_min_last_row",
                                     "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     sumOutput));

        generateTests("executors", "TestWindowedSum", sumDB);
    }

    public static final TableConfig countInput = new TableConfig("T",
                                                                 ABCIntSchema,
                                                                 new Integer[][] {
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
                                                                     { 20,  3, 2203}});

    public static final TableConfig countOutput = new TableConfig("count_output",
                                                                  ABCDIntSchema,
                                                                  new Integer[][] {
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
                                                                      { 20,  3, 2203, 5}});

    public void generatedCountPlan() throws Exception {
        DBConfig countDB = new DBConfig(getClass(),
                                        EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                        getCatalogString(),
                                        countInput,
                                        countOutput);
        String sqlStmt;
        sqlStmt = "select A, B, C, count(*) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count_star",
                                       sqlStmt,
                                       countOutput));
        sqlStmt = "select A, B, C, count(A+B) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count",
                                       sqlStmt,
                                       countOutput));
        generateTests("executors", "TestWindowedCount", countDB);
    }


    public final static TableConfig rankInput = new TableConfig("T",
                                                                ABCIntSchema,
                                                                new Integer[][] {
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
                                                                    { 20,  3, 2203}});
    public final static TableConfig rankOutput = new TableConfig("rank_dense_output",
                                                             ABCDIntSchema,
                                                             new Integer[][] {
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
                                                             });
    public final static TableConfig rankDenseOutput = new TableConfig("rank_dense_output",
                                                                  ABCDIntSchema,
                                                                  new Integer[][] {
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
                                                                  });
    public void generatedRankPlan() throws Exception {
        DBConfig rankDB = new DBConfig(getClass(),
                                       EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                       getCatalogString(),
                                       rankInput);
        String sqlStmt;
        sqlStmt = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_rank",
                                      sqlStmt,
                                      rankOutput));
        sqlStmt = "select A, B, C, dense_rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_dense_rank",
                                      sqlStmt,
                                      rankDenseOutput));
        generateTests("executors", "TestWindowedRank", rankDB);
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
            tg.generatedCountPlan();
            tg.generatedMinPlan();
            tg.generatedMaxPlan();
            tg.generatedSumPlan();
            tg.generatedRankPlan();
        } catch (Exception e) {
            System.err.printf("Unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

}
