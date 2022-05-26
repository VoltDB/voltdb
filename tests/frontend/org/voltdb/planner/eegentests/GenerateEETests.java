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

package org.voltdb.planner.eegentests;

import org.voltdb.catalog.Database;

/**
 * This is not a jUnit test, it's a Java application, which generates EE
 * Unit tests.  To run the application, run this Java class as a
 * java application.  All tests generated from this class use the same
 * DDL definitions.  In this class the DDL file is testplans-ee-geneators.sql,
 * set in the function setUp defined in this class.  Any other way of
 * getting a URL to a DDL definition is possible.  All the schema definitions
 * from any of the tables used in these files will be from this DDL file.
 *
 * After setting up the schema, follow the example below.  All the
 * possibilities are in the test generatedPlannerTest, where there is more
 * documentation.
 *
 * Finally, after defining member functions to generate the tests,
 * in the main() function, create a test generator object and call
 * the generator member functions, as is done in this file.
 *
 */
public class GenerateEETests extends EEPlanGenerator {
    private static final String DDL_FILENAME = "testplans-ee-generators.sql";

    public void setUp(String[] args) throws Exception {
        super.setUp(GenerateEETests.class.getResource(DDL_FILENAME),
                    "testplansgenerator",
                    true);
        processArgs(args);
    }


    //
    // It's better to make functions this way than to make
    // static constants.  The database db will often not
    // be initialized when initializing a static constant.
    //
    private TableConfig makeAAA(Database db) {
        final TableConfig AAAConfig = new TableConfig("AAA",
                                                      db,
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
        return AAAConfig;
    }

    private TableConfig makeBBB(Database db) {
        final TableConfig BBBConfig = new TableConfig("BBB",
                                                      db,
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
                                                          { 3,  30,  301} } );
             return BBBConfig;
    }

    /**
     * Tables with heterogeneous data need to have data whose
     * type is Object[][].
     *
     * @param db
     * @return
     */
    private TableConfig makeXXX(Database db) {
        final TableConfig XXXConfig = new TableConfig("XXX",
                                                      db,
                                                      new Object[][] {
                                                          { 1, "alpha", "beta" },
                                                          { 2, "gamma", "delta" } } );
        return XXXConfig;
    }

    private TableConfig makeCCC(Database db) {
        final TableConfig CCCConfig = new TableConfig("CCC",
                                                      db,
                                                      10000);
        return CCCConfig;
    }

    public void generatedInsertPolygonTest() throws Exception {
        //
        // First, get the database.  This is the database
        // which contains the catalog, with contains the
        // processed definitions from the DDL file.
        //
        Database db = getDatabase();
        //
        // This database config has no input tables.
        //
        DBConfig dbc = new DBConfig(getClass(),
                                   GenerateEETests.class.getResource(DDL_FILENAME),
                                   getCatalogString());
        // Add a test.  This test runs the insert statement.
        // We ignore the output, since this is just being
        // used to run under GDB.
        dbc.addTest(new TestConfig("test_insert_polygons",
                                  "insert into polygons values "
                                    + "("
                                    +     "100, "
                                    +     "validpolygonfromtext("
                                    +         "'POLYGON ((-102.052 41.002, -109.045 41.002, -109.045 36.999, -102.052 36.999, -102.052 41.002), (-104.035 40.24, -104.035 39.188, -105.714 39.188, -105.714 40.24, -104.035 40.24))'"
                                    +  ")"
                                    + ");",
                                  false));
        // Now, write the tests in the file executors/TestGeneratedPlans.cpp.
        generateTests("executors", "TestInsertPolygons", dbc);
    }

    public void generatedPlannerTest() throws Exception {
        //
        // First, get the database.  This is the database
        // which contains the catalog, with contains the
        // processed definitions from the DDL file.
        //
        Database db = getDatabase();
        //
        // Create some table configurations.  These are the parts of
        // the catalog we need to generate tests, cached for easy use.
        //
        // It's often more helpful to create a member function which
        // makes a table configuration, rather than creating a table
        // configuration as a static object.  This is because the
        // table configuration depends on the schema, which is in
        // the Database, and we won't have the Database until run time.
        //
        final TableConfig AAAConfig = makeAAA(db);
        final TableConfig BBBConfig = makeBBB(db);
        final TableConfig XXXConfig = makeXXX(db);
        // This is another way of making a TableConfig.  As we can see,
        // a TableConfig is given by a table name, the schema extracted from
        // the database db and some data.
        //
        final TableConfig orderByOutput = new TableConfig("test_order_by",
                                                          db,
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
        final TableConfig joinOutput = new TableConfig("test_join",
                                                       db,
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
                                                           { 3,  30,  301} } );
        //
        // This a third kind of configuration creates a table whose contents
        // are generated randomly.  It will have 10000 rows, with random
        // values of the right type for each of the columns.
        //
        final TableConfig CCCConfig = new TableConfig("CCC",
                                                      db,
                                                      10000);
        //
        // Create a DB config, which contains all the tables.
        // Note that input tables, like AAAConfig or BBBConfig,
        // and result tables, like orderByOutput or
        // joinOutput, need to be added.  Any number of tables
        // can be added.
        DBConfig dbc = new DBConfig(getClass(),
                                   GenerateEETests.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   AAAConfig,
                                   BBBConfig,
                                   CCCConfig,
                                   XXXConfig,
                                   orderByOutput,
                                   joinOutput);
        // Add a test.  This test runs the select statement
        // and expects the result to be orderByOutput.
        dbc.addTest(new TestConfig("test_order_by",
                                  "select A, B from AAA order by A, B;",
                                  false,
                                  orderByOutput));
        // Add another test.  This test runs the select
        // statement and expects the result to be joinOutput.
        dbc.addTest(new TestConfig("test_join",
                                  "select AAA.A, AAA.B, BBB.C from AAA join BBB on AAA.C = BBB.C order by AAA.A, AAA.B, AAA.C;",
                                  false,
                                  joinOutput));
        // In this case we don't care about the output
        // at all.  We just want to run the test.  This could
        // be used under gdb, where the output is long or
        // non-deterministic, or to do profiling, where we don't
        // care about specifying the output, and it will not
        // be validated at all.
        dbc.addTest(new TestConfig("test_cache", "select * from CCC;", false));
        // Now, write the tests in the file executors/TestGeneratedPlans.cpp.
        generateTests("executors", "TestGeneratedPlans", dbc);
    }

    TableConfig makeTConfig(Database db) {
        TableConfig TConfig = new TableConfig("T",
                                              db,
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
                                                  {  2,  3,    5}});
        return TConfig;
    }

    private TableConfig makeTestOutput(Database db) {
        TableConfig testOutput = new TableConfig("test_output",
                                                 db,
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
                                                     {  2,  3,    0} } );
        return testOutput;
    }

    public void generatedMaxPlan() throws Exception {
        Database db = getDatabase();
        TableConfig TConfig = makeTConfig(db);
        TableConfig testOutput = makeTestOutput(db);
        DBConfig maxDB = new DBConfig(getClass(),
                                      GenerateEETests.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig,
                                      testOutput);
        maxDB.addTest(new TestConfig("test_max_first_row",
                                     "select A, B, max(-1 * abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        maxDB.addTest(new TestConfig("test_max_middle_row",
                                     "select A, B, max(-1 * abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        maxDB.addTest(new TestConfig("test_max_last_row",
                                     "select A, B, max(-1 * abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        generateTests("executors", "TestWindowedMax", maxDB);
    }

    public void generatedMinPlan() throws Exception {
        Database db = getDatabase();
        TableConfig TConfig = makeTConfig(db);
        TableConfig testOutput = makeTestOutput(db);
        DBConfig minDB = new DBConfig(getClass(),
                                      GenerateEETests.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig,
                                      testOutput);
        minDB.addTest(new TestConfig("test_min_last_row",
                                     "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        minDB.addTest(new TestConfig("test_min_middle_row",
                                     "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        minDB.addTest(new TestConfig("test_min_first_row",
                                     "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     testOutput));

        generateTests("executors", "TestWindowedMin", minDB);
    }

    private TableConfig makeSumOutput(Database db) {
        TableConfig sumOutput = new TableConfig("test_sum_output",
                                         db,
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
                                             {  2,  3,   75} } );
        return sumOutput;
    }
    public void generatedSumPlan() throws Exception {
        Database db = getDatabase();
        TableConfig TConfig = makeTConfig(db);
        TableConfig sumOutput = makeSumOutput(db);
        DBConfig sumDB = new DBConfig(getClass(),
                                      GenerateEETests.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      TConfig,
                                      sumOutput);
        sumDB.addTest(new TestConfig("test_min_last_row",
                                     "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;",
                                     false,
                                     sumOutput));

        generateTests("executors", "TestWindowedSum", sumDB);
    }

    private TableConfig makeCountInput(Database db) {
        TableConfig countInput = new TableConfig("T",
                                                 db,
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
                                                     { 20,  3, 2203} } );
        return countInput;
    }

    private TableConfig makeCountOutput(Database db) {
        TableConfig countOutput = new TableConfig("count_output",
                                                  db,
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
        return countOutput;
    }

    public void generatedCountPlan() throws Exception {
        Database db = getDatabase();
        TableConfig countInput = makeCountInput(db);
        TableConfig countOutput = makeCountOutput(db);

        DBConfig countDB = new DBConfig(getClass(),
                                        GenerateEETests.class.getResource(DDL_FILENAME),
                                        getCatalogString(),
                                        countInput,
                                        countOutput);
        String sqlStmt;
        sqlStmt = "select A, B, C, count(*) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count_star",
                                       sqlStmt,
                                       false,
                                       countOutput));
        sqlStmt = "select A, B, C, count(A+B) over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        countDB.addTest(new TestConfig("test_count",
                                       sqlStmt,
                                       false,
                                       countOutput));
        generateTests("executors", "TestWindowedCount", countDB);
    }


    private TableConfig makeRankInput(Database db) {
        TableConfig rankInput = new TableConfig("T",
                                                db,
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
        return rankInput;
    }

    private TableConfig makeRankOutput(Database db) {
        TableConfig rankOutput = new  TableConfig("rank_output",
                                                  db,
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
        return rankOutput;
    }

    TableConfig makeRankDenseOutput(Database db)  {
        TableConfig rankDenseOutput = new TableConfig("rank_dense_output",
                                                      db,
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
        return rankDenseOutput;
    }

    TableConfig makeRowNumberOutput(Database db)  {
        TableConfig rowNumberOutput = new TableConfig("row_number_output",
                db,
                new Integer[][] {
                        // A   B    C    row_number
                        //--------------------------------------
                        {  1,  1,  101, 1},
                        {  1,  1,  102, 2},
                        //======================================
                        {  1,  2,  201, 3},
                        {  1,  2,  202, 4},
                        //======================================
                        {  1,  3,  203, 5},
                        //--------------------------------------
                        {  2,  1, 1101, 1},
                        {  2,  1, 1102, 2},
                        //======================================
                        {  2,  2, 1201, 3},
                        {  2,  2, 1202, 4},
                        //======================================
                        {  2,  3, 1203, 5},
                        //--------------------------------------
                        { 20,  1, 2101, 1},
                        { 20,  1, 2102, 2},
                        //======================================
                        { 20,  2, 2201, 3},
                        { 20,  2, 2202, 4},
                        //======================================
                        { 20,  3, 2203, 5},
                        //--------------------------------------
                });
        return rowNumberOutput;
    }

    TableConfig makeRowNumberWithoutPartitionOutput(Database db)  {
        TableConfig rowNumberOutput = new TableConfig("row_number_without_partition_output",
                db,
                new Integer[][] {
                        // A   B    C    row_number
                        //--------------------------------------
                        {  1,  1,  101, 1},
                        {  1,  1,  102, 2},
                        //======================================
                        {  1,  2,  201, 3},
                        {  1,  2,  202, 4},
                        //======================================
                        {  1,  3,  203, 5},
                        //--------------------------------------
                        {  2,  1, 1101, 6},
                        {  2,  1, 1102, 7},
                        //======================================
                        {  2,  2, 1201, 8},
                        {  2,  2, 1202, 9},
                        //======================================
                        {  2,  3, 1203, 10},
                        //--------------------------------------
                        { 20,  1, 2101, 11},
                        { 20,  1, 2102, 12},
                        //======================================
                        { 20,  2, 2201, 13},
                        { 20,  2, 2202, 14},
                        //======================================
                        { 20,  3, 2203, 15},
                        //--------------------------------------
                });
        return rowNumberOutput;
    }

    public void generatedRankPlan() throws Exception {
        Database db = getDatabase();
        TableConfig rankInput = makeRankInput(db);
        TableConfig rankOutput = makeRankOutput(db);
        TableConfig rankDenseOutput = makeRankDenseOutput(db);
        TableConfig rowNumberOutput = makeRowNumberOutput(db);
        TableConfig rowNumberWithoutPartitionOutput = makeRowNumberWithoutPartitionOutput(db);

        DBConfig rankDB = new DBConfig(getClass(),
                                       GenerateEETests.class.getResource(DDL_FILENAME),
                                       getCatalogString(),
                                       rankInput,
                                       rankOutput,
                                       rankDenseOutput,
                                       rowNumberOutput,
                                       rowNumberWithoutPartitionOutput);
        String sqlStmt;
        sqlStmt = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_rank",
                                      sqlStmt,
                                      false,
                                      rankOutput));
        sqlStmt = "select A, B, C, dense_rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        rankDB.addTest(new TestConfig("test_dense_rank",
                                      sqlStmt,
                                      false,
                                      rankDenseOutput));

        // row_number() with 'partition' and 'order by'
        sqlStmt = "select A, B, C, row_number() over (partition by A order by B) as R from T ORDER BY A;";

        rankDB.addTest(new TestConfig("test_row_number",
                sqlStmt,
                false,
                rowNumberOutput));

        // row_number() without 'order by'
        sqlStmt = "select A, B, C, row_number() over (partition by A) as R from T ORDER BY A;";

        rankDB.addTest(new TestConfig("test_row_number_without_order_by",
                sqlStmt,
                false,
                rowNumberOutput));

        // row_number() without 'partition'
        sqlStmt = "select A, B, C, row_number() over () as R from T;";

        rankDB.addTest(new TestConfig("test_row_number_without_partition",
                sqlStmt,
                false,
                rowNumberWithoutPartitionOutput));
        generateTests("executors", "TestWindowedRank", rankDB);
    }

    public void generatedStringPlan() throws Exception {
        Database db = getDatabase();
        // We test a particular possible bug in the test framework.
        // If the found values are shorter than the expected values
        // comparing the two may cause a read to non-existent memory.
        final TableConfig CCCConfig = new TableConfig("CCC",
                                                      db,
                                                      new Object[][] {
            { 100, "alpha", "beta" }
        });
        final TableConfig tooLongAnswerConfig = new TableConfig("CCCLongAns",
                                                         db,
                                                         new Object[][] {
            { 100, "alphaalpha", "betabeta" }

        });
        final TableConfig tooShortAnswerConfig = new TableConfig("CCCShortAns",
                                                         db,
                                                         new Object[][] {
            { 100, "al", "be" }

        });

        DBConfig GSDB = new DBConfig(getClass(),
                                      GenerateEETests.class.getResource(DDL_FILENAME),
                                      getCatalogString(),
                                      CCCConfig,
                                      tooShortAnswerConfig,
                                      tooLongAnswerConfig);
        GSDB.addTest(new TestConfig("test_long_string",
                                     "select * from CCC;",
                                     true,
                                     tooLongAnswerConfig));
        GSDB.addTest(new TestConfig("test_short_string",
                                     "select * from CCC;",
                                     true,
                                     tooShortAnswerConfig));
        generateTests("executors", "TestGeneratedString", GSDB);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static void main(String args[]) {
        GenerateEETests tg = new GenerateEETests();
        try {
            tg.setUp(args);
            tg.generatedPlannerTest();
            tg.generatedInsertPolygonTest();
            tg.generatedCountPlan();
            tg.generatedMinPlan();
            tg.generatedMaxPlan();
            tg.generatedSumPlan();
            tg.generatedRankPlan();
            tg.generatedStringPlan();
        } catch (Exception e) {
            System.err.printf("Unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }

}
