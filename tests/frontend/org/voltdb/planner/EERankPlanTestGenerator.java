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
 */package org.voltdb.planner;

public class EERankPlanTestGenerator extends PlannerTestCase {
    private static final String DDL_FILENAME = "testrankplan-eegenerator.sql";
    @Override
    protected void setUp() throws Exception {

        setupSchema(EERankPlanTestGenerator.class.getResource(DDL_FILENAME),
                    "testrankplan",
                    false);
    }


    public void testGeneratedPlan() throws Exception {
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
        DBConfig db = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   TConfig);
        String sqlStmt;
        sqlStmt = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";

        db.addTest(new TestConfig("test_rank",
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

        db.addTest(new TestConfig("test_dense_rank",
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
        generateTests("executors", "TestRank", db);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static void main(String args[]) {
        org.junit.runner.JUnitCore.runClasses(EEPlanTestGenerator.class);
    }

}
