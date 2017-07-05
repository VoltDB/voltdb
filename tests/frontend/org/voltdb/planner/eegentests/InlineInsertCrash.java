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
 */package org.voltdb.planner.eegentests;

import org.voltdb.catalog.Database;

public class InlineInsertCrash extends EEPlanGenerator {
    private static final String DDL_FILENAME = "testplans-ee-inlineinsertcrash.sql";

    @Override
    public void setUp() throws Exception {
        super.setUp(InlineInsertCrash.class.getResource(DDL_FILENAME),
                    "testplansgenerator",
                    true);
    }

    private TableConfig makeR7(Database db) {
        final TableConfig R7Config = new TableConfig("R7",
                                                      db,
                                                      new Object[][] {
                                                        { "alpha", "beta", "delta", "gamma" }});
        return R7Config;
    }

    private TableConfig makeDML(Database db, String DMLName, int numRows) {
        final TableConfig DMLConfig = new TableConfig(DMLName,
                                                      db,
                                                      new Object[][] {
                                                        {  numRows }});
        return DMLConfig;
    }

    public void generatedInlineInsertCrash() throws Exception {
        Database db = getDatabase();
        TableConfig R7Config = makeR7(db);
        TableConfig testOutput = makeDML(db, "DML", 1);
        DBConfig inlineInsertCrashDB = new DBConfig(getClass(),
                                                    GenerateEETests.class.getResource(DDL_FILENAME),
                                                    getCatalogString(),
                                                    R7Config,
                                                    testOutput);
        inlineInsertCrashDB.addTest(new TestConfig("inline_insert_crash",
                                                   "INSERT INTO P4  SELECT   *  FROM R7  T3     ORDER BY VCHAR_INLINE",
                                                   false,
                                                   testOutput).setPlanFragment(1));
        generateTests("executors", "TestInlineInsertCrash", inlineInsertCrashDB);
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public static void main(String args[]) {
        InlineInsertCrash tg = new InlineInsertCrash();
        tg.processArgs(args);
        try {
            tg.setUp();
            tg.generatedInlineInsertCrash();
        } catch (Exception e) {
            System.err.printf("Unexpected exception: %s\n", e.getMessage());
            e.printStackTrace();
        }
    }
}
