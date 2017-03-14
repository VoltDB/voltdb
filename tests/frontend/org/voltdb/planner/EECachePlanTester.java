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
 *//* This file is part of VoltDB.
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

import org.voltdb.VoltType;
import org.voltdb.planner.eegentests.EEPlanGenerator;
import org.voltdb.planner.eegentests.EEPlanTestGenerator;

public class EECachePlanTester extends EEPlanGenerator {
    private static final String DDL_FILENAME = "testplans-ee-generators.sql";
    private static final int    NUMBER_ROWS  = 10000000;

    @Override
    protected void setUp() throws Exception {
        setupSchema(EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                    "testplanseegenerator",
                    false);
    }


    public void generatedPlannerTest() throws Exception {
        //
        // This is a kind of table config which generates
        // random data.  It's better to make this kind of
        // a table when there is a lot of data, if it's possible.
        //
        TableConfig CCCConfig = new TableConfig("CCC",
                                                getDatabase(),
                                                NUMBER_ROWS);
        DBConfig db = new DBConfig(getClass(),
                                   EEPlanTestGenerator.class.getResource(DDL_FILENAME),
                                   getCatalogString(),
                                   CCCConfig);
        String sqlStmt;
        sqlStmt = "select A, B from AAA order by A, B;";

        // In this case we don't care about the output
        // at all.  We just want to run the test.  This could
        // be used under gdb, where the output is long or
        // non-deterministic, or to do profiling, where we don't
        // care about specifying the output.
        sqlStmt = "select * from CCC;";
        db.addTest(new TestConfig("test_cache", sqlStmt));
        generateTests("executors", "TestCacheBehavior", db);
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
