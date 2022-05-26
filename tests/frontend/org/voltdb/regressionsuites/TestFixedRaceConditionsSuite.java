/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * This regression suite test is intended to have regression tests
 * for bugs caused by race conditions in the EE, which require a
 * larger number of sites per host to reproduce.
 *
 * Sites per host is set to 8 for this test.
 */
public class TestFixedRaceConditionsSuite extends RegressionSuite {

    public void testEng13725IndexScanCount() throws Exception {
        Client client = getClient();

        // Since this was a race condition, run it several times to make sure it
        // doesn't fail.
        for (int i = 0; i < 100; ++i) {
            assertSuccessfulDML(client, "INSERT INTO ENG_13725_T2 VALUES (x'66');");
            assertSuccessfulDML(client, "INSERT INTO ENG_13725_T1 (ID) VALUES (-177);");

            // In this bug, the following update was not thread safe due to the index scan count.
            assertSuccessfulDML(client, "UPDATE ENG_13725_T1 " +
                    "SET ID = (SELECT COUNT(*) FROM ENG_13725_T2 WHERE ENG_13725_T1.VARBIN = VARBIN);");

            // The following statement crashed the server due to index corruption
            assertSuccessfulDML(client, "DELETE FROM ENG_13725_T2;");

            // Reset state for next iteration of test
            assertSuccessfulDML(client, "DELETE FROM ENG_13725_T1;");
        }
    }

    public TestFixedRaceConditionsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestFixedRaceConditionsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestFixedRaceConditionsSuite.class.getResource("fixed-race-conditions-ddl.sql"));

        // CONFIG #1: Single-host cluster with 8 sites per host
        config = new LocalCluster("fixedraces-eightsite.jar", 8, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL
        config = new LocalCluster("fixedraces-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
