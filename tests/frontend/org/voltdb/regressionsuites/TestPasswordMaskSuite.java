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

import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;

public class TestPasswordMaskSuite extends RegressionSuite {
    public TestPasswordMaskSuite(String name) {
        super(name);
    }

    public void testClientLogin() {
        this.m_username = "admin";
        this.m_password = "admin";
        boolean exceptionThrown = false;
        try {
            getClient();
        } catch (IOException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertFalse(exceptionThrown);

        this.m_username = "admin";
        this.m_password = "wrongpassword";
        exceptionThrown = false;
        try {
            getClient();
        } catch (IOException e) {
            e.printStackTrace();
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }

    /**
     * Build a list of the tests that will be run when TestSecuritySuite gets run by JUnit.
     * Use helper classes that are part of the RegressionSuite framework.
     * This particular class runs all tests on the the local JNI backend with both
     * one and two partition configurations, as well as on the hsql backend.
     *
     * @return The TestSuite containing all the tests to be run.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPasswordMaskSuite.class);

        // build up a project builder for the workload
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();

        UserInfo users[] = new UserInfo[] {
                new UserInfo("admin", "D033E22AE348AEB5660FC2140AEC35850C4DA9978C6976E5B5410415BDE908BD4DEE15DFB167A9C873FC4BB8A81F6F2AB448A918", new String[] {"administrator"}, false)
        };
        project.addUsers(users);

        // suite defines its own ADMINISTRATOR user
        project.setSecurityEnabled(true, false);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with one sites/partitions
        config = new LocalCluster("passwordmask-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build the jarfile
        if (!config.compile(project)) fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        // Not testing a cluster and assuming security shouldn't be affected by this

        return builder;
    }
}
