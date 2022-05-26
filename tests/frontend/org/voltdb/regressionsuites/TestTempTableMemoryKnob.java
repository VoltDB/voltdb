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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.failureprocs.FetchTooMuch;
import org.voltdb_testprocs.regressionsuites.failureprocs.InsertLotsOfData;

import junit.framework.Test;

public class TestTempTableMemoryKnob extends RegressionSuite {

    /**
     * Constructor needed for JUnit. Should just pass on parameters to superclass.
     * @param name The name of the method to test. This is just passed to the superclass.
     */
    public TestTempTableMemoryKnob(String name) {
        super(name);
    }


    // This is basically the same test as the testPerPlanFragmentMemoryOverload,
    // but in this case we bump up the temp table limit so that it doesn't
    // throw a proc call exception.
    public void testPerPlanFragmentMemoryKnob() throws IOException, ProcCallException {
        if (isHSQL() || isValgrind()) return;

        System.out.println("STARTING testPerPlanFragmentMemoryKnob");
        Client client = getClient();

        VoltTable[] results = null;

        int nextId = 0;

        for (int mb = 0; mb < 300; mb += 5) {
            results = client.callProcedure("InsertLotsOfData", 0, nextId).getResults();
            assertEquals(1, results.length);
            assertTrue(nextId < results[0].asScalarLong());
            nextId = (int) results[0].asScalarLong();
            System.err.println("Inserted " + (mb + 5) + "mb");
        }

        boolean threw = false;
        try
        {
            results = client.callProcedure("FetchTooMuch", 0).getResults();
            assertEquals(1, results.length);
            System.out.println("Fetched the 300 megabytes");
        }
        catch (ProcCallException e)
        {
            e.printStackTrace();
            threw = true;
        }
        assertFalse("Should have successfully completed a select with 300MB temp table but didn't",
                    threw);
    }

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTempTableMemoryKnob.class);

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 1 Local Site/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////

        // get a server config for the native backend with two sites/partitions
        VoltServerConfig config = new LocalCluster("tempknob-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(FetchTooMuch.class.getResource("failures-ddl.sql"));
        project.addProcedure(InsertLotsOfData.class);
        project.addProcedure(FetchTooMuch.class);

        // Give ourselves a little leeway for slop over 300 MB
        project.setMaxTempTableMemory(320);
        // build the jarfile
        if (!config.compile(project))
            fail();

        // add this config to the set of tests to run
        builder.addServerConfig(config);

        return builder;
    }
}
