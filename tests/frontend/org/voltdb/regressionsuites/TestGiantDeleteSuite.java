/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;
import org.voltdb_testprocs.regressionsuites.fixedsql.InsertBatch;


public class TestGiantDeleteSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { InsertBatch.class };

    public void testGiantDelete() throws IOException, ProcCallException
    {
        /*
         * Times out with valgrind
         */
        if (isValgrind()) {
            return;
        }
        Client client = getClient(1000 * 60 * 10);
        for (int i = 0; i < 100; i++) {
            client.callProcedure("InsertBatch", 200000, 0, i * 200000);
        }
        try {
            client.callProcedure("Delete");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Expected to be able to delete large batch but failed");
        }
        // Test repeatability
        for (int i = 0; i < 100; i++) {
            client.callProcedure("InsertBatch", 200000, 0, i * 200000);
        }
        try {
            client.callProcedure("Delete");
        }
        catch (ProcCallException pce) {
            pce.printStackTrace();
            fail("Expected to be able to delete large batch but failed");
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestGiantDeleteSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGiantDeleteSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("giant-delete-ddl.sql"));
        project.addProcedures(PROCEDURES);
        project.addStmtProcedure("Delete", "DELETE FROM ASSET WHERE ASSET_ID > -1;");

        config = new LocalCluster("giantdelete-onesite.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);

        return builder;
    }

}
