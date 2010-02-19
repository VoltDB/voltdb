/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb;

import java.io.IOException;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.ProcedureProfiler.Level;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;

import junit.framework.TestCase;

public class TestAdHocQueries extends TestCase {

    public void testSimple() throws InterruptedException, IOException, ProcCallException {
        Configuration config = new Configuration();
        config.m_backend = BackendTarget.NATIVE_EE_JNI;
        config.m_noLoadLibVOLTDB = false;
        config.setPathToCatalogForTest("tpcc.jar");
        config.m_profilingLevel = Level.DISABLED;

        TPCCProjectBuilder project = new TPCCProjectBuilder();
        project.addDefaultSchema();
        project.addDefaultPartitioning();
        project.addProcedures(org.voltdb.compiler.procedures.EmptyProcedure.class);
        assertTrue(project.compile(config.m_pathToCatalog));

        ServerThread server = new ServerThread(config);
        server.start();
        server.waitForInitialization();

        // do the test
        Client client = ClientFactory.createClient();
        client.createConnection("localhost", "program", "password");

        VoltTable modCount = client.callProcedure("@AdHoc", "INSERT INTO NEW_ORDER VALUES (1, 1, 1);")[0];
        assertTrue(modCount.getRowCount() == 1);
        assertTrue(modCount.asScalarLong() == 1);


        VoltTable result = client.callProcedure("@AdHoc", "SELECT * FROM NEW_ORDER;")[0];
        assertTrue(result.getRowCount() == 1);
        System.out.println(result.toString());

        boolean caught = false;
        try
        {
            client.callProcedure("@AdHoc", "SLEECT * FROOM NEEEW_OOORDERERER;");
        }
        catch (Exception e)
        {
            caught = true;
        }
        assertTrue("Bad SQL failed to throw expected exception", caught);

        server.shutdown();
        server.join();
        assertTrue(true);
    }

}
