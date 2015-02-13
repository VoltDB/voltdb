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

package org.voltdb;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocWithOnlyComment extends AdhocDDLTestBase {

    public void testAdhocWithOnlyComment() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "-- this used to hang the server");
            }
            catch (ProcCallException pce) {
                assertTrue("wrong exception details returned",
                        pce.getMessage().contains("no SQL statement provided"));
                threw = true;
            }
            assertTrue("Adhoc with no statements should return an error", threw);

            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "/* this never hung the server, \n but test it! */");
            }
            catch (ProcCallException pce) {
                // this takes a different path because it isn't treated as a
                // comment by the AsyncCompilerAgent and makes it through to
                // the DDLCompiler which complains because it never finds a
                // semicolon-terminated statement.  Updating the error message
                // check here will be left as an unexpected exercise for
                // whoever gets stuck making the returned message consistent.
                assertTrue("wrong exception details returned",
                        pce.getMessage().contains("unexpected end of statement"));
                threw = true;
            }
            assertTrue("Adhoc with no statements should return an error", threw);

            threw = false;
            try {
                // ensure that empty string also gets the same error
                m_client.callProcedure("@AdHoc", "");
            }
            catch (ProcCallException pce) {
                assertTrue("wrong exception details returned",
                        pce.getMessage().contains("no SQL statement provided"));
                threw = true;
            }
            assertTrue("Adhoc with no statements should return an error", threw);

            threw = false;
            try {
                // ensure that just random whitespace also gets the same treatment
                m_client.callProcedure("@AdHoc", "  \n   \t    ");
            }
            catch (ProcCallException pce) {
                assertTrue("wrong exception details returned",
                        pce.getMessage().contains("no SQL statement provided"));
                threw = true;
            }
            assertTrue("Adhoc with no statements should return an error", threw);
        }
        finally {
            teardownSystem();
        }
    }
}
