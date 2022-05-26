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

/*
 * This test is used for testing round trip DDL through Adhoc and SQLcmd.
 * We first build a catalog and pull the canonical DDL from it.
 * Then we feed this DDL to a bare server through Adhoc/SQLcmd,
 * pull the canonical DDL again, and check whether it remains the same.
 */

package org.voltdb.canonicalddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;
import java.net.URLDecoder;


import org.junit.Test;
import org.voltcore.utils.PortGenerator;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.fullddlfeatures.TestDDLFeatures;
import org.voltdb.utils.MiscUtils;

public class TestCanonicalDDLThroughSQLcmd extends AdhocDDLTestBase {

    private String firstCanonicalDDL = null;
    private boolean triedSqlcmdDryRun = false;

    private String getFirstCanonicalDDL() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("fullDDL.jar");

        VoltCompiler compiler = new VoltCompiler(false);
        final URL url = TestDDLFeatures.class.getResource("fullDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success = compiler.compileFromDDL(pathToCatalog, pathToSchema);
        assertTrue(success);
        return compiler.getCanonicalDDL();
    }

    private void secondCanonicalDDLFromAdhoc() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltCompiler compiler = new VoltCompiler(false);
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        m_client.callProcedure("@AdHoc", firstCanonicalDDL);

        // First line of canonical DDL differs thanks to creation time.  Avoid
        // it in the comparison
        // Sanity check that we're not trimming the entire fullddl.sql file away
        assertTrue(firstCanonicalDDL.indexOf('\n') < 100);
        String secondDDL = compiler.getCanonicalDDL();
        assertEquals(firstCanonicalDDL.substring(firstCanonicalDDL.indexOf('\n')),
                secondDDL.substring(secondDDL.indexOf('\n')));

        teardownSystem();
    }

    private void secondCanonicalDDLFromSQLcmd(boolean fastModeDDL) throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.setUseDDLSchema(true);
        PortGenerator pg = new PortGenerator();
        int httpdPort = pg.next();
        builder.setHTTPDPort(httpdPort);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        String roundtripDDL;

        assert(firstCanonicalDDL != null);

        if ( ! triedSqlcmdDryRun) {
            assertEquals("sqlcmd dry run failed -- maybe some sqlcmd component (the voltdb jar file?) needs to be rebuilt.",
                    0, callSQLcmd("\n", fastModeDDL));
            triedSqlcmdDryRun = true;
        }

        assertEquals("sqlcmd failed on input:\n" + firstCanonicalDDL, 0, callSQLcmd(firstCanonicalDDL, fastModeDDL));
        // IZZY: we force single statement SQL keywords to lower case, it seems
        // Sanity check that we're not trimming the entire fullddl.sql file away
        assertTrue(firstCanonicalDDL.indexOf('\n') < 100);

        assertEquals("sqlcmd failed on last call", 0, callSQLcmd("CREATE TABLE NONSENSE (id INTEGER);\n", fastModeDDL));
        assertTrue(firstCanonicalDDL.indexOf('\n') < 100);

        teardownSystem();
    }

    private int callSQLcmd(String ddl, boolean fastModeDDL) throws Exception {
        String commandPath = "bin/sqlcmd";
        final long timeout = 300000; // 300,000 millis -- give up after 5 minutes of trying.

        File f = new File("ddl.sql");
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(ddl.getBytes());
        fos.close();

        File out = new File("out.log");
        File error = new File("error.log");

        ProcessBuilder pb = null;
        if (fastModeDDL) {
            pb = new ProcessBuilder(commandPath, "--ddl-file=" + f.getPath());
        } else {
            pb = new ProcessBuilder(commandPath);
            pb.redirectInput(f);
        }

        pb.redirectOutput(out);
        pb.redirectError(error);
        Process process = pb.start();

        // Set timeout to 1 minute
        long starttime = System.currentTimeMillis();
        long elapsedtime = 0;
        long pollcount = 0;
        do {
            Thread.sleep(1000);
            try {
                int exitValue = process.exitValue();
                // Only verbosely report the successful exit after verbosely reporting a delay.
                // Frequent false alarms might lead to raising the sleep time.
                if (pollcount > 0) {
                    elapsedtime = System.currentTimeMillis() - starttime;
                    System.err.println("External process (" + commandPath + ") exited after being polled " +
                            pollcount + " times over " + elapsedtime + "ms");
                }

                // Debug the SQLCMD output if needed
                // System.out.println(new Scanner(out).useDelimiter("\\Z").next());

                return exitValue;
            }
            catch (IllegalThreadStateException notYetDone) {
                elapsedtime = System.currentTimeMillis() - starttime;
                ++pollcount;
                System.err.println("External process (" + commandPath + ") has not yet exited after " + elapsedtime + "ms");
                continue;
            }
            catch (Exception e) {
                elapsedtime = System.currentTimeMillis() - starttime;
                ++pollcount;
                System.err.println("External process (" + commandPath + ") has not yet exited after " + elapsedtime + "ms");
            }
        } while (elapsedtime < timeout);

        fail("External process (" + commandPath + ") timed out after " + elapsedtime + "ms on input:\n" + ddl);
        return -1;
    }

    @Test
    public void testCanonicalDDLRoundtrip() throws Exception {
        firstCanonicalDDL = getFirstCanonicalDDL();
        long starttime = System.currentTimeMillis();
        secondCanonicalDDLFromAdhoc();
        long adHocTime = System.currentTimeMillis() - starttime;

        starttime = System.currentTimeMillis();
        secondCanonicalDDLFromSQLcmd(false);
        long sqlcmdTime = System.currentTimeMillis() - starttime;

        starttime = System.currentTimeMillis();
        secondCanonicalDDLFromSQLcmd(true);
        long sqlcmdTimeDDLmode = System.currentTimeMillis() - starttime;

        System.out.println(String.format("AdHoc elapsed %d ms", adHocTime));
        System.out.println(String.format("SQLcmd elapsed %d ms", sqlcmdTime));
        System.out.println(String.format("SQLcmd fast DDL mode elapsed %d ms", sqlcmdTimeDDLmode));
    }
}
