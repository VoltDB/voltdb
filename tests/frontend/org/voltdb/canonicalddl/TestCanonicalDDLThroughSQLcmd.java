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

/*
 * This test is used for testing round trip DDL through Adhoc and SQLcmd.
 * We first build a catalog and pull the canonical DDL from it.
 * Then we feed this DDL to a bare server through Adhoc/SQLcmd,
 * pull the canonical DDL again, and check whether it remains the same.
 */

package org.voltdb.canonicalddl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;

import org.junit.Test;
import org.voltcore.utils.PortGenerator;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.fullddlfeatures.TestDDLFeatures;

public class TestCanonicalDDLThroughSQLcmd extends AdhocDDLTestBase {
    private String m_firstCanonicalDDL = null;
    private boolean m_triedSqlcmdDryRun = false;

    private String getFirstCanonicalDDL() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("fullDDL.jar");

        VoltCompiler compiler = new VoltCompiler();
        final URL url = TestDDLFeatures.class.getResource("fullDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success = compiler.compileFromDDL(pathToCatalog, pathToSchema);
        assertTrue(success);
        return compiler.getCanonicalDDL();
    }

    private void secondCanonicalDDLFromAdhoc() throws Exception {
        DeploymentBuilder db = new DeploymentBuilder()
        .setUseAdHocDDL(true);
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), "", db);
        assertNotNull("Configuration failed to compile", config);
        startSystem(config);

        m_client.callProcedure("@AdHoc", m_firstCanonicalDDL);

        // First line of canonical DDL differs thanks to creation time.  Avoid
        // it in the comparison
        // Sanity check that we're not trimming the entire fullddl.sql file away
        assertTrue(m_firstCanonicalDDL.indexOf('\n') < 100);
        ////FIXME: find a way to extract the canonical DDL from an ad-hocced schema.
        ////String secondDDL = CatalogUtil.getCanonicalDDLFromJar(config);
        ////assertEquals(m_firstCanonicalDDL.substring(m_firstCanonicalDDL.indexOf('\n')),
        ////        secondDDL.substring(secondDDL.indexOf('\n')));

        teardownSystem();
    }

    private void secondCanonicalDDLFromSQLcmd(boolean fastModeDDL) throws Exception {
        PortGenerator pg = new PortGenerator();
        int httpdPort = pg.next();
        DeploymentBuilder db = new DeploymentBuilder()
        .setUseAdHocDDL(true)
        .setHTTPDPort(httpdPort)
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), "", db);
        startSystem(config);

        String roundtripDDL;

        assert(m_firstCanonicalDDL != null);

        if ( ! m_triedSqlcmdDryRun) {
            assertEquals("sqlcmd dry run failed -- maybe some sqlcmd component (the voltdb jar file?) needs to be rebuilt.",
                    0, callSQLcmd("\n", fastModeDDL));
            m_triedSqlcmdDryRun = true;
        }

        assertEquals("sqlcmd failed on input:\n" + m_firstCanonicalDDL, 0, callSQLcmd(m_firstCanonicalDDL, fastModeDDL));
        roundtripDDL = getDDLFromHTTP(httpdPort);
        // IZZY: we force single statement SQL keywords to lower case, it seems
        // Sanity check that we're not trimming the entire fullddl.sql file away
        assertTrue(m_firstCanonicalDDL.indexOf('\n') < 100);
        assertEquals(m_firstCanonicalDDL.substring(m_firstCanonicalDDL.indexOf('\n')).toLowerCase(),
                roundtripDDL.substring(roundtripDDL.indexOf('\n')).toLowerCase());

        assertEquals("sqlcmd failed on last call", 0, callSQLcmd("CREATE TABLE NONSENSE (id INTEGER);\n", fastModeDDL));
        roundtripDDL = getDDLFromHTTP(httpdPort);
        assertTrue(m_firstCanonicalDDL.indexOf('\n') < 100);
        assertFalse(m_firstCanonicalDDL.substring(m_firstCanonicalDDL.indexOf('\n')).toLowerCase().equals(
                roundtripDDL.substring(roundtripDDL.indexOf('\n')).toLowerCase()));

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
            catch (Exception e) {
                elapsedtime = System.currentTimeMillis() - starttime;
                ++pollcount;
                System.err.println("External process (" + commandPath + ") has not yet exited after " + elapsedtime + "ms");
            }
        } while (elapsedtime < timeout);

        fail("External process (" + commandPath + ") timed out after " + elapsedtime + "ms on input:\n" + ddl);
        return -1;
    }

    private String getDDLFromHTTP(int httpdPort) throws Exception {
        URL ddlURL = new URL(String.format("http://localhost:%d/ddl/", httpdPort));

        HttpURLConnection conn = (HttpURLConnection) ddlURL.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.connect();

        BufferedReader in = null;
        try {
            if (conn.getInputStream() != null) {
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getInputStream(), "UTF-8"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (in == null) {
            throw new Exception("Unable to read response from server");
        }

        String line;
        StringBuffer sb = new StringBuffer();
        while ((line = in.readLine()) != null) {
            sb.append(line + "\n");
        }

        return sb.toString();
    }

    @Test
    public void testCanonicalDDLRoundtrip() throws Exception {
        m_firstCanonicalDDL = getFirstCanonicalDDL();
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
