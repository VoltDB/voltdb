/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestCanonicalDDLThroughSQLcmd extends AdhocDDLTestBase {

    String firstCanonicalDDL = null;

    public String getFirstCanonicalDDL() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("fullDDL.jar");

        VoltCompiler compiler = new VoltCompiler();
        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("fullDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success = compiler.compileFromDDL(pathToCatalog, pathToSchema);
        assertTrue(success);
        return compiler.getCanonicalDDL();
    }

    public void secondCanonicalDDLFromAdhoc() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltCompiler compiler = new VoltCompiler();
        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.setUseAdhocSchema(true);
        boolean success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        m_client.callProcedure("@AdHoc", firstCanonicalDDL);

        assertEquals(compiler.getCanonicalDDL(), firstCanonicalDDL);

        teardownSystem();
    }

    public void secondCanonicalDDLFromSQLcmd() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        builder.setUseAdhocSchema(true);
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
        assertTrue(callSQLcmd(firstCanonicalDDL));
        roundtripDDL = getDDLFromHTTP(httpdPort);
        // IZZY: we force single statement SQL keywords to lower case, it seems
        assertTrue(firstCanonicalDDL.equalsIgnoreCase(roundtripDDL));

        assertTrue(callSQLcmd("CREATE TABLE NONSENSE (id INTEGER);\n"));
        roundtripDDL = getDDLFromHTTP(httpdPort);
        assertFalse(firstCanonicalDDL.equals(roundtripDDL));

        teardownSystem();
    }

    public boolean callSQLcmd(String ddl) throws Exception {
        File f = new File("ddl.sql");
        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
        fos.write(ddl.getBytes());
        fos.close();

        File out = new File("out.log");

        File error = new File("error.log");

        ProcessBuilder pb = new ProcessBuilder("bin/sqlcmd");
        pb.redirectInput(f);
        pb.redirectOutput(out);
        pb.redirectError(error);
        Process process = pb.start();

        // Set timeout to 1 minute
        long starttime = System.currentTimeMillis();
        long endtime = starttime + 60000;

        int exitValue = -1;
        while(System.currentTimeMillis() < endtime) {
            Thread.sleep(1000);
            try{
                exitValue = process.exitValue();
                if(exitValue == 0) {
                    break;
                }
            }
            catch (Exception e) {
                System.out.println("Process hasn't exited");
            }
        }

        return (exitValue == 0);
    }

    public String getDDLFromHTTP(int httpdPort) throws Exception {
        URL ddlURL = new URL(String.format("http://localhost:%d/ddl/", httpdPort));

        HttpURLConnection conn = (HttpURLConnection) ddlURL.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.connect();

        BufferedReader in = null;
        try {
            if(conn.getInputStream() != null){
                in = new BufferedReader(
                        new InputStreamReader(
                        conn.getInputStream(), "UTF-8"));
            }
        } catch(IOException e){
            e.printStackTrace();
        }
        if(in == null) {
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
        firstCanonicalDDL = getFirstCanonicalDDL();

        secondCanonicalDDLFromAdhoc();
        secondCanonicalDDLFromSQLcmd();
    }
}
