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

package org.voltdb.canonicalddl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;

import org.junit.Test;
import org.voltdb.AdhocDDLTestBase;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestCanonicalDDLThroughSQLcmd extends AdhocDDLTestBase
{
    String firstCanonicalDDL = null;
    String secondCanonicalDDLFromAdhoc = null;
    String secondCanonicalDDLFromSQLcmd = null;

    public String getFirstCanonicalDDL() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("fullDDL.jar");

        VoltCompiler compiler = new VoltCompiler();
        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("fullDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success = compiler.compileFromDDL(pathToCatalog, pathToSchema);
        assertTrue(success);
        return compiler.getCanonicalDDL();
    }

    public String getSecondCanonicalDDLFromAdhoc() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltCompiler compiler = new VoltCompiler();
        VoltProjectBuilder builder = new VoltProjectBuilder();

        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("emptyDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success;

        // Use VoltProjectBuilder to write catalog and deployment.xml
        builder.setUseAdhocSchema(true);
        builder.addSchema(pathToSchema);
        success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        m_client.callProcedure("@AdHoc", firstCanonicalDDL);

        teardownSystem();
        return compiler.getCanonicalDDL();
    }

    public void getSecondCanonicalDDLFromSQLcmd() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("emptyDDL.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("emptyDDL.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();

        final URL url = TestCanonicalDDLThroughSQLcmd.class.getResource("emptyDDL.sql");
        String pathToSchema = URLDecoder.decode(url.getPath(), "UTF-8");
        boolean success;

        // Use VoltProjectBuilder to write catalog and deployment.xml
        builder.setUseAdhocSchema(true);
        builder.addLiteralSchema("--nothing");
        builder.setHTTPDPort(8080);
        success = builder.compile(pathToCatalog);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        startSystem(config);

        File f = new File("ddl.sql");
//        f.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(f);
//        System.out.println(firstCanonicalDDL);
        fos.write(firstCanonicalDDL.getBytes());

        ProcessBuilder pb = new ProcessBuilder("bin/sqlcmd");
        pb.redirectInput(f);
        Process process = pb.start();

        InputStream is = process.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);

        String line;
        StringBuffer sb = new StringBuffer();
        while((line = br.readLine()) != null)
        {
            sb.append(line);
            System.out.println(line);
        }

        URL ddlURL = new URL("http://localhost:8080/ddl/");

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

        sb = new StringBuffer();
        while ((line = in.readLine()) != null) {
            sb.append(line + "\n");
        }
        System.out.println("============================================");
        System.out.println(sb.toString());

        teardownSystem();
    }

    @Test
    public void testCanonicalDDLRoundtrip() throws Exception {
        firstCanonicalDDL = getFirstCanonicalDDL();


        secondCanonicalDDLFromAdhoc = getSecondCanonicalDDLFromAdhoc();
//
        assertEquals(firstCanonicalDDL, secondCanonicalDDLFromAdhoc);
    }

}
