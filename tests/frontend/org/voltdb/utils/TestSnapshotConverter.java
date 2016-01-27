/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.utils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Calendar;
import java.util.Random;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.SaveRestoreBase;

public class TestSnapshotConverter extends SaveRestoreBase
{

    public TestSnapshotConverter(String name) {
        super(name);
    }

    // Regression test for ENG-8609
    public void testSnapshotConverter() throws NoConnectionsException, IOException, ProcCallException
    {
        if (!MiscUtils.isPro()) { return; } // not supported in community
        if (isValgrind()) return;

        Client client = getClient();
        int expectedLines = 10;
        Random r = new Random(Calendar.getInstance().getTimeInMillis());
        for (int i = 0; i < expectedLines; i++) {
            int id = r.nextInt();
            client.callProcedure("T_SP.insert", String.format("Test String %s:%d", "SP", i), id, "blab", "blab");
            client.callProcedure("T_MP.insert", String.format("Test String %s:%d", "MP", i), id, "blab", "blab");
        }

        VoltTable[] results = null;
        try {
            results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, 1).getResults();
        } catch(Exception ex) {
            ex.printStackTrace();
            fail();
        }

        System.out.println(results[0]);
        try {
            results = client.callProcedure("@SnapshotStatus").getResults();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            fail();
        }
        System.out.println(results[0]);
        // better be two rows
        assertEquals(2, results[0].getRowCount());

        // start convert to MP snapshot to csv
        String[] argsMP = {"--table", "T_MP", "--type", "CSV", "--dir", TMPDIR, "--outdir",TMPDIR, TESTNONCE};
        try  {
            SnapshotConverter.main(argsMP);
        } catch (Exception ex) {
            fail();
        }
        File mpFile = new File(TMPDIR+"/T_MP.csv");
        assertEquals(expectedLines,countLines(mpFile));
        mpFile.deleteOnExit();

        // start convert to SP snapshot to csv
        String[] argsSP = {"--table", "T_SP", "--type", "CSV", "--dir", TMPDIR, "--outdir",TMPDIR, TESTNONCE};
        try  {
            SnapshotConverter.main(argsSP);
        } catch (Exception ex) {
            fail();
        }
        File spFile = new File(TMPDIR+"/T_SP.csv");
        // this test will fail frequently with different lines before ENG-8609
        assertEquals(expectedLines,countLines(spFile));
        spFile.deleteOnExit();
    }

    //
    // Build a list of the tests to be run. Use the regression suite
    // helpers to allow multiple backends.
    // JUnit magic that uses the regression suite helper classes.
    //
    static public Test suite() throws IOException
    {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSnapshotConverter.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
                "CREATE TABLE T_SP(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));" +

                "CREATE TABLE T_MP(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));");
        project.addPartitionInfo("T_SP", "A1");

        LocalCluster lcconfig = new LocalCluster("testsnapshotstatus.jar", 8, 1, 0,
                                               BackendTarget.NATIVE_EE_JNI);
        assertTrue(lcconfig.compile(project));
        builder.addServerConfig(lcconfig);

        return builder;
    }

    public static int countLines(File aFile) throws IOException {
        LineNumberReader reader = null;
        try {
            reader = new LineNumberReader(new FileReader(aFile));
            while ((reader.readLine()) != null);
            return reader.getLineNumber();
        } catch (Exception ex) {
            return -1;
        } finally {
            if(reader != null)
                reader.close();
        }
    }
}
