/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import org.junit.Test;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

public class TestDeterministicRowOrder extends JUnit4LocalClusterTest {

    protected static final String TMPDIR = "/tmp/" + System.getProperty("user.name");
    protected static final String TESTNONCE = "testnonce";

    static final String SCHEMA =
            "CREATE TABLE kv (" +
                    "key bigint not null, " +
                    "val bigint not null " +  // non-deterministic value (host ID)
                    "); " +
                    "PARTITION TABLE kv ON COLUMN key;" +
                    "CREATE INDEX idx_kv ON kv(val);" +
                    "CREATE TABLE foo(key bigint not null, val bigint not null);";

    Client client;
    final int sitesPerHost = 2;
    final int hostCount = 2;
    final int kfactor = 1;
    public static class TestProc extends VoltProcedure {
        public final SQLStmt stmt1 = new SQLStmt("DELETE FROM KV WHERE KEY < 10;");
        public final SQLStmt stmt2 = new SQLStmt("DELETE FROM FOO WHERE KEY < 10;");
        public final SQLStmt stmt3 = new SQLStmt("DELETE FROM KV WHERE KEY < 30;");
        public final SQLStmt stmt4 = new SQLStmt("DELETE FROM FOO WHERE KEY < 30;");
        public final SQLStmt stmt5 = new SQLStmt("DELETE FROM KV WHERE KEY < 60;");

        public VoltTable[] run() {
            voltQueueSQL(stmt1);
            voltExecuteSQL(false);
            voltQueueSQL(stmt2);
            voltExecuteSQL(false);
            voltQueueSQL(stmt3);
            voltExecuteSQL(false);
            voltQueueSQL(stmt4);
            voltExecuteSQL(false);
            voltQueueSQL(stmt5);
            return voltExecuteSQL(true);
         }
    }
    private static  final String PROCEDURES =
            "CREATE PROCEDURE FROM CLASS org.voltdb.TestDeterministicRowOrder$TestProc;" ;
    LocalCluster createCluster() throws IOException {
        LocalCluster server = null;
        VoltFile.resetSubrootForThisProcess();
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addLiteralSchema(PROCEDURES);
            server = new LocalCluster("TestDeterministicRowOrder.jar", sitesPerHost, hostCount, kfactor, BackendTarget.NATIVE_EE_JNI);
            server.overrideAnyRequestForValgrind();
            assertTrue("Catalog compilation failed", server.compile(builder));

            server.setHasLocalServer(false);
            client = ClientFactory.createClient();
            server.startUp();
            for (String s : server.getListenerAddresses()) {
                client.createConnection(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
        return server;
    }
    private void shutDown(LocalCluster cluster) {
        if ( client != null) {
            try {
                client.close();
            } catch (InterruptedException e) {
            }
        }
        if (cluster != null) {
            try {
                cluster.shutDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testBatchDelete() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster();
        try {
            for (int i = 0; i < 100; i++) {
                client.callProcedure("@AdHoc", "insert into KV values(" + i + "," + i + ")");
                client.callProcedure("@AdHoc", "insert into FOO values(" + i + "," + i + ")");
            }
            System.out.print("deleting from partitioned table KV...");
            ClientResponse resp = client.callProcedure("@AdHoc", "delete from KV where key < 50");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = client.callProcedure("@AdHoc", "select * from KV order by key").getResults()[0];
            int count = 0;
            while(vt.advanceRow()) {
                assert(vt.getLong(0) >= 50);
                count++;
            }
            assert(count == 50);
            System.out.print("deleting from replicated table FOO...");
            resp = client.callProcedure("@AdHoc", "delete from FOO where key < 50");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            vt = client.callProcedure("@AdHoc", "select * from FOO order by key").getResults()[0];
            count = 0;
            while(vt.advanceRow()) {
                assert(vt.getLong(0) >= 50);
                count++;
            }
            assert(count == 50);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testUpdate() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster();
        try {
            for (int i = 0; i <= 100; i++) {
                client.callProcedure("@AdHoc", "insert into KV values(" + i + "," + i + ")");
                client.callProcedure("@AdHoc", "insert into FOO values(" + i + "," + i + ")");
            }
            System.out.println("updating table KV...1");
            ClientResponse resp = client.callProcedure("@AdHoc", "update KV set val = 100 where key < 50;");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = client.callProcedure("@AdHoc", "select * from KV order by key").getResults()[0];
            while(vt.advanceRow()) {
                if (vt.getLong(0) < 50) {
                    assert(vt.getLong(1) == 100);
                } else {
                    assert(vt.getLong(1) >= 50);
                }
            }

            System.out.println("updating table KV...2");
            resp = client.callProcedure("@AdHoc", "update KV set val = 200 where key < 60;");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            vt = client.callProcedure("@AdHoc", "select * from KV order by key").getResults()[0];
            while(vt.advanceRow()) {
                if (vt.getLong(0) < 60) {
                    assert(vt.getLong(1) == 200);
                } else {
                    assert(vt.getLong(1) >= 60);
                }
            }

            System.out.println("update table FOO...1");
            resp = client.callProcedure("@AdHoc", "update FOO set val = 100 where key < 50;");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            vt = client.callProcedure("@AdHoc", "select * from FOO order by key").getResults()[0];
            while(vt.advanceRow()) {
                if (vt.getLong(0) < 50) {
                    assert(vt.getLong(1) == 100);
                } else {
                    assert(vt.getLong(1) >= 50);
                }
            }

            System.out.println("updating table FOO...2");
            resp = client.callProcedure("@AdHoc", "update FOO set val = 200 where key < 60;");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            vt = client.callProcedure("@AdHoc", "select * from FOO order by key").getResults()[0];
            while(vt.advanceRow()) {
                if (vt.getLong(0) < 60) {
                    assert(vt.getLong(1) == 200);
                } else {
                    assert(vt.getLong(1) >= 60);
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testTruncateDelete() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster();
        try {
            for (int i = 0; i <= 100; i++) {
                client.callProcedure("@AdHoc", "insert into KV values(" + i + "," + i + ")");
                client.callProcedure("@AdHoc", "insert into FOO values(" + i + "," + i + ")");
            }
            System.out.print("deleting from partitioned table KV...");
            ClientResponse resp = client.callProcedure("@AdHoc", "truncate table KV;");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = client.callProcedure("@AdHoc", "select count(*) from KV order by key").getResults()[0];
            assert(vt.asScalarLong() == 0);
            System.out.print("deleting from replicated table FOO...");
            resp = client.callProcedure("@AdHoc", "truncate table FOO");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            vt = client.callProcedure("@AdHoc", "select count(*) from FOO order by key").getResults()[0];
            assert(vt.asScalarLong() == 0);
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testSnapshotSaveAndRestore() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster();
        try {
            for (int i = 0; i < 20; i++) {
                client.callProcedure("@AdHoc", "insert into kv values(" + i + "," + i + ")");
                client.callProcedure("@AdHoc", "insert into foo values(" + i + "," + i + ")");
            }
            File tempDir = new File(TMPDIR);
            if (!tempDir.exists()) {
                assertTrue(tempDir.mkdirs());
            }
            deleteTestFiles(TESTNONCE);

            System.out.println("Saving snapshot...");
            ClientResponse resp  = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1);
            VoltTable vt = resp.getResults()[0];
            while (vt.advanceRow()) {
                assertTrue(vt.getString("RESULT").equals("SUCCESS"));
            }
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            long rows = vt.asScalarLong();
            vt = client.callProcedure("@AdHoc", "select count(*) from foo").getResults()[0];
            long mprows = vt.asScalarLong();
            client.drain();
            System.out.println("Saved snapshot with " + rows + ", reloading snapshot...");
            vt = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults()[0];
            System.out.println(vt.toFormattedString());
            while (vt.advanceRow()) {
                if (vt.getString("RESULT").equals("FAILURE")) {
                    fail(vt.getString("ERR_MSG"));
                }
            }
            System.out.println("snapshot reloaded");
            vt = client.callProcedure("@AdHoc", "select * from KV").getResults()[0];
            vt = client.callProcedure("@AdHoc", "select * from FOO").getResults()[0];
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            assert((rows *2) == vt.asScalarLong());
            vt = client.callProcedure("@AdHoc", "select count(*) from FOO").getResults()[0];
            assert((mprows *2) == vt.asScalarLong());
        } catch (Exception e) {
            fail(e.getMessage());
        } finally {
            shutDown(server);
        }
    }

    protected void deleteTestFiles(final String nonce) {
        FilenameFilter cleaner = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String file) {
                return file.startsWith(nonce) || file.endsWith(".vpt") || file.endsWith(".digest")
                        || file.endsWith(".tsv") || file.endsWith(".csv") || file.endsWith(".incomplete")
                        || new File(dir, file).isDirectory();
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files) {
            deleteRecursively(tmp_file);
        }
    }

    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File f2 : f.listFiles()) {
                deleteRecursively(f2);
            }
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) {
                    return;
                }
                System.err.println("Couldn't delete " + f.getPath());
                System.err.println("Remaining files are:");
                for (File f2 : f.listFiles()) {
                    System.err.println("    " + f2.getPath());
                }
                //Recurse until stack overflow trying to delete, y not rite?
                deleteRecursively(f);
            }
        } else {
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) {
                    return;
                }
                System.err.println("Couldn't delete " + f.getPath());
            }
            assertTrue(deleted);
        }
    }
}
