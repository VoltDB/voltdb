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
                    "nondetval bigint not null " +  // non-deterministic value (host ID)
                    "); " +
                    "PARTITION TABLE kv ON COLUMN key;" +
                    "CREATE INDEX idx_kv ON kv(nondetval);" +
                    "CREATE TABLE foo(key bigint not null, nondetval bigint not null);";

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
            for (int i = 0; i < 20; i++) {
                client.callProcedure("@AdHoc", "insert into kv values(" + i + "," + i + ")");
            }
            System.out.println("deleting from KV...");
            ClientResponse resp = client.callProcedure("@AdHoc", "delete from KV where key < 10");
            assert(resp.getStatus() == ClientResponse.SUCCESS);
            VoltTable vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            assert(10 == vt.asScalarLong());
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
