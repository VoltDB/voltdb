package org.voltdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestRoutingEdgeCases extends TestCase {

    /**
     * Insert into a <8byte integer column using byte[] to send
     * the int. The column is also the partition column. Make sure
     * TheHashinator doesn't screw up.
     */
    @Test
    public void testPartitionKeyAsBytes() throws Exception {

        ServerThread localServer = null;
        Client client = null;

        try {
            String simpleSchema =
                    "create table blah (" +
                    "ival integer default 0 not null, " +
                    "PRIMARY KEY(ival)); " +
                    "PARTITION TABLE blah ON COLUMN ival;";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(simpleSchema);
            builder.addStmtProcedure("Insert", "insert into blah values (?);", null);
            boolean success = builder.compile(Configuration.getPathToCatalogForTest("edgecases.jar"), 7, 1, 0);
            assert(success);
            MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("edgecases.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("edgecases.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("edgecases.xml");
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            try {
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.order(ByteOrder.LITTLE_ENDIAN);
                buf.putInt(7);
                byte[] value = buf.array();

                client.callProcedure("Insert", value);
                fail();
            }
            catch (ProcCallException pce) {
                assertTrue(pce.getMessage().contains("Array / Scalar parameter mismatch"));
            }

            // for now, @LoadSinglepartitionTable assumes 8 byte integers, even if type is < 8 bytes
            // this is ok because we don't expose this functionality

            //VoltTable t = new VoltTable(new VoltTable.ColumnInfo("foo", VoltType.INTEGER));
            //t.addRow(13);
            //
            //ByteBuffer buf = ByteBuffer.allocate(4);
            //buf.order(ByteOrder.LITTLE_ENDIAN);
            //buf.putInt(13);
            //byte[] value = buf.array();

            //client.callProcedure("@LoadSinglepartitionTable", value, "blah", t);
        }
        finally {
            if (client != null) client.close();
            if (localServer != null) localServer.shutdown();
        }
    }
}
