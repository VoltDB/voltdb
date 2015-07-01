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

            // For now, @LoadSinglepartitionTable assumes 8 byte integers, even if type is < 8 bytes
            // This is ok because we don't expose this functionality.
            // The code below will throw a constraint violation, but it really shouldn't. There's
            // Another comment about this in ProcedureRunner about a reasonable fix for this.

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
