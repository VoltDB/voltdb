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

import java.util.Random;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.utils.Encoder;

public class TestVarBinaryPartition extends TestCase {
    public void testPartitionAndInsert () throws Exception {
        String simpleSchema =
                "create table BLAH (" +
                "clm_varinary varbinary(128) default '00' not null," +
                "clm_smallint smallint default 0 not null, " +
                ");" +
                "";
        //.addStmtProcedure("Insert", "INSERT into BLAH values (?, ?, ?, ?, ?, ?, ?);");
        Configuration config = Configuration.compile(getClass().getSimpleName(), simpleSchema,
                new DeploymentBuilder(2));
        assertNotNull("Configuration failed to compile", config);
        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponse resp;
        resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES ('22',1);");
        assertEquals(1, resp.getResults()[0].asScalarLong());
        resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES ('80',3);" );
        assertEquals(1, resp.getResults()[0].asScalarLong());
        resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES ('8081828384858687888990',4);" );
        assertEquals(1, resp.getResults()[0].asScalarLong());

        Random rand = new Random();
        for ( int i = 0; i < 1000; i++ ) {
            byte[] bytes = new byte[rand.nextInt(128)];
            rand.nextBytes(bytes);
            // Just to mix things up, alternate methods of INSERT among
            // literal hex string, hex string parameter, and byte[] parameter.
            if ( i % 2 == 0 ) {
                String hexString = Encoder.hexEncode(bytes);
                if ( i % 4 == 0 ) {
                    resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES ('" + hexString + "',5);" );
                } else {
                    resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (?,5);", hexString);
                }
            } else {
                resp = client.callProcedure("@AdHoc", "INSERT INTO BLAH VALUES (?,5);", bytes);
            }
            assertEquals(1, resp.getResults()[0].asScalarLong());
        }
        resp =  client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM BLAH;");
        assertEquals(3 + 1000, resp.getResults()[0].asScalarLong());
    }
}
