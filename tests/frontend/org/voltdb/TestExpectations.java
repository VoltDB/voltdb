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

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb_testprocs.regressionsuites.failureprocs.CrushExpectations;

public class TestExpectations extends TestCase {
    public void testSimple() throws Exception {
        CatalogBuilder cb = new CatalogBuilder(
                "create table blah (" +
                "ival bigint default 0 not null, " +
                "sval varchar(255) not null, " +
                "PRIMARY KEY(ival));\n" +
                "PARTITION TABLE blah ON COLUMN ival;\n" +
                "")
        .addStmtProcedure("Insert", "insert into blah values (?, ?);")
        .addProcedures(CrushExpectations.class)
        ;
        Configuration config = Configuration.compile(getClass().getSimpleName(), cb,
                new DeploymentBuilder());
        assertNotNull("Configuration failed to compile", config);
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        try {
            ClientResponse r = client.callProcedure("CrushExpectations", 0);
            assertEquals(0, r.getResults()[0].asScalarLong());
        }
        catch (ProcCallException e) {
            e.printStackTrace();
            fail();
        }

        client.close();

        localServer.shutdown();
    }
}
