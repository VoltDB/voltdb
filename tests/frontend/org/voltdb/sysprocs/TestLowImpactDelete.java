/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.sysprocs;

import junit.framework.TestCase;

import org.voltdb.InProcessVoltDBServer;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class TestLowImpactDelete extends TestCase {

    // make sure LowImpactDelete complains if the table is missing
    // also doubles as a smoke test to make sure it can be called at all
    public void testMissingTable() throws Exception {
        InProcessVoltDBServer server = new InProcessVoltDBServer();
        try {
            server.start();
            Client client = server.getClient();

            // fail on missing table
            try {
                client.callProcedure("@LowImpactDelete", "notable", "nocolumn", "75", "<", 1000, 2000);
                fail();
            }
            catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Table \"notable\" not"));
            }

            // add a table
            client.callProcedure("@AdHoc", "create table foo (a integer, b varchar(255), c integer, primary key (a));");

            // fail on missing column
            try {
                client.callProcedure("@LowImpactDelete", "foo", "nocolumn", "75", "<", 1000, 2000);
                fail();
            }
            catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Column \"nocolumn\" not"));
            }

            // fail on missing index
            try {
                client.callProcedure("@LowImpactDelete", "foo", "c", "75", "<", 1000, 2000);
                fail();
            }
            catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Count not find index"));
            }

            // fail on improper type
            try {
                client.callProcedure("@LowImpactDelete", "foo", "a", "stringdata", "<", 1000, 2000);
                fail();
            }
            catch (ProcCallException e) {
                assertTrue(e.getMessage().contains("Unable to convert"));
            }
        }
        finally {
            server.shutdown();
        }
    }

}
