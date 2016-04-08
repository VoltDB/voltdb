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

package genqa;

import genqa.VerifierUtils.Config;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class ReadVoltRows {
    // static VoltLogger log = new VoltLogger("ReadVoltRows");
    long rowid = 0;
    long numread = 0;
    Client m_client;

    public ReadVoltRows(Client client) {
        // log = new VoltLogger("ReadVoltRows.readSomeRows");
        System.out.println("rvr constructor");
        m_client = client;
    }

    public VoltTable readSomeRows(long rowid, long count)
            throws NoConnectionsException, IOException, ProcCallException {
        // log = new VoltLogger("ReadVoltRows.readSomeRows");

        ClientResponse response = m_client.callProcedure("SelectwithLimit",
                rowid, rowid + count - 1, count);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.out.println("Bad response on SelectwithLimit: "
                    + ClientResponse.SUCCESS);
            System.exit(-1);
        }
        return response.getResults()[0];
    }

    public boolean checkTable(VoltTable t, Connection jdbccon) {
        // rowid is column 0
        // get rowid first, then use it to pull a matching row from Vertica

        long rowid = 0;
        final int colCount = t.getColumnCount();
        int colMismatches = 1;
        ResultSet rs;
        int rowCheck = 0; // number of columns mismatching in row
        boolean success = true;
        t.resetRowPosition();
        while (t.advanceRow()) {
            rowid = t.getLong("rowid");
            //System.out.println("Got Volt row " + rowid);
            rs = JDBCGetData.jdbcRead(rowid);
            //System.out.println("Got JDBC row");
            try {
                colMismatches = RowCompare.rowcompare(t, rs);
                if (colMismatches != 0) {
                    System.err.println("Row check failed on rowId: " + rowid + " on " + colMismatches + " columns.");
                    success = false;
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return success;
    }
}
