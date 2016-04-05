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

/* 4> insert into x values(1);
 (Returned 1 rows in 0.01s)
 5> insert into x values(2);
 (Returned 1 rows in 0.00s)
 */

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

    public void checkTable(VoltTable t, Connection jdbccon) {
        // rowid is column 0
        // get rowid first, then use it to pull a matching row from Vertica

        long rowid = 0;
        final int colCount = t.getColumnCount();
        int rowCount = 1;
        ResultSet rs;

        t.resetRowPosition();
        while (t.advanceRow()) {
            rowid = t.getLong("rowid");
            System.out.println("Got Volt row " + rowid);
            rs = JDBCGetData.jdbcRead(rowid);
            System.out.println("Got JDBC row");
            try {
                RowCompare.rowcompare(t, rs);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    private void compare(VoltTable t, ResultSet rs) {
        // iterate through columns and check data values for expected match
        System.out.println("Preparing to compare VDB and JDBC rows:");
        // System.out.println("VoltTable: " + t.toString());
        // System.out.println("JDBC row: " + rs.toString());
        try {
            if (!rs.next()) {
                System.out.println("In compare: no JDBC row available");
            } else {
                long rowid = rs.getInt("rowid");
                System.out.println("In compare: JDBC rowid: " + rowid);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void displayTable(VoltTable t) {

        final int colCount = t.getColumnCount();
        int rowCount = 1;
        t.resetRowPosition();
        while (t.advanceRow()) {
            System.out.printf("--- Row %d ---\n", rowCount++);

            for (int col = 0; col < colCount; col++) {
                System.out.printf("%s: ", t.getColumnName(col));
                switch (t.getColumnType(col)) {
                case TINYINT:
                case SMALLINT:
                case BIGINT:
                case INTEGER:
                    System.out.printf("%d\n", t.getLong(col));
                    break;
                case STRING:
                    System.out.printf("%s\n", t.getString(col));
                    break;
                case DECIMAL:
                    System.out.printf("%f\n", t.getDecimalAsBigDecimal(col));
                    break;
                case FLOAT:
                    System.out.printf("%e\n", t.getDouble(col));
                    break;
                case GEOGRAPHY:
                    System.out.printf("%s\n", t.getGeographyValue(col));
                    break;
                case GEOGRAPHY_POINT:
                    System.out.printf("%s\n", t.getGeographyPointValue(col));
                    break;
                case NULL:
                    System.out.print("null");
                    break;
                case TIMESTAMP:
                    System.out.printf("%s\n", t.getTimestampAsLong(col));
                    break;
                default:
                    System.out.print("Default case: "
                            + t.getColumnType(col).toString());
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        // VoltLogger log = new VoltLogger("ReadVoltRows.main");

        // setup configuration from command line arguments and defaults
        Config config = new VerifierUtils.Config();
        config.parse(JDBCVoltVerifier.class.getName(), args);
        System.out.println("Configuration settings:");
        System.out.println(config.getConfigDumpString());

        String servers = "localhost";
        int ratelimit = 2_000_000;
        long rowid = 0;
        Client client = null;
        ReadVoltRows rvr;
        JDBCGetData jdbc;
        Connection jdbccon;
        System.out.println("starting rvr");

        jdbc = new JDBCGetData();
        jdbccon = jdbc.jdbcConnect(config);

        // System.exit(0);
        try {
            client = VerifierUtils.dbconnect(servers, ratelimit);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        System.out.println("opened db connection");
        rvr = new ReadVoltRows(client);

        long rowCount = 0;
        VoltTable v = null;

        do {
            // System.out.println("i: " + i);
            try {
                v = rvr.readSomeRows(rowid, 10);
            } catch (IOException | ProcCallException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            rowCount = v.getRowCount();
            System.out.println("rowCount: " + rowCount + ". rowid: " + rowid);
            rowid += 10;
            rvr.displayTable(v);
        } while (rowCount > 0);

        // while (v.advanceRow()) {
        // System.out.println("v: " + v.toFormattedString());
        // int i = (int) v.get(0, VoltType.INTEGER);
        // System.out.println("i: " + i);
        // }
    }
}
