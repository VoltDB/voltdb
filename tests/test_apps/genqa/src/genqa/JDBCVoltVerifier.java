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
import java.util.Timer;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

public class JDBCVoltVerifier {
    // Volt DB client handle
    static Client client;

    // JDBC client handle
    Connection conn;

    static VerifierUtils.Config config;

    // validated command line configuration
    // static Config config;
    // Timer for periodic stats printing
    static Timer statsTimer;
    static Timer checkTimer;
    // Benchmark start time
    long benchmarkStartTS;
    static long rowCheckTotal = 0;
    static boolean FAILFAST = true;
    /**
     * compare each column in a batch of rows, batches are processed by this query:
     * select * from export_mirror_partitioned_table where rowid between ? and ? order by rowid limit ?
     * @param rvr
     * @param client
     * @param jdbcclient
     * @return
     */
    public static boolean processRows(ReadVoltRows rvr, Client client, Connection jdbcclient) {

        int batchSize = 200;
        long rowid = 0;
        long rowCount = 0;
        VoltTable v = null;
        boolean checkStatus = true;
        do {
            try {
                v = rvr.readSomeRows(rowid, batchSize);
            } catch (IOException | ProcCallException e) {
                e.printStackTrace();
            }

            rowCount = v.getRowCount();
            rowid += batchSize;
            rowCheckTotal += rowCount;

            if (rowCount > 0) {
                checkStatus = rvr.checkTable(v, jdbcclient);
                // Fail fast
                if ( !checkStatus && FAILFAST ) {
                    break;
                }
            }
            System.out.println("Current row id: " + rowid);
        } while (rowCount > 0);
        return checkStatus;
    }

    public static void main(String[] args) {
        Client client = null;
        Connection jdbcConnection;
        ReadVoltRows rvr;
        int ratelimit = Integer.MAX_VALUE;

        // setup configuration from command line arguments and defaults
        VerifierUtils.Config config = new VerifierUtils.Config();

        config.parse(JDBCVoltVerifier.class.getName(), args);
        System.out.println("Configuration settings:");
        System.out.println(config.getConfigDumpString());


        System.out.println("Connecting to " + config.vdbServers);
        try {
            client = VerifierUtils.dbconnect(config.vdbServers, ratelimit);
        } catch (IOException e) {
            e.printStackTrace();
        }
        rvr = new ReadVoltRows(client);

        System.out.println("Connecting to the JDBC target " + config.jdbcDBMS);
        jdbcConnection = JDBCGetData.jdbcConnect(config);

        if ( ! processRows(rvr, client, jdbcConnection) ) {
            System.err.println("Check Table failed, see log for errors");
            System.exit(1);
        }
        System.out.println("Total rows checked in VoltDB and JDBC Table: " + rowCheckTotal);
        System.exit(0);
    }
}
