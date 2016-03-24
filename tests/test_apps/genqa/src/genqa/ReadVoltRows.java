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

import java.io.IOException;

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
     //static VoltLogger log = new VoltLogger("ReadVoltRows");
    long rowid = 0;
    long numread = 0;
    Client m_client;

    public ReadVoltRows(Client client) {
        //log = new VoltLogger("ReadVoltRows.readSomeRows");
        System.out.println("rvr constructor");
        m_client = client;
    }

    public VoltTable readSomeRows(long rowid, long count) throws NoConnectionsException, IOException, ProcCallException {
        //log = new VoltLogger("ReadVoltRows.readSomeRows");

        ClientResponse response = m_client.callProcedure("SelectwithLimit", rowid, count);
        if (response.getStatus() != ClientResponse.SUCCESS) {
            System.out.println("Bad response on SelectwithLimit: " + ClientResponse.SUCCESS);
            System.exit(-1);
        }
        return response.getResults()[0];
    }

    public static void main(String[] args) {
        VoltLogger log = new VoltLogger("ReadVoltRows.main");

        String servers = "localhost";
        int ratelimit = 2_000_000;
        long rowid = 0;
        long count = 1;
        Client client = null;
        ReadVoltRows rvr;
        log.info("starting rvr");

        System.exit(0);
        try {
            client = VerifierUtils.dbconnect(servers, ratelimit);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        log.info("opened db connection");
        rvr = new ReadVoltRows(client);

        VoltTable v = null;
        try {
            v = rvr.readSomeRows(rowid, count);
        } catch (IOException | ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        while (v.advanceRow()) {
            log.info("v: " + v.toFormattedString());
            int i = (int) v.get(0, VoltType.INTEGER);
            log.info("i: " + i);
        }
    }

}
