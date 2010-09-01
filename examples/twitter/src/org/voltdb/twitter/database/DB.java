/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
package org.voltdb.twitter.database;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.twitter.database.procedures.Delete;
import org.voltdb.twitter.database.procedures.Insert;
import org.voltdb.twitter.database.procedures.Select;
import org.voltdb.twitter.util.HashTag;

public class DB {

    private Client client;

    public DB(List<String> servers) {
        client = ClientFactory.createClient();
        for (String server : servers) {
            try {
                client.createConnection(server, "program", "none");
            } catch (UnknownHostException e) {
                e.printStackTrace();
                System.exit(-1);
            } catch (IOException e) {
                System.err.println("Could not connect to database, terminating: (" + server + ")");
                System.exit(-1);
            }
        }
    }

    // insert a new hashtag
    public void insertHashTag(String hashTag) {
        try {
            client.callProcedure(
                    new ProcedureCallback() {

                        @Override
                        public void clientCallback(ClientResponse response) {}

                    },
                    Insert.class.getSimpleName(),
                    hashTag,
                    System.currentTimeMillis() * 1000L);
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // select hashtags within a certain time range
    public List<HashTag> selectHashTags(long maxAgeMicros, int limit) {
        try {
            ClientResponse response = client.callProcedure(Select.class.getSimpleName(),
                    System.currentTimeMillis() * 1000L - maxAgeMicros, limit);
            VoltTable[] tables = response.getResults();
            VoltTable table = tables[0];
            VoltTableRow row = table.fetchRow(0);

            int rowCount = table.getRowCount();
            List<HashTag> hashTags = new LinkedList<HashTag>();
            for (int i = 0; i < rowCount; i++) {
                hashTags.add(new HashTag(row.getString(0), (int) row.getLong(1)));
                row.advanceRow();
            }
            return hashTags;
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }

        return null;
    }

    // delete hashtags older than a certain max age
    public long deleteHashTags(long maxAgeMicros) {
        try {
            long deleteAllEarlierThan = System.currentTimeMillis() * 1000L - maxAgeMicros;
            long deleteCount = client.callProcedure(Delete.class.getSimpleName(),
                    deleteAllEarlierThan).getResults()[0].asScalarLong();
            return deleteCount;
        } catch (NoConnectionsException e) {
            System.err.println("Lost connection to database, terminating");
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ProcCallException e) {
            e.printStackTrace();
        }
        return -1;
    }

}
