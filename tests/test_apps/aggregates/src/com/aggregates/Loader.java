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

package com.aggregates;

import java.io.IOException;

import org.voltdb.client.*;

public class Loader {

    // A blob of data to store over and over again to force large rows.
    static StringBuffer bigStringPayload = new StringBuffer();
    static {
        for  (int i=0; i < 1000; i++) {
            bigStringPayload.append("C");
        }
    }

    static class MyAsyncCallback implements ProcedureCallback {
        @Override
        public synchronized void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();
            if (status != ClientResponse.SUCCESS) {
                System.err.println("Loader failed an insertion.");
                System.err.println(clientResponse.getStatusString());
                System.err.println(clientResponse.getException());
                System.exit(-1);
            }
        }
    }

    public static void main(String[] args) {
        org.voltdb.client.Client myApp = null;
        String hostname = null;
        String username = null;
        String password = null;
        int lines = 0;
        try {
            if ((args != null) && (args.length < 3)) {
                System.out
                .println("usage: hostname username password P1_rowcount");
                System.exit(1);
            }
            hostname = args[0];
            username = args[1];
            password = args[2];
            lines = Integer.valueOf(args[3]);
            myApp = getConnection(hostname, username, password);
            runLoader(myApp, lines);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        } finally {
            try {
                if (myApp != null) {
                    myApp.drain();
                    myApp.close();
                }
            } catch (NoConnectionsException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // SEE: readme.markdown for a description of what's loaded.
    private static void runLoader(Client myApp, int rows) throws NoConnectionsException, IOException {
        int p2pk = 0;

        for (int i=0; i < rows; i++) {
            // insert in to P1
            int p1pk = i;
            int int1 = p1pk * -1;
            myApp.callProcedure(new MyAsyncCallback(), "InsertP1", p1pk, int1, bigStringPayload.toString());

            // Insert the foreign key rows in to P2
            for (int j=0; j < (i % 5); j++) {
                int p2int1 = p2pk * -1;
                myApp.callProcedure(new MyAsyncCallback(), "InsertP2", p2pk++, p1pk, p2int1, bigStringPayload.toString());
            }
        }
    }

    static private org.voltdb.client.Client
    getConnection(String hostname, String user, String password)
    throws Exception
    {
        org.voltdb.client.Client myApp = null;
        myApp = ClientFactory.createClient();
        myApp.createConnection(hostname, user, password);
        return myApp;
    }
}
