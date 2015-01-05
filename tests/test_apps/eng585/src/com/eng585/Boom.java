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

package com.eng585;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

public class Boom {
        static class MyAsyncCallback implements ProcedureCallback {
                @Override
                public synchronized void clientCallback(ClientResponse clientResponse) {
                        final byte status = clientResponse.getStatus();

                        if (status != ClientResponse.SUCCESS) {
                                System.err.println("Failed to execute!!!");
                                System.err.println(clientResponse.getStatusString());
                                System.err.println(clientResponse.getException());
                                System.exit(-1);
                        } else {
                            long max = clientResponse.getResults()[0].asScalarLong();
                            System.out.println("MAX: " + max);
                        }
                }
        }

        public static void main(String[] args) {
                org.voltdb.client.Client myApp = null;
                String hostname = null;
                String username = null;
                String password = null;
                try {

                        if ((args != null) && (args.length < 3)) {
                                System.out
                                                .println("args: hostname username password");
                                System.exit(1);

                        }
                        hostname = args[0];
                        username = args[1];
                        password = args[2];

                        myApp = getConnection(hostname, username, password);

                        myApp.callProcedure(new MyAsyncCallback(), "test", "1");

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
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        } catch (InterruptedException ie) {
                                // TODO Auto-generated catch block
                                ie.printStackTrace();
                        }
                }

        }

        static private org.voltdb.client.Client getConnection(String hostname,
                        String user, String password) throws Exception {
                org.voltdb.client.Client myApp = null;
                myApp = ClientFactory.createClient();
                myApp.createConnection(hostname, user, password);

                return myApp;

        }
}
