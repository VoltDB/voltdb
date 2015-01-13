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
import java.io.InputStreamReader;
import java.util.Scanner;

import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientResponse;

public class LoadT {
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
                                // clientResponse.getResults() = VoltTable[] from stored
                                // procedure
                                // clientResponse.getClientRoundtrip() = total time (in
                                // milliseconds) from stored procedure invocation to return
                        }
                }
        }

        public static void main(String[] args) {
                org.voltdb.client.Client myApp = null;
                String hostname = null;
                String username = null;
                String password = null;
                int lines = 0;
                System.out.println("Loading T table");
                try {

                        if ((args != null) && (args.length < 3)) {
                                System.out
                                                .println("args: hostname username password");
                                System.exit(1);

                        }
                        hostname = args[0];
                        username = args[1];
                        password = args[2];
                        lines = Integer.valueOf(args[3]);


                        myApp = getConnection(hostname, username, password);

                        processFile(myApp, lines);

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

        static private void processFile(org.voltdb.client.Client myApp,
                                        int lines) throws Exception {
                FileInputStream fstream = null;
                DataInputStream in = null;
                String strLine = null;
        long totalLines = 0;

                try
                {
                    for (long workspaceId = 1; workspaceId < 6; workspaceId++)
                    {
                        System.out.println(String.format("Loading workspaceId %d",workspaceId));

                        long lines_per_workspaceId = lines;
                        long countoflines = 0;

                        while (countoflines < lines_per_workspaceId) {
                                ++countoflines;
                    totalLines++;

                    strLine = String.format("%d|%d|N|aaaa|%d|bbbb|cccc|dddd|eeee|ffff|Y|gggg|hhhh|jjjj|kkkk|llll|mmmm|US|01012000|nnnn|01022000|oooo|pppp|Y|qqqq|rrrr|ssss|N|tttt|2222|uuuu|01032000|vvvv|01042000|100.01|xxxx|1|2|3|4|5|6|Y|5555|01052000|yyyy|zzzz|AAAA|BBBB|CCCC|7777",
                                            workspaceId, countoflines, countoflines);

                                Object[] parameterargs = getEachLine(strLine);

                                try {
                                        myApp.callProcedure(new MyAsyncCallback(),"InsertT", parameterargs);
                                } catch (NoConnectionsException e) {
                                        e.printStackTrace();
                                }

                                if ((countoflines % 50000) == 0) {
                                        System.out.printf("On Line %,d\n", countoflines);
                                }
                        }
            }
                }
                catch(Exception exp)
                {
                        exp.printStackTrace();
                }
                finally {
                        System.out.println("This is the last line" + strLine);
                        System.out.println("Count of lines " + totalLines);
                        // Close the input stream
                        if (in != null) {
                                in.close();
                        }
                        if (fstream != null) {
                                fstream.close();
                        }

                }

        }

        public static String[] getEachLine(String strLine) throws Exception {
                if(strLine!=null)
                {
                        strLine=strLine.trim();
                }
                String strwithEnfOfLine = strLine + "|EOL";
                Scanner sc = new Scanner(strwithEnfOfLine);
                sc.useDelimiter("\\|");
                int i = 0;
                String[] parameterargs = new String[51];
                while (sc.hasNext()) {
                        if (i == 51) {
                                break;
                        }
                        String token = sc.next();
                        parameterargs[i] = token;
                        i++;

                }

                return parameterargs;
        }
}
