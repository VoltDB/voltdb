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
import java.util.Random;
import java.util.Scanner;

import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

public class LoadS {
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
                                                .println("args: hostname username password lines");
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
                int countoflines = 0;
                int max = 0;
                String strLine = null;

                try
                {
                        long total_lines = lines;

                        Random rand = new Random(System.currentTimeMillis());

                    while (countoflines < total_lines)
                    {
                            ++countoflines;

                            int int2 = rand.nextInt();
                            if (int2 > max)
                            {
                                max = int2;
                            }

                strLine = String.format("%d*aaaa*01012000*bbbb*cccc*dddd*eeee*N**%d*************10000000**273873817.87*9898989.87********",
                                        countoflines, int2);

                            String[] parameterargs = getEachLine(strLine);
                            // Need to send the partitioned key as long or else this will
                            // throw error during runtime

                            callProcedure(myApp, countoflines, parameterargs[0],
                                            parameterargs[1], parameterargs[2], parameterargs[3],
                                            parameterargs[4], parameterargs[5], parameterargs[6],
                                            parameterargs[7], parameterargs[8], parameterargs[9],
                                            parameterargs[10], parameterargs[11],
                                            parameterargs[12], parameterargs[13],
                                            parameterargs[14], parameterargs[15],
                                            parameterargs[16], parameterargs[17],
                                            parameterargs[18], parameterargs[19],
                                            parameterargs[20], parameterargs[21],
                                            parameterargs[22], parameterargs[23],
                                            parameterargs[24], parameterargs[25],
                                            parameterargs[26], parameterargs[27],
                                            parameterargs[28], parameterargs[29],
                                            parameterargs[30], parameterargs[31],
                                            parameterargs[32], parameterargs[33]);

                    }
                } catch (Exception exp) {
                        exp.printStackTrace();
                } finally {
                        System.out.println("This is the last line" + strLine);
                        System.out.println("Count of lines " + countoflines);
                        System.out.println("MAX S_INT2 VALUE: " + max);
                        // Close the input stream
                        if (in != null) {
                                in.close();
                        }
                        if (fstream != null) {
                                fstream.close();
                        }

                }

        }

        private static void callProcedure(org.voltdb.client.Client myApp,
                        int countoflines, String s_pk, String v1,
                        String ts1, String v2,
                        String v3, String v4,
                        String v5, String v6,
                        String int1, String int2,
                        String int3, String int4,
                        String int5, String int6,
                        String v7, String int7, String v8,
                        String v9, String v10, String v11,
                        String bigint1, String v12, String bigint2,
                        String bigint3, String decimal1,
                        String decimal2, String decimal3,
                        String decimal4, String bigint4,
                        String bigint5, String v13,
                        String bigint6,
                        String bigint7, String bigint8)
                        throws NoConnectionsException, IOException {
                boolean queued = false;

                long s_pk_temp = 0;
                if (s_pk != null) {
                        s_pk_temp = Long.parseLong(s_pk);
                }
                int int1_temp = 0;
                if ((int1 != null)
                                && (!"".equals(int1))) {
                        int1_temp = Integer.parseInt(int1);
                }

                int int2_temp = 0;

                if ((int2 != null)
                                && (!"".equals(int2))) {
                        int2_temp = Integer.parseInt(int2);
                }
                int int3_temp = 0;

                if ((int3 != null) && (!"".equals(int3))) {
                        int3_temp = Integer.parseInt(int3);
                }
                int int4_temp = 0;
                if ((int4 != null)
                                && (!"".equals(int4))) {
                        int4_temp = new Integer(int4);
                }

                int int5_temp = 0;
                if ((int5 != null)
                                && (!"".equals(int5))) {
                        int5_temp = Integer.parseInt(int5);
                }
                int int6_temp = 0;

                if ((int6 != null) && (!"".equals(int6))) {
                        int6_temp = Integer.parseInt(int6);
                }
                long bigint1_temp = 0;
                if ((bigint1 != null) && (!"".equals(bigint1))) {
                        bigint1_temp = Long.parseLong(bigint1);
                }

                long bigint2_temp = 0;

                if ((bigint2 != null) && (!"".equals(bigint2))) {
                        bigint2_temp = Long.parseLong(bigint2);
                }
                long bigint3_temp = 0;

                if ((bigint3 != null) && (!"".equals(bigint3))) {
                        bigint3_temp = Long.parseLong(bigint3);
                }

                BigDecimal decimal1_temp = null;
                if ((decimal1 != null) && (!"".equals(decimal1))) {
                        decimal1_temp = new BigDecimal(decimal1);
                }
                BigDecimal decimal2_temp = null;
                if ((decimal2 != null) && (!"".equals(decimal2))) {
                        decimal2_temp = new BigDecimal(decimal2);
                }
                BigDecimal decimal3_temp = null;
                if ((decimal3 != null) && (!"".equals(decimal3))) {
                        decimal3_temp = new BigDecimal(decimal3);
                }
                BigDecimal decimal4_temp = null;
                if ((decimal4 != null) && (!"".equals(decimal4))) {
                        decimal4_temp = new BigDecimal(decimal4);
                }

                long bigint4_temp = 0;
                ;
                if ((bigint4 != null) && (!"".equals(bigint4))) {
                        bigint4_temp = Long.parseLong(bigint4);
                }
                long bigint5_temp = 0;
                ;
                if ((bigint5 != null) && (!"".equals(bigint5))) {
                        bigint5_temp = Long.parseLong(bigint5);
                }

                long bigint6_temp = 0;
                ;
                if ((bigint6 != null)
                                && (!"".equals(bigint6))) {
                        bigint6_temp = Long
                                        .parseLong(bigint6);
                }
                long bigint7_temp = 0;
                ;
                if ((bigint7 != null)
                                && (!"".equals(bigint7))) {
                        bigint7_temp = Long
                                        .parseLong(bigint7);
                }
                long bigint8_temp = 0;
                ;
                if ((bigint8 != null) && (!"".equals(bigint8))) {
                        bigint8_temp = Long.parseLong(bigint8);
                }
                int int7_temp = 0;
                if ((int7 != null) && (!"".equals(int7))) {
                        int7_temp = Integer.parseInt(int7);
                }

                TimestampType ts1_temp = null;

                if ((ts1 != null) && (!"".equals(ts1))) {
                        SimpleDateFormat sdf = new SimpleDateFormat("MMddyyyy");

                        Date ts1_temp_temp = null;
                        try {
                                ts1_temp_temp = sdf.parse(ts1);
                                ts1_temp = new TimestampType(
                                                ts1_temp_temp.getTime() * 1000);
                        } catch (ParseException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                        }
                }

                myApp.callProcedure(new MyAsyncCallback(), "InsertS",
                                        s_pk_temp, v1, ts1_temp, v2,
                                        v3, v4, v5,
                                        v6, int1_temp,
                                        int2_temp, int3_temp,
                                        int4_temp, int5_temp,
                                        int6_temp, v7, int7_temp, v8,
                                        v9, v10, v11, bigint1_temp,
                                        v12, bigint2_temp, bigint3_temp, decimal1_temp,
                                        decimal2_temp, decimal3_temp, decimal4_temp,
                                        bigint4_temp, bigint5_temp, v13,
                                        bigint6_temp, bigint7_temp,
                                        bigint8_temp);

                // myApp.callProcedure("Insert", parameterargs);

                if ((countoflines % 25000) == 0) {
                        System.out.printf("On Line %,d\n", countoflines);
                }

        }

        public static String[] getEachLine(String strLine) throws Exception {
                if (strLine != null) {
                        strLine = strLine.trim();
                }
                String strwithEnfOfLine = strLine + "*EOL";
                Scanner sc = new Scanner(strwithEnfOfLine);
                sc.useDelimiter("\\*");
                int i = 0;
                String[] parameterargs = new String[34];
                while (sc.hasNext()) {
                        if (i == 34) {
                                break;
                        } else {
                                String token = sc.next();
                                parameterargs[i] = token;

                        }
                        i++;
                }

                return parameterargs;
        }

}
