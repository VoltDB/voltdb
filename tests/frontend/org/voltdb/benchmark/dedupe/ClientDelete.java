/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.benchmark.dedupe;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class ClientDelete {
    public static void main(String args[]) {
        long numDeletes = Long.valueOf(args[0]);
        String serverList = args[1];
        int outputToFile = Integer.valueOf(args[2]);
        String outputFileName = "log-ClientDelete.log";
        FileOutputStream foStatus;

        System.out.printf("Executing %,d deletes per transaction\n",numDeletes);
        if (outputToFile == 1) {
            System.out.printf("Outputting to file %s\n",outputFileName);
        } else {
            System.out.printf("File output disabled.\n");
        }

        int num_partitions = 0;

        //int intCounter;
        long longCounter;

        ClientConfig clientConfig = new ClientConfig("program", "none");
        final org.voltdb.client.Client voltclient = ClientFactory.createClient(clientConfig);

        String[] voltServers = serverList.split(",");

        for (String thisServer : voltServers) {
            try {
                System.out.printf("Connecting to server: %s\n",thisServer);

                voltclient.createConnection(thisServer);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //long startTime = System.currentTimeMillis();

        // get the # of partitions in my cluster
        try {
            VoltTable vtPartitionInfo[] = voltclient.callProcedure("@Statistics","partitioncount",0l).getResults();
            num_partitions = (int) vtPartitionInfo[0].fetchRow(0).getLong(0);
            System.out.println("System is running with " + num_partitions + " partition(s).");
        } catch (ProcCallException e) {
            System.err.println("ProcCallException:");
            e.printStackTrace();
        } catch (NoConnectionsException e) {
            System.err.println("NoConnectionsException:");
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("IOException:");
            e.printStackTrace();
        }

        boolean foundRows = true;

        while (foundRows) {
            // do a single delete at each partition
            foundRows = false;

            for (longCounter = 0; longCounter < num_partitions; longCounter++) {
                try {
                    long callTimeBegin = System.currentTimeMillis();

                    VoltTable vtDeleteVisits[] = voltclient.callProcedure("DeleteVisits", longCounter, numDeletes, callTimeBegin).getResults();
                    int rowCount = (int) vtDeleteVisits[0].fetchRow(0).getLong(0);

                    long callTimeEnd = System.currentTimeMillis();

                    String currentDate = new Date().toString();
                    System.out.printf("[%s] Ran delete on partition %d : deleted %,d row(s) in %,d milliseconds\n",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin));
                    if (outputToFile == 1) {
                        try {
                            foStatus = new FileOutputStream(outputFileName,true);
                            new PrintStream(foStatus).printf("[%s] Ran delete on partition %d : deleted %,d row(s) in %,d milliseconds\n",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin));
                            foStatus.close();
                        } catch (IOException e) {
                              e.printStackTrace();
                              System.exit(-1);
                        }
                    }

                    if (rowCount > 0) {
                        foundRows = true;
                    }
                } catch (ProcCallException e) {
                    System.err.println("ProcCallException:");
                    e.printStackTrace();
                } catch (NoConnectionsException e) {
                    System.err.println("NoConnectionsException:");
                    e.printStackTrace();
                } catch (IOException e) {
                    System.err.println("IOException:");
                    e.printStackTrace();
                }
            }
        }

        String currentDate2 = new Date().toString();
        System.out.printf("[%s] Finished deletion job, shutting down.\n",currentDate2);
        if (outputToFile == 1) {
            try {
                foStatus = new FileOutputStream(outputFileName,true);
                new PrintStream(foStatus).printf("[%s] Finished deletion job, shutting down.\n",currentDate2);
                foStatus.close();
            } catch (IOException e) {
                  e.printStackTrace();
                  System.exit(-1);
            }
        }

        try {
            voltclient.drain();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            voltclient.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

