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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

public class ClientMover {
    public static void main(String args[]) {
        long numMoves = Long.valueOf(args[0]);
        String serverList = args[1];
        long clientDurationSeconds = Long.valueOf(args[2]);
        long loopPauseSeconds = Long.valueOf(args[3]);
        int outputToFile = Integer.valueOf(args[4]);
        String outputFileName = "log-ClientMover.log";
        FileOutputStream foStatus;

        System.out.printf("Executing %,d moves per transaction\n",numMoves);
        System.out.printf("Running for %,d second(s)\n",clientDurationSeconds);
        if (outputToFile == 1) {
            System.out.printf("Outputting to file %s\n",outputFileName);
        } else {
            System.out.printf("File output disabled.\n");
        }

        int num_partitions = 0;

        long playerId;
        long gameId;
        long socialId;
        long clientId;
        long visitTime;

        int intCounter;
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

        //java.util.Random rand = new java.util.Random(0);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (1000l * clientDurationSeconds);
        long currentTime = startTime;

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
        boolean foundFullRowset = false;

        while (endTime > currentTime) {
            // do a single archive at each partition
            foundRows = false;
            foundFullRowset = false;

            long callTimeMillis = System.currentTimeMillis();

            FileOutputStream fout;
            try {
                String outFileName = "/tmp/" + callTimeMillis + "_rows.txt";

                fout = new FileOutputStream(outFileName,true);

                for (longCounter = 0; longCounter < num_partitions; longCounter++) {
                    try {
                        long callTimeBegin = System.currentTimeMillis();
                        VoltTable vtArchiveVisits[] = voltclient.callProcedure("ArchiveVisits", longCounter, numMoves, callTimeMillis).getResults();
                        long callTimeEnd = System.currentTimeMillis();
                        int rowCount = vtArchiveVisits[0].getRowCount();

                        if (rowCount > 0) {
                            // write out the rows
                            foundRows = true;

                            if (rowCount == numMoves) {
                                foundFullRowset = true;
                            }

                            // write out the rows
                            for (intCounter = 0; intCounter < rowCount; intCounter++) {
                                VoltTableRow row = vtArchiveVisits[0].fetchRow(intCounter);
                                playerId = row.getLong(0);
                                gameId = row.getLong(1);
                                socialId = row.getLong(2);
                                clientId = row.getLong(3);
                                visitTime = row.getLong(4);

                                Date visitDate = new Date(visitTime);

                                new PrintStream(fout).print(playerId + "\t" + gameId + "\t" + socialId + "\t" + clientId + "\t" + visitDate + "\n");
                            }
                        }

                        String currentDate = new Date().toString();
                        System.out.printf("[%s] Ran archive on partition %d : archived %,d row(s) in %,d milliseconds\n",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin));
                        if (outputToFile == 1) {
                            foStatus = new FileOutputStream(outputFileName,true);
                            new PrintStream(foStatus).printf("[%s] Ran archive on partition %d : archived %,d row(s) in %,d milliseconds\n",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin));
                            foStatus.close();
                        }

                    } catch (ProcCallException e) {
                        System.err.println("ProcCallException:");
                        e.printStackTrace();
                    } catch (NoConnectionsException e) {
                        System.err.println("NoConnectionsException:");
                        e.printStackTrace();
                    }
                }

                fout.close();

                if (!foundRows) {
                    // no rows found, delete file
                    boolean success = (new File(outFileName)).delete();
                    if (success) {
                        System.out.println("No rows found for this run, deleted " + outFileName);
                    } else {
                        System.out.println("No rows found for this run, BUT unable to delete " + outFileName);
                    }

                    // pause for 5 seconds
                    System.out.println("Pausing for 5 seconds...");
                    long pauseCurrentMillis = System.currentTimeMillis();
                    long pauseEndMillis = pauseCurrentMillis + (5l * 1000l);

                    while (pauseCurrentMillis < pauseEndMillis) {
                        pauseCurrentMillis = System.currentTimeMillis();
                    }
                } else if (!foundFullRowset) {
                    // none of the rowsets were full (amount requested)

                    // pause for given number seconds
                    System.out.printf("No full rowsets found, pausing for %d seconds...\n",loopPauseSeconds);
                    long pauseCurrentMillis = System.currentTimeMillis();
                    long pauseEndMillis = pauseCurrentMillis + (loopPauseSeconds * 1000l);

                    while (pauseCurrentMillis < pauseEndMillis) {
                        pauseCurrentMillis = System.currentTimeMillis();
                    }
                }

            } catch (IOException e) {
                  e.printStackTrace();
                  System.exit(-1);
            }

            currentTime = System.currentTimeMillis();
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

