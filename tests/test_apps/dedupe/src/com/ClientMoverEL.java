package com;

import org.voltdb.client.ClientFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import java.util.Date;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.File;

import org.apache.log4j.Logger;

public class ClientMoverEL {
    public static final Logger m_logger = Logger.getLogger(ClientDelete.class.getName());

    public static void main(String args[]) {
        long numMoves = (long) Long.valueOf(args[0]);
        String serverList = args[1];
        long clientDurationSeconds = (long) Long.valueOf(args[2]);
        long loopPauseSeconds = (long) Long.valueOf(args[3]);

        m_logger.info(String.format("Executing %,d moves per transaction",numMoves));
        m_logger.info(String.format("Running for %,d second(s)",clientDurationSeconds));

        int num_partitions = 0;

        long playerId;
        long gameId;
        long socialId;
        long clientId;
        long visitTime;

        int intCounter;
        long longCounter;

        final org.voltdb.client.Client voltclient = ClientFactory.createClient();
  
        String[] voltServers = serverList.split(",");
  
        for (String thisServer : voltServers) {
            try {
                thisServer = thisServer.trim();
                m_logger.info(String.format("Connecting to server: %s",thisServer));
                voltclient.createConnection(thisServer, "program", "none");
            } catch (IOException e) {
                m_logger.error(e.toString());
                System.exit(-1);
            }
        }
  
        java.util.Random rand = new java.util.Random(0);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (1000l * clientDurationSeconds);
        long currentTime = startTime;

        // get the # of partitions in my cluster
        try {
            VoltTable vtPartitionInfo[] = voltclient.callProcedure("@Statistics","partitioncount",0l).getResults();
            num_partitions = (int) vtPartitionInfo[0].fetchRow(0).getLong(0);
            m_logger.info("System is running with " + num_partitions + " partition(s).");
        } catch (ProcCallException e) {
            m_logger.error("ProcCallException:");
            m_logger.error(e.toString());
        } catch (IOException e) {
            m_logger.error("IOException:");
            m_logger.error(e.toString());
            System.exit(-1);
        }

        boolean foundRows = true;
        boolean foundFullRowset = false;

        while (endTime > currentTime) {
            // do a single archive at each partition
            foundRows = false;
            foundFullRowset = false;

            long callTimeMillis = System.currentTimeMillis();

            try {
                for (longCounter = 0; longCounter < num_partitions; longCounter++) {
                    try {
                        long callTimeBegin = System.currentTimeMillis();
                        VoltTable vtArchiveVisits[] = voltclient.callProcedure("ArchiveVisitsEL", longCounter, numMoves, callTimeMillis).getResults();
                        long callTimeEnd = System.currentTimeMillis();
                        int rowCount = (int) vtArchiveVisits[0].fetchRow(0).getLong(1);

                        if (rowCount > 0) {
                            // write out the rows
                            foundRows = true;

                            if (rowCount == numMoves) {
                                foundFullRowset = true;
                            }
                        }

                        String currentDate = new Date().toString();
                        m_logger.info(String.format("[%s] Ran archive on partition %d : archived %,d row(s) in %,d milliseconds",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin)));

                    } catch (ProcCallException e) {
                        m_logger.error("ProcCallException:");
                        m_logger.error(e.toString());
                    } catch (NoConnectionsException e) {
                        m_logger.error("IOException:");
                        m_logger.error(e.toString());
                        System.exit(-1);
                    }
                }

                if (!foundRows) {
                    // no rows found
                    m_logger.info("No rows found for this run.");

                    // pause for 5 seconds
                    m_logger.info(String.format("Pausing for %d seconds...",loopPauseSeconds));
                    long pauseCurrentMillis = System.currentTimeMillis();
                    long pauseEndMillis = pauseCurrentMillis + (loopPauseSeconds * 1000l);

                    while (pauseCurrentMillis < pauseEndMillis) {
                        pauseCurrentMillis = System.currentTimeMillis();
                    }
                } else if (!foundFullRowset) {
                    // none of the rowsets were full (amount requested)

                    // pause for given number seconds
                    m_logger.info(String.format("No full rowsets found, pausing for %d seconds...",loopPauseSeconds));
                    long pauseCurrentMillis = System.currentTimeMillis();
                    long pauseEndMillis = pauseCurrentMillis + (loopPauseSeconds * 1000l);

                    while (pauseCurrentMillis < pauseEndMillis) {
                        pauseCurrentMillis = System.currentTimeMillis();
                    }
                }

            } catch (IOException e) {
                  m_logger.error(e.toString());
                  System.exit(-1);
            }

            currentTime = System.currentTimeMillis();
        }

        try {
            voltclient.drain();
        } catch (InterruptedException e) {
            m_logger.error(e.toString());
            System.exit(-1);
        } catch (NoConnectionsException e) {
            m_logger.error(e.toString());
            System.exit(-1);
        }

        try {
            voltclient.close();
        } catch (Exception e) {
            m_logger.error(e.toString());
            System.exit(-1);
        }
    }
}

