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

import org.apache.log4j.Logger;

public class ClientDelete {
    public static final Logger m_logger = Logger.getLogger(ClientDelete.class.getName());

    public static void main(String args[]) {
        long numDeletes = (long) Long.valueOf(args[0]);
        String serverList = args[1];

        m_logger.info(String.format("Executing %,d deletes per transaction",numDeletes));

        int num_partitions = 0;

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
  
        long startTime = System.currentTimeMillis();

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
                    m_logger.info(String.format("[%s] Ran delete on partition %d : deleted %,d row(s) in %,d milliseconds",currentDate,longCounter,rowCount,(callTimeEnd-callTimeBegin)));

                    if (rowCount > 0) {
                        foundRows = true;
                    }
                } catch (ProcCallException e) {
                    m_logger.error("ProcCallException:");
                    m_logger.error(e.toString());
                } catch (IOException e) {
                    m_logger.error("IOException:");
                    m_logger.error(e.toString());
                    System.exit(-1);
                }
            }
        }

        String currentDate2 = new Date().toString();
        m_logger.info(String.format("[%s] Finished deletion job, shutting down.",currentDate2));

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

