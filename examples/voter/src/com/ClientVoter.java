/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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


// Client application for "Voter" sample application
//
//   Connects to server and creates random votes.

package com;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

public class ClientVoter {
    public static long min_execution_milliseconds = 999999999l;
    public static long max_execution_milliseconds = -1l;
    public static long tot_execution_milliseconds = 0;
    public static long tot_executions = 0;
    public static long tot_executions_latency = 0;
    public static long[] latency_counter = new long[] {0,0,0,0,0,0,0,0,0};

    public static long[] vote_result_counter = new long[] {0,0,0};

    public static boolean checkLatency = false;

    public static final ReentrantLock counterLock = new ReentrantLock();
    static class AsyncCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();
            if (status != ClientResponse.SUCCESS) {
                System.err.println("Failed to execute!!!");
                System.err.println(clientResponse.getStatusString());
                System.err.println(clientResponse.getException());
                System.exit(-1);
            } else {
                counterLock.lock();
                try {
                    tot_executions++;
                    VoltTable vtResults[] = clientResponse.getResults();
                    int vote_result = (int) vtResults[0].fetchRow(0).getLong(0);

                    vote_result_counter[vote_result]++;

                    if (checkLatency) {
                        long execution_time =  clientResponse.getClientRoundtrip();

                        tot_executions_latency++;
                        tot_execution_milliseconds += execution_time;

                        if (execution_time < min_execution_milliseconds) {
                            min_execution_milliseconds = execution_time;
                        }

                        if (execution_time > max_execution_milliseconds) {
                            max_execution_milliseconds = execution_time;
                        }

                        // change latency to bucket
                        int latency_bucket = (int) (execution_time / 25l);
                        if (latency_bucket > 8) {
                            latency_bucket = 8;
                        }
                        latency_counter[latency_bucket]++;
                    }
                } finally {
                    counterLock.unlock();
                }
            }
        }
    }


    public static void main(String args[]) {
        if (args.length != 7) {
            System.err.println("ClientVoter [number of contestants] [votes per phone number] " +
                    "[transactions per second] [client feedback interval (seconds)] " +
                    "[test duration (seconds)] [lag record delay (seconds)] [server list (comma separated)]");
            System.exit(1);
        }

        int maxContestant = Integer.valueOf(args[0]);
        if ((maxContestant < 1) || (maxContestant > 12)) {
            System.err.println("Number of contestants must be between 1 and 12");
            System.exit(1);
        }

        long maxVotesPerPhoneNumber = Long.valueOf(args[1]);
        long transactions_per_second = Long.valueOf(args[2]);
        long transactions_per_milli = transactions_per_second / 1000l;
        long client_feedback_interval_secs = Long.valueOf(args[3]);
        long test_duration_secs = Long.valueOf(args[4]);
        long lag_latency_seconds = Long.valueOf(args[5]);
        String serverList = args[6];
        long lag_latency_millis = lag_latency_seconds * 1000l;
        long thisOutstanding = 0;
        long lastOutstanding = 0;

        String[] contestantNames = {"Edwina Burnam",
                                    "Tabatha Gehling",
                                    "Kelly Clauss",
                                    "Jessie Alloway",
                                    "Alana Bregman",
                                    "Jessie Eichman",
                                    "Allie Rogalski",
                                    "Nita Coster",
                                    "Kurt Walser",
                                    "Ericka Dieter",
                                    "Loraine Nygren",
                                    "Tania Mattioli"};

        System.out.printf("Allowing %d votes per phone number\n",maxVotesPerPhoneNumber);
        System.out.printf("Submitting %,d SP Calls/sec\n",transactions_per_second);
        System.out.printf("Feedback interval = %,d second(s)\n",client_feedback_interval_secs);
        System.out.printf("Running for %,d second(s)\n",test_duration_secs);
        System.out.printf("Latency not recorded for %d second(s)\n",lag_latency_seconds);

        long phoneNumber;
        byte contestantNumber;

        long transactions_this_second = 0;
        long last_millisecond = System.currentTimeMillis();
        long this_millisecond = System.currentTimeMillis();

        ClientConfig config = new ClientConfig("program", "none");
        /*
         * Allow a large number of outstanding txns in order to be able to drive a large
         * cluster. Config must be modified before instantiating the client.
         */
        config.setMaxOutstandingTxns(6000);
        final org.voltdb.client.Client voltclient = ClientFactory.createClient(config);

        String[] voltServers = serverList.split(",");

        for (String thisServer : voltServers) {
            try {
                thisServer = thisServer.trim();
                System.out.printf("Connecting to server: '%s'\n",thisServer);

                voltclient.createConnection(thisServer);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        try {
            /*
             * Initialize the database if this is the first connecting client,
             *  otherwise get existing configuration information
             */
            VoltTable[] vtInitialize =
                voltclient.callProcedure("Initialize", maxContestant, contestantNames).getResults();
            maxContestant = (int) vtInitialize[0].fetchRow(0).getLong(0);
            System.out.printf("Running for %d contestant(s)\n",maxContestant);
        } catch (ProcCallException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        java.util.Random rand = new java.util.Random();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (1000l * test_duration_secs);
        long currentTime = startTime;
        long lastFeedbackTime = startTime;
        long num_sp_calls = 0;
        long startRecordingLatency = startTime + lag_latency_millis;

        AsyncCallback callBack = new AsyncCallback();

        while (endTime > currentTime) {
            num_sp_calls++;

            phoneNumber = rand.nextLong() % 9999999999l;
            contestantNumber = (byte) (((rand.nextInt(maxContestant) * rand.nextInt(maxContestant)) % maxContestant) + 1);
            if ((num_sp_calls % 100) == 0) {
                // introduce a potentially bad contestant number every 100 votes
                contestantNumber = (byte) ((rand.nextInt(maxContestant) + 1) * 2);
            }

            try {
                voltclient.callProcedure(callBack, "Vote", phoneNumber, contestantNumber, maxVotesPerPhoneNumber);
            } catch (java.io.IOException e) {
                e.printStackTrace();
                if (e instanceof NoConnectionsException) {
                    System.exit(-1);
                }
            }

            transactions_this_second++;
            if (transactions_this_second >= transactions_per_milli) {
                this_millisecond = System.currentTimeMillis();
                while (this_millisecond <= last_millisecond) {
                    this_millisecond = System.currentTimeMillis();
                }
                last_millisecond = this_millisecond;
                transactions_this_second = 0;
            }

            currentTime = System.currentTimeMillis();

            if ((!checkLatency) && (currentTime >= startRecordingLatency)) {
                // time to start recording latency information
                checkLatency = true;
            }

            if (currentTime >= (lastFeedbackTime + (client_feedback_interval_secs * 1000))) {
                final long elapsedTimeMillis2 = System.currentTimeMillis()-startTime;
                lastFeedbackTime = currentTime;

                final long runTimeMillis = endTime - startTime;

                float elapsedTimeSec2 = elapsedTimeMillis2/1000F;
                if (tot_executions_latency == 0) {
                    tot_executions_latency = 1;
                }

                double percentComplete = ((double) elapsedTimeMillis2 / (double) runTimeMillis) * 100;
                if (percentComplete > 100.0) {
                    percentComplete = 100.0;
                }

                counterLock.lock();
                try {
                    thisOutstanding = num_sp_calls - tot_executions;

                    double avgLatency = (double) tot_execution_milliseconds / (double) tot_executions_latency;

                    System.out.printf("%.3f%% Complete | SP Calls: %,d at %,.2f SP/sec | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f\n",percentComplete, num_sp_calls, (num_sp_calls / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, avgLatency);

                    lastOutstanding = thisOutstanding;
                } finally {
                    counterLock.unlock();
                }
            }
        }

        try {
            voltclient.drain();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        long elapsedTimeMillis = System.currentTimeMillis()-startTime;
        float elapsedTimeSec = elapsedTimeMillis/1000F;

        System.out.println("\n");
        System.out.println("*************************************************************************");
        System.out.println("Voting Results");
        System.out.println("*************************************************************************");

        System.out.printf(" - Accepted votes = %,d\n",vote_result_counter[0]);
        System.out.printf(" - Rejected votes (invalid contestant) = %,d\n",vote_result_counter[1]);
        System.out.printf(" - Rejected votes (voter over limit) = %,d\n\n",vote_result_counter[2]);

        String winnerName = "<<UNKNOWN>>";

        try {
            VoltTable[] vtResults = voltclient.callProcedure("Results").getResults();

            int rowCount = vtResults[0].getRowCount();
            if (rowCount == 0) {
                System.out.println(" - No results to report.");
            } else {
                for (int ii = 0; ii < rowCount; ii++) {
                    VoltTableRow row = vtResults[0].fetchRow(ii);
                    String resultName = row.getString(0);
                    long resultVotes = row.getLong(1);
                    System.out.printf(" - Contestant %s received %,d vote(s)\n",resultName,resultVotes);
                    winnerName = resultName;
                }
            }
        } catch (ProcCallException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (java.io.IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.printf("\n - Contestant %s was the winner.\n",winnerName);


        System.out.println("\n");
        System.out.println("*************************************************************************");
        System.out.println("System Statistics");
        System.out.println("*************************************************************************");

        System.out.printf(" - Ran for %,.2f seconds\n",elapsedTimeSec);
        System.out.printf(" - Performed %,d Stored Procedure calls\n",num_sp_calls);
        System.out.printf(" - At %,.2f calls per second\n",num_sp_calls / elapsedTimeSec);
        System.out.printf(" - Average Latency = %.2f ms\n",((double) tot_execution_milliseconds / (double) tot_executions_latency));
        System.out.printf(" -   Latency   0ms -  25ms = %,d\n",latency_counter[0]);
        System.out.printf(" -   Latency  25ms -  50ms = %,d\n",latency_counter[1]);
        System.out.printf(" -   Latency  50ms -  75ms = %,d\n",latency_counter[2]);
        System.out.printf(" -   Latency  75ms - 100ms = %,d\n",latency_counter[3]);
        System.out.printf(" -   Latency 100ms - 125ms = %,d\n",latency_counter[4]);
        System.out.printf(" -   Latency 125ms - 150ms = %,d\n",latency_counter[5]);
        System.out.printf(" -   Latency 150ms - 175ms = %,d\n",latency_counter[6]);
        System.out.printf(" -   Latency 175ms - 200ms = %,d\n",latency_counter[7]);
        System.out.printf(" -   Latency 200ms+        = %,d\n",latency_counter[8]);

        try {
            voltclient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

