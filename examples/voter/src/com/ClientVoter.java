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
    public static final short[] areaCodes = new short[]{907,205,256,334,251,870,501,479,480,602,623,928,520,341,764,628,831,925,909,562,661,510,650,949,760,415,951,209,669,408,559,626,442,530,916,627,714,707,310,323,213,424,747,818,858,935,619,805,369,720,303,970,719,860,203,959,475,202,302,689,407,239,850,727,321,754,954,927,352,863,386,904,561,772,786,305,941,813,478,770,470,404,762,706,678,912,229,808,515,319,563,641,712,208,217,872,312,773,464,708,224,847,779,815,618,309,331,630,317,765,574,260,219,812,913,785,316,620,606,859,502,270,504,985,225,318,337,774,508,339,781,857,617,978,351,413,443,410,301,240,207,517,810,278,679,313,586,947,248,734,269,989,906,616,231,612,320,651,763,952,218,507,636,660,975,816,573,314,557,417,769,601,662,228,406,336,252,984,919,980,910,828,704,701,402,308,603,908,848,732,551,201,862,973,609,856,575,957,505,775,702,315,518,646,347,212,718,516,917,845,631,716,585,607,914,216,330,234,567,419,440,380,740,614,283,513,937,918,580,405,503,541,971,814,717,570,878,835,484,610,267,215,724,412,401,843,864,803,605,423,865,931,615,901,731,254,325,713,940,817,430,903,806,737,512,361,210,979,936,409,972,469,214,682,832,281,830,956,432,915,435,801,385,434,804,757,703,571,276,236,540,802,509,360,564,206,425,253,715,920,262,414,608,304,307};
    public static final String[] states = new String[] {"AK","AL","AL","AL","AL","AR","AR","AR","AZ","AZ","AZ","AZ","AZ","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CA","CO","CO","CO","CO","CT","CT","CT","CT","DC","DE","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","FL","GA","GA","GA","GA","GA","GA","GA","GA","GA","HI","IA","IA","IA","IA","IA","ID","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IL","IN","IN","IN","IN","IN","IN","KS","KS","KS","KS","KY","KY","KY","KY","LA","LA","LA","LA","LA","MA","MA","MA","MA","MA","MA","MA","MA","MA","MD","MD","MD","MD","ME","MI","MI","MI","MI","MI","MI","MI","MI","MI","MI","MI","MI","MI","MI","MN","MN","MN","MN","MN","MN","MN","MO","MO","MO","MO","MO","MO","MO","MO","MS","MS","MS","MS","MT","NC","NC","NC","NC","NC","NC","NC","NC","ND","NE","NE","NH","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NJ","NM","NM","NM","NV","NV","NY","NY","NY","NY","NY","NY","NY","NY","NY","NY","NY","NY","NY","NY","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH","OH","OK","OK","OK","OR","OR","OR","PA","PA","PA","PA","PA","PA","PA","PA","PA","PA","PA","RI","SC","SC","SC","SD","TN","TN","TN","TN","TN","TN","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","TX","UT","UT","UT","VA","VA","VA","VA","VA","VA","VA","VA","VT","WA","WA","WA","WA","WA","WA","WI","WI","WI","WI","WI","WV","WY"};
    public static final short[] votingMap = new short[areaCodes.length];
    public static long min_execution_milliseconds = 999999999l;
    public static long max_execution_milliseconds = -1l;
    public static long tot_execution_milliseconds = 0;
    public static long tot_executions = 0;
    public static long tot_executions_latency = 0;
    public static long[] latency_counter = new long[] {0,0,0,0,0,0,0,0,0};

    public static long cycle_min_execution_milliseconds = 999999999l;
    public static long cycle_max_execution_milliseconds = -1l;
    public static long cycle_tot_execution_milliseconds = 0;
    public static long cycle_tot_executions_latency = 0;

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

                        cycle_tot_executions_latency++;
                        cycle_tot_execution_milliseconds += execution_time;

                        if (execution_time < min_execution_milliseconds) {
                            min_execution_milliseconds = execution_time;
                        }
                        if (execution_time < cycle_min_execution_milliseconds) {
                            cycle_min_execution_milliseconds = execution_time;
                        }

                        if (execution_time > max_execution_milliseconds) {
                            max_execution_milliseconds = execution_time;
                        }
                        if (execution_time > cycle_max_execution_milliseconds) {
                            cycle_max_execution_milliseconds = execution_time;
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
        if (args.length != 11) {
            System.err.println("ClientVoter [number of contestants] [votes per phone number] " +
                    "[transactions per second] [client feedback interval (seconds)] " +
                    "[test duration (seconds)] [lag record delay (seconds)] [server list (comma separated)] [auto-tuning] [target latency ms] [adjustment rate] [adjustment interval]");
            System.exit(1);
        }

        int maxContestant = Integer.valueOf(args[0]);
        if ((maxContestant < 1) || (maxContestant > 12)) {
            System.err.println("Number of contestants must be between 1 and 12");
            System.exit(1);
        }

        long maxVotesPerPhoneNumber = Long.valueOf(args[1]);
        long transactions_per_second_requested = Long.valueOf(args[2]);
        long transactions_per_second = transactions_per_second_requested;
        long transactions_per_milli = transactions_per_second / 1000l;
        long client_feedback_interval_secs = Long.valueOf(args[3]);
        long test_duration_secs = Long.valueOf(args[4]);
        long lag_latency_seconds = Long.valueOf(args[5]);
        String serverList = args[6];
        long lag_latency_millis = lag_latency_seconds * 1000l;
        long thisOutstanding = 0;
        long lastOutstanding = 0;

        boolean use_auto_tuning = Boolean.valueOf(args[7]) && (transactions_per_second > 1000);
        double auto_tuning_target_latency_millis = Double.valueOf(args[8]);
        double auto_tuning_adjustment_rate = Double.valueOf(args[9]);
        if (auto_tuning_adjustment_rate > 1.0) {
            auto_tuning_adjustment_rate = auto_tuning_adjustment_rate / 100.0;
        }
        long auto_tuning_interval_secs = Long.valueOf(args[10]);

        String contestantNameList = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway,Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster,Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
        String[] contestantNames = contestantNameList.split(",");

        System.out.printf("Allowing %d votes per phone number\n",maxVotesPerPhoneNumber);
        System.out.printf("Submitting %,d Transactions/sec (TPS)\n",transactions_per_second);
        System.out.printf("Feedback interval = %,d second(s)\n",client_feedback_interval_secs);
        System.out.printf("Running for %,d second(s)\n",test_duration_secs);
        System.out.printf("Latency not recorded for %d second(s)\n",lag_latency_seconds);
        if (use_auto_tuning) {
            System.out.println("Auto-Tuning = ON");
            System.out.printf(" - Tuning interval = %,d second(s)\n", auto_tuning_interval_secs);
            System.out.printf(" - Target latency = %.2f ms\n", auto_tuning_target_latency_millis);
            System.out.printf(" - Adjustment rate = %.2f%%\n", auto_tuning_adjustment_rate * 100.0);
        }

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
                voltclient.callProcedure("Initialize", maxContestant, contestantNameList, areaCodes, states).getResults();
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
        long lastAutoTuningTime = startTime;
        long num_sp_calls = 0;
        long cycle_num_sp_calls = 0;
        long startRecordingLatency = startTime + lag_latency_millis;
        int callIdx = 0;

        AsyncCallback callBack = new AsyncCallback();

        for(int i=0;i<votingMap.length;i++)
            votingMap[i] = (short)(((rand.nextInt(maxContestant) * rand.nextInt(maxContestant) + (int)Math.floor(100*Math.sin((double)i))) % maxContestant) + 1);

        while (endTime > currentTime) {
            num_sp_calls++;
            cycle_num_sp_calls++;
            callIdx = (rand.nextInt(areaCodes.length) * rand.nextInt(areaCodes.length)) % areaCodes.length;
            phoneNumber = areaCodes[callIdx]*10000000l + (Math.abs(rand.nextLong()) % 9999999l);
            contestantNumber = (byte)(rand.nextInt(100) > 40 ? votingMap[callIdx] : ((rand.nextInt(maxContestant) % maxContestant) + 1));
            if ((num_sp_calls % 100) == 0) {
                // introduce a potentially bad contestant number every 100 votes
                contestantNumber = (byte)((rand.nextInt(maxContestant) + 1) * 2);
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

            if (use_auto_tuning && (currentTime >= (lastAutoTuningTime + (auto_tuning_interval_secs * 1000)))) {

                long cycle_elapsedTimeMillis2 = System.currentTimeMillis()-lastAutoTuningTime;
                float cycle_elapsedTimeSec2 = cycle_elapsedTimeMillis2/1000F;
                lastAutoTuningTime = currentTime;

                counterLock.lock();
                try {
                    // Only adjust if both the cycle and total was below request - avoid *some* random spike from downgrading the system too much
                    if ((((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency) > auto_tuning_target_latency_millis) && (((double) tot_execution_milliseconds / (double) tot_executions_latency) > auto_tuning_target_latency_millis))
                    {
                        long new_transactions_per_second = (((long)(Math.min(cycle_num_sp_calls / cycle_elapsedTimeSec2,transactions_per_second) * auto_tuning_adjustment_rate))/1000l)*1000l;
                        String last_tuning_warning = "";
                        if ((new_transactions_per_second <= 1000) || (new_transactions_per_second == transactions_per_second)) {
                            use_auto_tuning = false;
                            last_tuning_warning = " | WARNING: Minimum load boundary reached.";
                        }

                        System.out.printf("Auto-Tuning | Observed: %,.2f TPS | Latency: min = %d | max = %d | avg = %.2f | Adjusting DOWN: %,d TPS%s\n"
                                         , (cycle_num_sp_calls / cycle_elapsedTimeSec2)
                                         , cycle_min_execution_milliseconds
                                         , cycle_max_execution_milliseconds
                                         , ((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency)
                                         , new_transactions_per_second
                                         , last_tuning_warning
                                         );
                        transactions_per_second = new_transactions_per_second;
                        transactions_per_milli = transactions_per_second/1000l;
                    }
                    else if (((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency) < 0.9d*auto_tuning_target_latency_millis)
                    {
                        long new_transactions_per_second = ((long)Math.max(1.05d*transactions_per_second, (double)(cycle_num_sp_calls / cycle_elapsedTimeSec2))/1000l) *1000l;
                        if (new_transactions_per_second > transactions_per_second_requested)
                            new_transactions_per_second = transactions_per_second_requested;
                        if (new_transactions_per_second > transactions_per_second)
                        {
                            System.out.printf("Auto-Tuning | Observed: %,.2f TPS | Latency: min = %d | max = %d | avg = %.2f | Adjusting UP: %,d TPS\n"
                                             , (cycle_num_sp_calls / cycle_elapsedTimeSec2)
                                             , cycle_min_execution_milliseconds
                                             , cycle_max_execution_milliseconds
                                             , ((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency)
                                             , new_transactions_per_second
                                             );
                            transactions_per_second = new_transactions_per_second;
                            transactions_per_milli = transactions_per_second/1000l;
                        }
                    }
                    cycle_num_sp_calls = 0;

                    cycle_min_execution_milliseconds = 999999999l;
                    cycle_max_execution_milliseconds = -1l;
                    cycle_tot_execution_milliseconds = 0;
                    cycle_tot_executions_latency = 0;
                } finally {
                    counterLock.unlock();
                }
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

                    System.out.printf("%.3f%% Complete | Transactions: %,d at %,.2f TPS | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f\n",percentComplete, num_sp_calls, (num_sp_calls / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, avgLatency);

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
        int winnerId = 0;

        try {
            VoltTable[] vtResults = voltclient.callProcedure("Results").getResults();

            int rowCount = vtResults[0].getRowCount();
            if (rowCount == 0) {
                System.out.println(" - No results to report.");
            } else {
                for (int ii = 0; ii < rowCount; ii++) {
                    VoltTableRow row = vtResults[0].fetchRow(ii);
                    String resultName = row.getString(0);
                    winnerId = (int)row.getLong(1);
                    long resultVotes = row.getLong(2);
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
        if (transactions_per_second < transactions_per_second_requested) {
            System.out.println("*************************************************************************");
            System.out.println("Auto-Tuning Results");
            System.out.println("*************************************************************************");
            System.out.printf(" - Optimal Load: %,d TPS to match/approach desired %.2f ms Latency\n", transactions_per_second, auto_tuning_target_latency_millis);
        }

        try {
            voltclient.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

