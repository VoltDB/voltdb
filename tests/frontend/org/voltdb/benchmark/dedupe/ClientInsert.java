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
import java.util.Calendar;
import java.util.Date;

import org.voltdb.VoltTable;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

public class ClientInsert {
    public static long min_execution_milliseconds = 999999999l;
    public static long max_execution_milliseconds = -1l;
    public static long tot_execution_milliseconds = 0;
    public static long tot_executions = 0;
    public static long tot_executions_latency = 0;
    public static long[] latency_counter = new long[] {0,0,0,0,0,0,0,0,0};

    public static boolean checkLatency = false;

    static class AsyncCallback implements ProcedureCallback {
        @Override
        public synchronized void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();

            if (status != ClientResponse.SUCCESS) {
                System.err.println("Failed to execute!!!");
                System.err.println(clientResponse.getStatusString());
                System.err.println(clientResponse.getException());
                System.exit(-1);
            } else {
                tot_executions++;
                if (checkLatency) {
                    pClientCallback(clientResponse.getResults(), clientResponse.getClientRoundtrip());
                }
            }
        }

        protected void pClientCallback(VoltTable[] vtResults, int clientRoundtrip) {
            long called_time_milliseconds = vtResults[0].fetchRow(0).getLong(0);

            long current_time_milliseconds = System.currentTimeMillis();
            long execution_time = current_time_milliseconds - called_time_milliseconds;

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
        };
    }

    public static void main(String args[]) {
        //boolean backPressure = false;
        long transactions_per_second = Long.valueOf(args[0]);
        long transactions_per_milli = transactions_per_second / 1000l;
        long client_feedback_interval_secs = Long.valueOf(args[1]);
        long test_duration_secs = Long.valueOf(args[2]);
        int max_playerId = Integer.valueOf(args[3]);
        int max_gameId = Integer.valueOf(args[4]);
        long day_offset = Long.valueOf(args[5]);
        long lag_latency_seconds = Long.valueOf(args[6]);
        long lag_latency_millis = lag_latency_seconds * 1000l;
        long thisOutstanding = 0;
        long lastOutstanding = 0;
        String serverList = args[7];
        int outputToFile = Integer.valueOf(args[8]);
        String outputFileName = "log-ClientRandom.log";
        FileOutputStream foStatus;

        System.out.printf("Submitting %,d SP Calls/sec\n",transactions_per_second);
        System.out.printf("Feedback interval = %,d second(s)\n",client_feedback_interval_secs);
        System.out.printf("Running for %,d second(s)\n",test_duration_secs);
        System.out.printf("Max playerId = %,d\n",max_playerId);
        System.out.printf("Max gameId = %,d\n",max_gameId);
        System.out.printf("Offsetting insert timestamps by %d day(s)\n",day_offset);
        System.out.printf("Latency not recorded for %d second(s)\n",lag_latency_seconds);
        if (outputToFile == 1) {
            System.out.printf("Outputting to file %s\n",outputFileName);
        } else {
            System.out.printf("File output disabled.\n");
        }

        long playerId;
        long gameId;
        long socialId = 1l;
        long clientId = 1l;
        long visitTimeMillis;

        long transactions_this_second = 0;
        long last_millisecond = System.currentTimeMillis();
        long this_millisecond = System.currentTimeMillis();

        long milliseconds_offset = day_offset * 1000l * 60 * 60 * 24;

        AsyncCallback callBack = new AsyncCallback();

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

        // make random object totally random (set my milliseconds) so we can have multiple clients running simultaneously
        java.util.Random rand = new java.util.Random();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (1000l * test_duration_secs);
        long currentTime = startTime;
        long lastFeedbackTime = startTime;
        long num_sp_calls = 0;
        long startRecordingLatency = startTime + lag_latency_millis;

        int hourOfDay = Calendar.getInstance().get(Calendar.HOUR_OF_DAY) + 1;
        int minuteOfHour = Calendar.getInstance().get(Calendar.MINUTE) + 1;
        long nextHourCheck = -1;
        int timeLimitedMaxPlayerId = 1;

        while (endTime > currentTime) {
            num_sp_calls++;

            if (currentTime > nextHourCheck) {
                // check the hour of the day once per minute
                hourOfDay = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
                minuteOfHour = Calendar.getInstance().get(Calendar.MINUTE);
                nextHourCheck = currentTime + (1000l * 60l);

                if (day_offset > 0) {
                    // all player IDs allowed in system, backfilling yesterday data
                    timeLimitedMaxPlayerId = max_playerId;

                    System.out.printf("day_offset > 0, player pool governer disabled : hourLimitedMaxPlayerId = %d\n",timeLimitedMaxPlayerId);
                } else {
                    // only allow 5% of total player pool per hour to enter the system
                    timeLimitedMaxPlayerId = (int) (((hourOfDay + (minuteOfHour / 60.0)) * 5.0 / 100.0) * max_playerId);
                    if (timeLimitedMaxPlayerId > max_playerId) {
                        timeLimitedMaxPlayerId = max_playerId;
                    } else if (timeLimitedMaxPlayerId < 1) {
                        timeLimitedMaxPlayerId = 1;
                    }

                    System.out.printf("hourOfDay = %d : minuteOfHour = %d : hourLimitedMaxPlayerId = %d\n",hourOfDay,minuteOfHour,timeLimitedMaxPlayerId);

                }
            }

            playerId = rand.nextInt(timeLimitedMaxPlayerId);
            gameId = rand.nextInt(max_gameId);
            socialId = 1;
            clientId = 1l;
            visitTimeMillis = System.currentTimeMillis();

            try {
                boolean queued = false;
                while (!queued) {
                    queued = voltclient.callProcedure(callBack, "RecordHit", playerId, gameId, socialId, clientId, (visitTimeMillis - milliseconds_offset), visitTimeMillis);

                    if (!queued) {
                        try {
                            voltclient.backpressureBarrier();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
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
                synchronized(callBack) {
                    lastFeedbackTime = currentTime;

                    long elapsedTimeMillis2 = System.currentTimeMillis()-startTime;
                    float elapsedTimeSec2 = elapsedTimeMillis2/1000F;

                    if (tot_executions_latency == 0) {
                        tot_executions_latency = 1;
                    }
                    thisOutstanding = num_sp_calls - tot_executions;

                    long runTimeMillis = endTime - startTime;

                    double percentComplete = ((double) elapsedTimeMillis2 / (double) runTimeMillis) * 100;
                    if (percentComplete > 100.0) {
                        percentComplete = 100.0;
                    }

                    String currentDate = new Date().toString();
                    System.out.printf("[%s] %.3f%% Complete | SP Calls: %,d at %,.2f SP/sec | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f\n",currentDate, percentComplete, num_sp_calls, (num_sp_calls / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, ((double) tot_execution_milliseconds / (double) tot_executions_latency));
                    if (outputToFile == 1) {
                        try {
                            foStatus = new FileOutputStream(outputFileName,true);
                            new PrintStream(foStatus).printf("[%s] %.3f%% Complete | SP Calls: %,d at %,.2f SP/sec | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f\n",currentDate, percentComplete, num_sp_calls, (num_sp_calls / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, ((double) tot_execution_milliseconds / (double) tot_executions_latency));
                            foStatus.close();
                        } catch (IOException e) {
                              e.printStackTrace();
                              System.exit(-1);
                        }
                    }

                    lastOutstanding = thisOutstanding;
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
        System.out.println("Checking results");
        System.out.println("*************************************************************************");

        System.out.printf(" - System ran for %12.4f seconds\n",elapsedTimeSec);
        System.out.printf(" - Performed %,d SP calls\n",num_sp_calls);
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
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

