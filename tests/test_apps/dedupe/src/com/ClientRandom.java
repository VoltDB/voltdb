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

package com;

import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NullCallback;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.NoConnectionsException;

import java.util.Date;
import java.util.ArrayList;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.apache.log4j.Logger;

public class ClientRandom {
    public static long min_execution_milliseconds = 999999999l;
    public static long max_execution_milliseconds = -1l;
    public static long tot_execution_milliseconds = 0;
    public static long tot_executions = 0;
    public static long tot_executions_latency = 0;
    public static long[] latency_counter = new long[] {0,0,0,0,0,0,0,0,0};

    public static final Logger m_logger = Logger.getLogger(ClientRandom.class.getName());

    public static boolean checkLatency = false;

    static class AsyncCallback implements ProcedureCallback {
        @Override
        public synchronized void clientCallback(ClientResponse clientResponse) {
            final byte status = clientResponse.getStatus();
            FileOutputStream foStatus;

            if (status != ClientResponse.SUCCESS) {
                m_logger.error("***AsyncCallback Issue : Failed to execute!!!");
                m_logger.error(clientResponse.getStatusString());
                m_logger.error(clientResponse.getException());
            } else {
                tot_executions++;
                if (checkLatency) {
                    pClientCallback(clientResponse.getResults(), clientResponse.getClientRoundtrip());
                }
            }
        }

        protected void pClientCallback(VoltTable[] vtResults, int clientRoundtrip) {
            long execution_time = (long) clientRoundtrip;

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
        boolean backPressure = false;
        long transactions_per_second = (long) Long.valueOf(args[0]);
        long transactions_per_milli = transactions_per_second / 1000l;
        long client_feedback_interval_secs = (long) Long.valueOf(args[1]);
        long test_duration_secs = (long) Long.valueOf(args[2]);
        int max_playerId = (int) Integer.valueOf(args[3]);
        int max_field1 = (int) Integer.valueOf(args[4]);
        long day_offset = (long) Long.valueOf(args[5]);
        long lag_latency_seconds = (long) Long.valueOf(args[6]);
        long lag_latency_millis = lag_latency_seconds * 1000l;
        long thisOutstanding = 0;
        long lastOutstanding = 0;
        String serverList = args[7];
        long reset_latency_seconds = (long) Long.valueOf(args[8]);
        long reset_latency_millis = reset_latency_seconds * 1000l;


        m_logger.info(String.format("Submitting %,d SP Calls/sec",transactions_per_second));
        m_logger.info(String.format("Feedback interval = %,d second(s)",client_feedback_interval_secs));
        m_logger.info(String.format("Running for %,d second(s)",test_duration_secs));
        m_logger.info(String.format("Max playerId = %,d",max_playerId));
        m_logger.info(String.format("Max field1 = %,d",max_field1));
        m_logger.info(String.format("Offsetting insert timestamps by %d day(s)",day_offset));
        m_logger.info(String.format("Latency not recorded for %d second(s)",lag_latency_seconds));
        m_logger.info(String.format("Resetting min/max/avg latency every %d second(s)",reset_latency_seconds));

        long playerId;
        long field1;
        long field2 = 1l;
        long field3 = 1l;
        long visitTimeMillis;

        long transactions_this_second = 0;
        long last_millisecond = System.currentTimeMillis();
        long this_millisecond = System.currentTimeMillis();

        long milliseconds_offset = day_offset * 1000l * 60 * 60 * 24;

        AsyncCallback callBack = new AsyncCallback();

        // create generic client
        final org.voltdb.client.Client voltclient = ClientFactory.createClient();
        // create HEAVYWEIGHT client with expected message size of 128 bytes
//        final org.voltdb.client.Client voltclient = ClientFactory.createClient(128,null,true,null);

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

        // make random object totally random (set my milliseconds) so we can have multiple clients running simultaneously
        java.util.Random rand = new java.util.Random();

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (1000l * test_duration_secs);
        long currentTime = startTime;
        long lastFeedbackTime = startTime;
        long num_sp_calls = 0;
        long startRecordingLatency = startTime + lag_latency_millis;
        long nextLatencyReset = startTime + reset_latency_millis;

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

                    m_logger.info(String.format("day_offset > 0, player pool governer disabled : hourLimitedMaxPlayerId = %d",timeLimitedMaxPlayerId));
                } else {
                    // only allow 5% of total player pool per hour to enter the system
                    timeLimitedMaxPlayerId = (int) ((((double) hourOfDay + ((double) minuteOfHour / 60.0)) * 5.0 / 100.0) * (double) max_playerId);
                    if (timeLimitedMaxPlayerId > max_playerId) {
                        timeLimitedMaxPlayerId = max_playerId;
                    } else if (timeLimitedMaxPlayerId < 1) {
                        timeLimitedMaxPlayerId = 1;
                    }

                    m_logger.info(String.format("hourOfDay = %d : minuteOfHour = %d : hourLimitedMaxPlayerId = %d",hourOfDay,minuteOfHour,timeLimitedMaxPlayerId));
                }
            }

            playerId = (long) rand.nextInt(timeLimitedMaxPlayerId);
            field1 = (long) rand.nextInt(max_field1);
            field2 = 1l;
            field3 = 1l;
            visitTimeMillis = System.currentTimeMillis();

            try {
                voltclient.callProcedure(callBack, "RecordHit", playerId, field1, field2, field3, (visitTimeMillis - milliseconds_offset), visitTimeMillis);
            } catch (IOException e) {
                m_logger.error(e.toString());
                System.exit(-1);
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

            if (currentTime >= nextLatencyReset) {
                synchronized(callBack) {
                    nextLatencyReset = currentTime + reset_latency_millis;

                    min_execution_milliseconds = 999999999l;
                    max_execution_milliseconds = -1l;
                    tot_execution_milliseconds = 0;
                    tot_executions_latency = 0;

                    m_logger.info("...Resetting latency min/max/avg");
                }
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
                    m_logger.info(String.format("[%s] %.3f%% Complete | SP Calls: %,d at %,.2f SP/sec | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f",currentDate, percentComplete, num_sp_calls, (num_sp_calls / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, (double) ((double) tot_execution_milliseconds / (double) tot_executions_latency)));

                    lastOutstanding = thisOutstanding;
                }
            }
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

        long elapsedTimeMillis = System.currentTimeMillis()-startTime;
        float elapsedTimeSec = elapsedTimeMillis/1000F;

        m_logger.info("*************************************************************************");
        m_logger.info("Checking results");
        m_logger.info("*************************************************************************");
        m_logger.info(String.format(" - System ran for %12.4f seconds",elapsedTimeSec));
        m_logger.info(String.format(" - Performed %,d SP calls",num_sp_calls));
        m_logger.info(String.format(" - At %,.2f calls per second",num_sp_calls / elapsedTimeSec));
        m_logger.info(String.format(" - Average Latency = %.2f ms",(double) ((double) tot_execution_milliseconds / (double) tot_executions_latency)));
        m_logger.info(String.format(" -   Latency   0ms -  25ms = %,d",latency_counter[0]));
        m_logger.info(String.format(" -   Latency  25ms -  50ms = %,d",latency_counter[1]));
        m_logger.info(String.format(" -   Latency  50ms -  75ms = %,d",latency_counter[2]));
        m_logger.info(String.format(" -   Latency  75ms - 100ms = %,d",latency_counter[3]));
        m_logger.info(String.format(" -   Latency 100ms - 125ms = %,d",latency_counter[4]));
        m_logger.info(String.format(" -   Latency 125ms - 150ms = %,d",latency_counter[5]));
        m_logger.info(String.format(" -   Latency 150ms - 175ms = %,d",latency_counter[6]));
        m_logger.info(String.format(" -   Latency 175ms - 200ms = %,d",latency_counter[7]));
        m_logger.info(String.format(" -   Latency 200ms+        = %,d",latency_counter[8]));

        try {
            voltclient.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

