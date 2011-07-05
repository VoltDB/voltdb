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

// Client application for "KeyValue" sample application
//
//   Connects to server, initialize database if needed, generate gets and puts


package com;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;


import org.voltdb.VoltTable;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.logging.VoltLogger;

public class ClientKV {

    private static byte[] gzip(byte[] bytes)
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream((int)(bytes.length * 0.7));
            GZIPOutputStream gzos = new GZIPOutputStream(baos);
            gzos.write(bytes);
            gzos.close();
            return baos.toByteArray();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    private static byte[] gunzip(byte bytes[])
    {
        // Check to see if it's gzip-compressed
        // GZIP Magic Two-Byte Number: 0x8b1f (35615)
        if( (bytes != null) && (bytes.length >= 4) )
        {

            int head = (bytes[0] & 0xff) | ((bytes[1] << 8) & 0xff00);
            if( GZIPInputStream.GZIP_MAGIC == head )
            {
                ByteArrayInputStream  bais = null;
                GZIPInputStream gzis = null;
                ByteArrayOutputStream baos = null;
                byte[] buffer = new byte[2048];
                int    length = 0;

                try
                {
                    baos = new ByteArrayOutputStream();
                    bais = new ByteArrayInputStream( bytes );
                    gzis = new GZIPInputStream( bais );

                    while( ( length = gzis.read( buffer ) ) >= 0 )
                        baos.write(buffer,0,length);

                    // No error? Get new bytes.
                    bytes = baos.toByteArray();

                }   // end try
                catch( java.io.IOException e )
                {
                    e.printStackTrace();
                    // Just return originally-decoded bytes
                }   // end catch
                finally
                {
                    try{ baos.close(); } catch( Exception e ){}
                    try{ gzis.close(); } catch( Exception e ){}
                    try{ bais.close(); } catch( Exception e ){}
                }   // end finally

            }   // end if: gzipped
        }   // end if: bytes.length >= 2
        return bytes;
    }

    public enum spName { GET, PUT };

    public static Object lockObject = new Object();

    public static long min_execution_milliseconds = 999999999l;
    public static long max_execution_milliseconds = -1l;
    public static long tot_execution_milliseconds = 0;
    public static long tot_executions = 0;
    public static long tot_executions_latency = 0;
    public static long[] latency_counter = new long[] {0,0,0,0,0,0,0,0,0};

    public static long get_value_compressed_bytes = 0;
    public static long get_value_uncompressed_bytes = 0;

    public static long cycle_min_execution_milliseconds = 999999999l;
    public static long cycle_max_execution_milliseconds = -1l;
    public static long cycle_tot_execution_milliseconds = 0;
    public static long cycle_tot_executions_latency = 0;

    public static boolean checkLatency = false;

    public static final VoltLogger m_logger = new VoltLogger(ClientKV.class.getName());

    private enum Behavior {
        NONE,
        GZIP;
    }

    private static Behavior behavior = Behavior.NONE;

    static class AsyncCallback implements ProcedureCallback {
        public spName spnProcedure;
        public String cbKeyValue;

        public AsyncCallback(spName spnValue, String keyValue) {
            spnProcedure = spnValue;
            cbKeyValue = keyValue;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            synchronized(lockObject) {
                final byte status = clientResponse.getStatus();

                if (status != ClientResponse.SUCCESS) {
                    m_logger.error("Failed to execute!!!");
                    m_logger.error(clientResponse.getStatusString());
                    m_logger.error(clientResponse.getException());
                } else {
                    tot_executions++;

                    // check if this was a GET and if so check size
                    if (this.spnProcedure == spName.GET) {
                        if (clientResponse.getResults()[0].getRowCount() == 0) {
                            m_logger.info("Get Miss, key = " + cbKeyValue);
                        } else {
                            byte[] baGetValue = clientResponse.getResults()[0].fetchRow(0).getVarbinary(0);
                            get_value_compressed_bytes += baGetValue.length;

                            if (behavior == Behavior.NONE) {
                                //if NOT bas64 encoding or compressing
                                get_value_uncompressed_bytes += baGetValue.length;
                            } else if (behavior == Behavior.GZIP) {
                                // if NOT using compression
                                get_value_uncompressed_bytes += gunzip(baGetValue).length;
                            } else {
                                System.err.println("Unsupported behavior " + behavior);
                                System.exit(-1);
                            }
                        }
                    }

                    if (checkLatency) {
                        pClientCallback(clientResponse.getResults(), clientResponse.getClientRoundtrip());
                    }
                }
            }
        }

        protected void pClientCallback(VoltTable[] vtResults, int clientRoundtrip) {
            long execution_time = clientRoundtrip;

            tot_executions_latency++;
            cycle_tot_executions_latency++;

            tot_execution_milliseconds += execution_time;
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
        };
    }

    public static void main(String args[]) {
        long transactions_per_second_requested = Long.valueOf(args[0]);
        long transactions_per_second = transactions_per_second_requested;
        long transactions_per_milli = transactions_per_second / 1000l;
        long client_feedback_interval_secs = Long.valueOf(args[1]);
        long test_duration_secs = Long.valueOf(args[2]);
        long lag_latency_seconds = Long.valueOf(args[3]);
        long lag_latency_millis = lag_latency_seconds * 1000l;
        String serverList = args[4];
        int key_size = Integer.valueOf(args[5]);
        int min_value_size = Integer.valueOf(args[6]);
        int max_value_size = Integer.valueOf(args[7]);
        long initial_size = Long.valueOf(args[8]);
        int percent_gets = Integer.valueOf(args[9]);
        final int behavior_type = Integer.valueOf(args[10]);

        boolean use_auto_tuning = Boolean.valueOf(args[11]) && (transactions_per_second > 1000);
        double auto_tuning_target_latency_millis = Double.valueOf(args[12]);
        double auto_tuning_adjustment_rate = Double.valueOf(args[13]);
        if (auto_tuning_adjustment_rate > 1.0) {
            auto_tuning_adjustment_rate = auto_tuning_adjustment_rate / 100.0;
        }
        long auto_tuning_interval_secs = Long.valueOf(args[14]);

        long thisOutstanding = 0;
        long lastOutstanding = 0;
        long put_value_compressed_bytes = 0;
        long put_value_uncompressed_bytes = 0;

        m_logger.info(String.format("Submitting %,d Transactions/sec (TPS)",transactions_per_second));
        if (use_auto_tuning) {
            m_logger.info(String.format("Auto-Tuning = ON"));
            m_logger.info(String.format(" - Tuning interval = %,d second(s)", auto_tuning_interval_secs));
            m_logger.info(String.format(" - Target latency = %.2f ms", auto_tuning_target_latency_millis));
            m_logger.info(String.format(" - Adjustment rate = %.2f%%", auto_tuning_adjustment_rate * 100.0));
        }
        m_logger.info(String.format("Feedback interval = %,d second(s)",client_feedback_interval_secs));
        m_logger.info(String.format("Running for %,d second(s)",test_duration_secs));
        m_logger.info(String.format("Latency not recorded for %d second(s)",lag_latency_seconds));
        m_logger.info(String.format("Key size = %,d",key_size));
        m_logger.info(String.format("Minimum Value size = %,d",min_value_size));
        m_logger.info(String.format("Maximum Value size = %,d",max_value_size));
        m_logger.info(String.format("Initial number of keys/values = %,d",initial_size));
        m_logger.info(String.format("Percentage Gets (vs. puts) = %,d",percent_gets));
        if (behavior_type == 1) {
            m_logger.info(String.format("Payload stored as is."));
            behavior = Behavior.NONE;
        } else {
            m_logger.info(String.format("Payload will be Gzipped."));
            behavior = Behavior.GZIP;
        }

        long transactions_this_second = 0;
        long last_millisecond = System.currentTimeMillis();
        long this_millisecond = System.currentTimeMillis();

        ClientConfig config = new ClientConfig("program", "none");
        final org.voltdb.client.Client voltclient = ClientFactory.createClient(config);

        String[] voltServers = serverList.split(",");

        for (String thisServer : voltServers) {
            try {
                thisServer = thisServer.trim();
                m_logger.info(String.format("Connecting to server: %s",thisServer));
                voltclient.createConnection(thisServer);
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
        long lastAutoTuningTime = startTime;
        long num_sp_calls = 0;
        long cycle_num_sp_calls = 0;
        long startRecordingLatency = startTime + lag_latency_millis;

        long num_gets = 0;
        long num_puts = 0;

        String this_key;

        byte[] baGenericValue = new byte[max_value_size];

        for (int i=0; i < max_value_size; i++) {
            // set the "64" to whatever number of "values" from 256 you want included in the payload, the lower the number the more compressible the payload
            baGenericValue[i] = (byte) rand.nextInt(64);
        }

        // test if database needs initialization
        int initialize_data = 0;

        try {
            String init_key = String.format("K%1$#" + (key_size-1) + "s", initial_size);
            VoltTable[] vtInit = voltclient.callProcedure("Get", init_key).getResults();
            if (vtInit[0].getRowCount() == 0) {
                // database is not fully initialized, do initialization
                initialize_data = 1;
            }
        } catch (ProcCallException e) {
            m_logger.error("ProcCallException");
            m_logger.error(e.toString());
            System.exit(-1);
        } catch (IOException e) {
            m_logger.error("IOException");
            m_logger.error(e.toString());
            System.exit(-1);
        }

        if (initialize_data == 1) {
            // ***********************************************************************************************************************************************
            // initial population
            // ***********************************************************************************************************************************************
            m_logger.info(String.format("******************************************************************************************************************************"));
            m_logger.info(String.format("Populating Initial Data"));
            m_logger.info(String.format("******************************************************************************************************************************"));
            while (num_puts < initial_size) {
                num_puts++;

                this_key = String.format("K%1$#" + (key_size-1) + "s", num_puts);
                byte[] baThisValue = Arrays.copyOfRange(baGenericValue,0,min_value_size+rand.nextInt(max_value_size-min_value_size+1));
                byte[] this_value = null;

                if (behavior == Behavior.NONE) {
                    // if not using compression
                    this_value = baThisValue;
                } else if (behavior == Behavior.GZIP) {
                    // if using compression
                    this_value = gzip(baThisValue);
                } else {
                    System.err.println("Unsupported behavior " + behavior);
                    System.exit(-1);
                }

                put_value_uncompressed_bytes += baThisValue.length;
                put_value_compressed_bytes += this_value.length;

                try {
                    voltclient.callProcedure(new AsyncCallback(spName.PUT, this_key), "Put", this_key, this_value);
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

                if (currentTime >= (lastFeedbackTime + (client_feedback_interval_secs * 1000))) {
                    synchronized(lockObject) {
                        lastFeedbackTime = currentTime;

                        long elapsedTimeMillis2 = System.currentTimeMillis()-startTime;
                        float elapsedTimeSec2 = elapsedTimeMillis2/1000F;

                        if (tot_executions_latency == 0) {
                            tot_executions_latency = 1;
                        }
                        thisOutstanding = num_puts - tot_executions;

                        double percentComplete = ((double) num_puts / (double) initial_size) * 100;
                        if (percentComplete > 100.0) {
                            percentComplete = 100.0;
                        }

                        // calculate IO statistics
                        VoltTable vtIOStats = voltclient.getIOStatsInterval();
                        int vtRowCount = vtIOStats.getRowCount();
                        long bytesRead = 0;
                        long bytesWritten = 0;
                        double readMBPerSecond = 0;
                        double writeMBPerSecond = 0;

                        if (vtRowCount > 0) {
                            bytesRead = vtIOStats.fetchRow(vtRowCount-1).getLong(9);
                            bytesWritten = vtIOStats.fetchRow(vtRowCount-1).getLong(11);
                            readMBPerSecond = (bytesRead / 1024.0 / 1024.0) / client_feedback_interval_secs;
                            writeMBPerSecond = (bytesWritten / 1024.0 / 1024.0) / client_feedback_interval_secs;
                        } else {
                            readMBPerSecond = -1.0;
                            writeMBPerSecond = -1.0;
                        }

                        String currentDate = new Date().toString();
                        m_logger.info(String.format("[%s] %.3f%% Complete | Transactions: %,d at %,.2f TPS | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f | Client MB in/out = %,.3f / %,.3f",currentDate, percentComplete, num_puts, (num_puts / elapsedTimeSec2), thisOutstanding,(thisOutstanding - lastOutstanding), min_execution_milliseconds, max_execution_milliseconds, ((double) tot_execution_milliseconds / (double) tot_executions_latency),readMBPerSecond,writeMBPerSecond));

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

            if (num_puts == 0) { num_puts = 1; };
            if (num_gets == 0) { num_gets = 1; };
            if (put_value_uncompressed_bytes == 0) { put_value_uncompressed_bytes = 1; };
            if (get_value_uncompressed_bytes == 0) { get_value_uncompressed_bytes = 1; };
            if (tot_executions_latency == 0) { tot_executions_latency = 1; };
            if (elapsedTimeSec == 0) { elapsedTimeSec = 1; };

            m_logger.info(String.format("*************************************************************************"));
            m_logger.info(String.format("Checking Results - Populating Initial Data"));
            m_logger.info(String.format("*************************************************************************"));
            m_logger.info(String.format(" - System ran for %12.4f seconds",elapsedTimeSec));
            m_logger.info(String.format(" - Transactions / GETS / PUTS   = %,d / %,d / %,d",num_gets + num_puts, num_gets, num_puts));
            m_logger.info(String.format(" - Transactions per second = %,.2f",num_puts / elapsedTimeSec));
            m_logger.info(String.format(" -         GETS per second = %,.2f",num_gets / elapsedTimeSec));
            m_logger.info(String.format(" -         PUTS per second = %,.2f",num_puts / elapsedTimeSec));
            m_logger.info(String.format(" - PUTS Uncompressed Bytes / Compressed Bytes / Compressed Size / Avg Value Size Bytes = %,d / %,d / %,.2f%% / %,.2f",put_value_uncompressed_bytes,put_value_compressed_bytes,((double) put_value_compressed_bytes / (double) put_value_uncompressed_bytes) * 100.0, (double) put_value_uncompressed_bytes / (double) num_puts));
            m_logger.info(String.format(" - GETS Uncompressed Bytes / Compressed Bytes / Compressed Size / Avg Value Size Bytes = %,d / %,d / %,.2f%% / %,.2f",get_value_uncompressed_bytes,get_value_compressed_bytes,((double) get_value_compressed_bytes / (double) get_value_uncompressed_bytes) * 100.0, (double) get_value_uncompressed_bytes / (double) num_gets));
            m_logger.info(String.format(" - Average Latency = %.2f ms",((double) tot_execution_milliseconds / (double) tot_executions_latency)));
            m_logger.info(String.format(" -   Latency   0ms -  25ms = %,d",latency_counter[0]));
            m_logger.info(String.format(" -   Latency  25ms -  50ms = %,d",latency_counter[1]));
            m_logger.info(String.format(" -   Latency  50ms -  75ms = %,d",latency_counter[2]));
            m_logger.info(String.format(" -   Latency  75ms - 100ms = %,d",latency_counter[3]));
            m_logger.info(String.format(" -   Latency 100ms - 125ms = %,d",latency_counter[4]));
            m_logger.info(String.format(" -   Latency 125ms - 150ms = %,d",latency_counter[5]));
            m_logger.info(String.format(" -   Latency 150ms - 175ms = %,d",latency_counter[6]));
            m_logger.info(String.format(" -   Latency 175ms - 200ms = %,d",latency_counter[7]));
            m_logger.info(String.format(" -   Latency 200ms+        = %,d",latency_counter[8]));
        }


        // ***********************************************************************************************************************************************
        // gets and puts
        // ***********************************************************************************************************************************************

        thisOutstanding = 0;
        lastOutstanding = 0;
        put_value_compressed_bytes = 0;
        put_value_uncompressed_bytes = 0;
        get_value_compressed_bytes = 0;
        get_value_uncompressed_bytes = 0;

        min_execution_milliseconds = 999999999l;
        max_execution_milliseconds = -1l;
        tot_execution_milliseconds = 0;
        tot_executions = 0;
        tot_executions_latency = 0;
        latency_counter = new long[] {0,0,0,0,0,0,0,0,0};
        checkLatency = false;

        cycle_min_execution_milliseconds = 999999999l;
        cycle_max_execution_milliseconds = -1l;
        cycle_tot_execution_milliseconds = 0;
        cycle_tot_executions_latency = 0;

        transactions_this_second = 0;
        last_millisecond = System.currentTimeMillis();
        this_millisecond = System.currentTimeMillis();

        startTime = System.currentTimeMillis();
        endTime = startTime + (1000l * test_duration_secs);
        currentTime = startTime;
        lastFeedbackTime = startTime;
        lastAutoTuningTime = startTime;
        num_sp_calls = 0;
        cycle_num_sp_calls = 0;
        startRecordingLatency = startTime + lag_latency_millis;

        num_gets = 0;
        num_puts = 0;

        m_logger.info(String.format("******************************************************************************************************************************"));
        m_logger.info(String.format("Running Get/Put Benchmark"));
        m_logger.info(String.format("******************************************************************************************************************************"));
        while (currentTime < endTime) {
            num_sp_calls++;
            cycle_num_sp_calls++;

            // determine if this is a get or a put
            int getTest = rand.nextInt(99)+1;

            this_key = String.format("K%1$#" + (key_size-1) + "s", (long) ((rand.nextDouble() * initial_size) + 1));

            if (getTest <= percent_gets) {
                // do a get
                num_gets++;

                try {
                    voltclient.callProcedure(new AsyncCallback(spName.GET, this_key), "Get", this_key);
                } catch (IOException e) {
                    m_logger.error(e.toString());
                    System.exit(-1);
                }
            } else {
                // do a put
                num_puts++;
                byte[] baThisValuePut = Arrays.copyOfRange(baGenericValue,0,min_value_size+rand.nextInt(max_value_size-min_value_size+1));
                byte[] this_value = null;

                if (behavior == Behavior.NONE) {
                    // if not using compression
                    this_value = baThisValuePut;
                } else if (behavior == Behavior.GZIP) {
                    // if using compression
                    this_value = gzip(baThisValuePut);
                } else {
                    System.err.println("Unsupported behavior " + behavior);
                    System.exit(-1);
                }

                put_value_uncompressed_bytes += baThisValuePut.length;
                put_value_compressed_bytes += this_value.length;

                try {
                    voltclient.callProcedure(new AsyncCallback(spName.PUT, this_key), "Put", this_key, this_value);
                } catch (IOException e) {
                    m_logger.error(e.toString());
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
                synchronized(lockObject) {

                    long cycle_elapsedTimeMillis2 = System.currentTimeMillis()-lastAutoTuningTime;
                    float cycle_elapsedTimeSec2 = cycle_elapsedTimeMillis2/1000F;

                    lastAutoTuningTime = currentTime;

                    // Only adjust if both the cycle and total was below request - avoid *some* random spike from downgrading the system too much
                    if ((((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency) > auto_tuning_target_latency_millis) && (((double) tot_execution_milliseconds / (double) tot_executions_latency) > auto_tuning_target_latency_millis))
                    {
                        long new_transactions_per_second = (((long)(Math.min(cycle_num_sp_calls / cycle_elapsedTimeSec2,transactions_per_second) * auto_tuning_adjustment_rate))/1000l)*1000l;
                        String last_tuning_warning = "";
                        if ((new_transactions_per_second <= 1000) || (new_transactions_per_second == transactions_per_second)) {
                            use_auto_tuning = false;
                            last_tuning_warning = " | WARNING: Minimum load boundary reached.";
                        }

                        m_logger.info(String.format("[%s] Auto-Tuning | Observed: %,.2f TPS | Latency: min = %d | max = %d | avg = %.2f | Adjusting DOWN: %,d TPS%s"
                                                   , new Date().toString()
                                                   , (cycle_num_sp_calls / cycle_elapsedTimeSec2)
                                                   , cycle_min_execution_milliseconds
                                                   , cycle_max_execution_milliseconds
                                                   , ((double) cycle_tot_execution_milliseconds / (double) cycle_tot_executions_latency)
                                                   , new_transactions_per_second
                                                   , last_tuning_warning
                                                   )
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
                }
            }
            if (currentTime >= (lastFeedbackTime + (client_feedback_interval_secs * 1000))) {
                synchronized(lockObject) {

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

                    // calculate IO statistics
                    VoltTable vtIOStats = voltclient.getIOStatsInterval();
                    int vtRowCount = vtIOStats.getRowCount();
                    long bytesRead = 0;
                    long bytesWritten = 0;
                    double readMBPerSecond = 0;
                    double writeMBPerSecond = 0;

                    if (vtRowCount > 0) {
                        bytesRead = vtIOStats.fetchRow(vtRowCount-1).getLong(9);
                        bytesWritten = vtIOStats.fetchRow(vtRowCount-1).getLong(11);
                        readMBPerSecond = (bytesRead / 1024.0 / 1024.0) / client_feedback_interval_secs;
                        writeMBPerSecond = (bytesWritten / 1024.0 / 1024.0) / client_feedback_interval_secs;
                    } else {
                        readMBPerSecond = -1.0;
                        writeMBPerSecond = -1.0;
                    }

                    String currentDate = new Date().toString();
                    m_logger.info(String.format("[%s] %.3f%% Complete | Transactions: %,d at %,.2f TPS | outstanding = %d (%d) | min = %d | max = %d | avg = %.2f | Client MB in/out = %,.3f / %,.3f"
                                               , currentDate
                                               , percentComplete
                                               , num_sp_calls
                                               , (num_sp_calls / elapsedTimeSec2)
                                               , thisOutstanding
                                               , (thisOutstanding - lastOutstanding)
                                               , min_execution_milliseconds
                                               , max_execution_milliseconds
                                               , ((double) tot_execution_milliseconds / (double) tot_executions_latency)
                                               , readMBPerSecond
                                               , writeMBPerSecond
                                               )
                                 );
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

        if (num_puts == 0) { num_puts = 1; };
        if (num_gets == 0) { num_gets = 1; };
        if (put_value_uncompressed_bytes == 0) { put_value_uncompressed_bytes = 1; };
        if (get_value_uncompressed_bytes == 0) { get_value_uncompressed_bytes = 1; };
        if (tot_executions_latency == 0) { tot_executions_latency = 1; };
        if (elapsedTimeSec == 0) { elapsedTimeSec = 1; };

        m_logger.info(String.format("*************************************************************************"));
        m_logger.info(String.format("Checking Results - Get/Put Benchmark"));
        m_logger.info(String.format("*************************************************************************"));
        m_logger.info(String.format(" - System ran for %12.4f seconds",elapsedTimeSec));
        m_logger.info(String.format(" - Transactions / GETS / PUTS   = %,d / %,d / %,d",num_sp_calls, num_gets, num_puts));
        m_logger.info(String.format(" - Transactions per second = %,.2f",num_sp_calls / elapsedTimeSec));
        m_logger.info(String.format(" -         GETS per second = %,.2f",num_gets / elapsedTimeSec));
        m_logger.info(String.format(" -         PUTS per second = %,.2f",num_puts / elapsedTimeSec));
        m_logger.info(String.format(" - PUTS Uncompressed Bytes / Compressed Bytes / Compressed Size / Avg Value Size Bytes = %,d / %,d / %,.2f%% / %,.2f",put_value_uncompressed_bytes,put_value_compressed_bytes,((double) put_value_compressed_bytes / (double) put_value_uncompressed_bytes) * 100.0, (double) put_value_uncompressed_bytes / (double) num_puts));
        m_logger.info(String.format(" - GETS Uncompressed Bytes / Compressed Bytes / Compressed Size / Avg Value Size Bytes = %,d / %,d / %,.2f%% / %,.2f",get_value_uncompressed_bytes,get_value_compressed_bytes,((double) get_value_compressed_bytes / (double) get_value_uncompressed_bytes) * 100.0, (double) get_value_uncompressed_bytes / (double) num_gets));
        m_logger.info(String.format(" - Average Latency = %.2f ms",((double) tot_execution_milliseconds / (double) tot_executions_latency)));
        m_logger.info(String.format(" -   Latency   0ms -  25ms = %,d",latency_counter[0]));
        m_logger.info(String.format(" -   Latency  25ms -  50ms = %,d",latency_counter[1]));
        m_logger.info(String.format(" -   Latency  50ms -  75ms = %,d",latency_counter[2]));
        m_logger.info(String.format(" -   Latency  75ms - 100ms = %,d",latency_counter[3]));
        m_logger.info(String.format(" -   Latency 100ms - 125ms = %,d",latency_counter[4]));
        m_logger.info(String.format(" -   Latency 125ms - 150ms = %,d",latency_counter[5]));
        m_logger.info(String.format(" -   Latency 150ms - 175ms = %,d",latency_counter[6]));
        m_logger.info(String.format(" -   Latency 175ms - 200ms = %,d",latency_counter[7]));
        m_logger.info(String.format(" -   Latency 200ms+        = %,d",latency_counter[8]));
        if (transactions_per_second < transactions_per_second_requested) {
            m_logger.info(String.format("*************************************************************************"));
            m_logger.info(String.format("Auto-Tuning Results"));
            m_logger.info(String.format("*************************************************************************"));
            m_logger.info(String.format(" - Optimal Load: %,d TPS to match/approach desired %.2f ms Latency", transactions_per_second, auto_tuning_target_latency_millis));
        }
        try {
            voltclient.close();
        } catch (Exception e) {
            m_logger.error(e.toString());
            System.exit(-1);
        }
    }
}

