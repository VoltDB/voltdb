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
package com;

import java.util.ArrayList;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

// Multi-Threaded Synchronous Client
public class JiggleClient
{
    /*
     * Synchronous Jiggle Application Client.
     */
    static class SyncClient extends ClientThread
    {
        private final int PoolSize;
        private final String ProcedureName;

        public SyncClient(AppMonitor monitor, Client client, long maxProcessPerMillisecond, int poolSize, String procedureName)
        {
            super(monitor, client, maxProcessPerMillisecond);
            this.PoolSize = poolSize;
            this.ProcedureName = procedureName;
        }

        @Override
        public void processOne() throws Exception
        {
            this.updateCounter(this.Client.callProcedure(this.ProcedureName, (long)this.Rand.nextInt(this.PoolSize)).getClientRoundtrip());
        }
    }

    // Asynchronous Jiggle Application Client.
    static class AsyncClient extends ClientThread
    {
        static class AsyncCallback implements ProcedureCallback
        {
            private final AsyncClient Owner;
            public AsyncCallback(AsyncClient owner)
            {
                super();
                this.Owner = owner;
            }
            @Override
            public void clientCallback(ClientResponse clientResponse) {
                final byte status = clientResponse.getStatus();
                if (status != ClientResponse.SUCCESS) {
                    // will track errors at some point
                    System.err.println("Failed to execute!!!");
                    System.err.println(clientResponse.getStatusString());
                    System.err.println(clientResponse.getException());
                    System.exit(-1);
                }
                else
                    this.Owner.updateCounter(clientResponse.getClientRoundtrip());
            }
        }
        private final int PoolSize;
        private final String ProcedureName;
        private final AsyncCallback Callback;

        public AsyncClient(AppMonitor monitor, Client client, long maxProcessPerMillisecond, int poolSize, String procedureName)
        {
            super(monitor, client, maxProcessPerMillisecond);
            this.PoolSize = poolSize;
            this.ProcedureName = procedureName;
            this.Callback = new AsyncCallback(this);
        }

        @Override
        public void processOne() throws Exception
        {
            this.Client.callProcedure(this.Callback, this.ProcedureName, (long)this.Rand.nextInt(this.PoolSize));
        }
    }

    // Prints application usage.
    public static void printUsage()
    {
        System.out.println(
          "Usage: JiggleClient --help\n"
        + "   or  JiggleClient --pool=pool_size\n"
        + "                    --procedure=procedure_name\n"
        + "                    --threads=thread_count\n"
        + "                    --rate=max_process_call_per_second\n"
        + "                    [--async]\n"
        + "                    [--share-connection]\n"
        + "                    [--display-interval=display_interval_in_seconds]\n"
        + "                    [--duration=run_duration_in_seconds]\n"
        + "                    [--servers=comma_separated_server_list]\n"
        + "                    [--port=port_number]\n"
        + "\n"
        + "--pool=pool_size\n"
        + "  Size of the record pool to operate on - larger sizes will cause a higher insert/update-delete rate.\n"
        + "\n"
        + "--procedure=procedure_name\n"
        + "  Name of the Jiggle procedure to call.\n"
        + "\n"
        + "--threads=thread_count\n"
        + "  Number of concurrent threads attacking the database.\n"
        + "\n"
        + "--rate=max_process_call_per_second\n"
        + "  Maximum number of process calls per thread per second.\n"
        + "\n"
        + "[--async]\n"
        + "  Whether to use Asynchronous clients (synchronous clients are used by default).\n"
        + "\n"
        + "[--share-connection]\n"
        + "  Whether threads should share a single connection or create a separate connection of their own.\n"
        + "\n"
        + "[--display-interval=display_interval_in_seconds]\n"
        + "  Interval for performance feedback.\n"
        + "  Default: 10 seconds.\n"
        + "\n"
        + "[--duration=run_duration_in_seconds]\n"
        + "  Benchmark duration.\n"
        + "  Default: 120 seconds.\n"
        + "\n"
        + "[--servers=comma_separated_server_list]\n"
        + "  List of servers to connect to.\n"
        + "  Default: localhost.\n"
        + "\n"
        + "[--port=port_number]\n"
        + "  Client port to connect to on cluster nodes.\n"
        + "  Default: 21212.\n"
        );
    }

    // Application entry point.
    public static void main(String args[])
    {
        try
        {
            // Initialize parameter defaults
            int pool = 0;
            String procedure = "";
            int threadCount = 0;
            long rate = 0;
            boolean async = false;
            boolean shareConnection = false;
            long displayInterval = 10l;
            long duration = 120l;
            String serverList = "localhost";
            int port = 21212;

            // Parse out parameters
            for(int i = 0; i < args.length; i++)
            {
                String arg = args[i];
                if (arg.startsWith("--procedure="))
                    procedure = arg.split("=")[1];
                else if (arg.startsWith("--pool="))
                    pool = Integer.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--threads="))
                    threadCount = Integer.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--rate="))
                    rate = Long.valueOf(arg.split("=")[1]);
                else if (arg.equals("--async"))
                    async = true;
                else if (arg.equals("--share-connection"))
                    shareConnection = true;
                else if (arg.startsWith("--display-interval="))
                    displayInterval = Long.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--duration="))
                    duration = Long.valueOf(arg.split("=")[1]);
                else if (arg.startsWith("--servers="))
                    serverList = arg.split("=")[1];
                else if (arg.startsWith("--port="))
                    port = Integer.valueOf(arg.split("=")[1]);
                else if (arg.equals("--help"))
                {
                    printUsage();
                    System.exit(0);
                }
                else
                {
                    System.err.println("Invalid Usage.");
                    printUsage();
                    System.exit(-1);
                }
            }

            // Validate parameters
            if ((pool <= 0) || (procedure == "") || (threadCount <= 0) || (rate <= 0))
            {
                System.err.println("Invalid Usage.");
                printUsage();
                System.exit(-1);
            }

            // Split server list
            String[] servers = serverList.split(",");

            // Print out parameters
            System.out.printf(
                               "-------------------------------------------------------------------------------------\n"
                             + "       Procedure: %s\n"
                             + "        PoolSize: %d\n"
                             + "         Threads: %d (%s)\n"
                             + "      Connection: %s\n"
                             + "  Rate (/Thread): %d TPS\n"
                             + "        Feedback: Every %,d second(s)\n"
                             + "        Duration: %,d second(s)\n"
                             + "         Servers: %s\n"
                             + "            Port: %d\n"
                             + "-------------------------------------------------------------------------------------\n"
                             , procedure
                             , pool
                             , threadCount
                             , (async?"Async":"Sync")
                             , (shareConnection?"Shared":"One-per-Thread")
                             , rate
                             , displayInterval
                             , duration
                             , serverList
                             , port
                             );

            // Start monitor
            AppMonitor monitor = new AppMonitor(duration*1000l, displayInterval*1000l);

            // Create shared connection (only if needed)
            Client client = shareConnection ? ClientExtensions.GetClient(servers,port) : null;

            // Create threads
            ArrayList<Thread> threads = new ArrayList<Thread>();
            for (int i = 0; i < threadCount; i++)
            {
                threads.add(
                    new Thread(
                          async
                        ? new AsyncClient( monitor
                                         , shareConnection ? client : ClientExtensions.GetClient(servers,port)
                                         , rate
                                         , pool
                                         , procedure
                                         )
                        : new SyncClient( monitor
                                        , shareConnection ? client : ClientExtensions.GetClient(servers,port)
                                        , rate
                                        , pool
                                        , procedure
                                        )
                              )
                           );
            }

            // Start threads
            for (Thread thread : threads)
                thread.start();

            // Wait for threads to complete (they will as soon as the monitor gives the order)
            for (Thread thread : threads)
                thread.join();

            // Stop monitor (displays final results)
            monitor.stop();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

