/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
/*
 * VoltCache provides a Memcache-like interface on a VoltDB database.
 *
 * This samples uses multiple threads to post synchronous requests to the
 * VoltDB server, simulating multiple client application posting
 * synchronous requests to the cache, using the VoltCache interface.
 *
 * While this example demonstrates simple Get/Set operations, the VoltCache
 * interface exposes many more operations (Add, Replace, Prepend, Append,
 * Check-and-Set Increment/Decrement, etc.) and also provides asynchronous
 * cache interaction.
 *
 * You can review the IVoltCache interface for full details of the API.
 *
 * To use VoltCache in your application, simply import com.api.*
 */
package voltcache;

import java.util.ArrayList;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLongArray;

import org.voltdb.CLIConfig;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;

import voltcache.api.VoltCache;
import voltcache.api.VoltCacheResult;

public class Benchmark
{
	
	// handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";
	
    // Initialize some common constants and variables
    private static final AtomicLongArray GetStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray GetCompressionResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutStoreResults = new AtomicLongArray(2);
    private static final AtomicLongArray PutCompressionResults = new AtomicLongArray(2);

    // Reference to the database connection we will use in them main thread
    private VoltCache cache;
    
    // Configuration for benchmark
    private VoltCacheConfig config;
    private ClientStatsContext fullStatsContext;

    // Class for each thread that will be run in parallel, performing requests against the VoltDB server
    private class ClientThread implements Runnable
    {   
        private final long duration;
        private final PayloadProcessor processor;
        private final double getPutRatio;
        public ClientThread(PayloadProcessor processor, long duration, double getPutRatio) throws Exception
        {
            
            this.duration = duration;
            this.processor = processor;
            this.getPutRatio = getPutRatio;
        }

        @Override
        public void run()
        {
            
            try
            {
                
                long endTime = System.currentTimeMillis() + (1000l * this.duration);
                Random rand = new Random();
                while (endTime > System.currentTimeMillis())
                {
                    // Decide whether to perform a GET or PUT operation
                    if (rand.nextDouble() < getPutRatio)
                    {
                        try
                        {
                            String key = processor.generateRandomKeyForRetrieval();
                            VoltCacheResult result = cache.get(key);
                            // Cache miss (Key does not exist)
                            if (result.Data.size() == 0)
                                GetStoreResults.incrementAndGet(1);
                            else
                            {
                                final PayloadProcessor.Pair pair = processor.retrieveFromStore(key, result.Data.get(key).Value);
                                GetStoreResults.incrementAndGet(0);
                                GetCompressionResults.addAndGet(0, pair.getStoreValueLength());
                                GetCompressionResults.addAndGet(1, pair.getRawValueLength());
                            }
                        }
                        catch(Exception x)
                        {
                            GetStoreResults.incrementAndGet(1);
                        }
                    }
                    else
                    {
                        final PayloadProcessor.Pair pair = processor.generateForStore();
                        try
                        {
                            // Put a key/value pair, asynchronously
                            cache.set(pair.Key, 0, 0, pair.getStoreValue(), false);
                            PutStoreResults.incrementAndGet(0);
                        }
                        catch(Exception x)
                        {
                            PutStoreResults.incrementAndGet(1);
                        }
                        finally
                        {
                            PutCompressionResults.addAndGet(0, pair.getStoreValueLength());
                            PutCompressionResults.addAndGet(1, pair.getRawValueLength());
                        }
                    }
                }
            }
            catch(Exception x)
            {
                System.err.println("Exception: " + x);
                x.printStackTrace();
            }
        }
    }
    
    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class VoltCacheConfig extends CLIConfig {
    	
		@Option(desc = "Number of concurrent threads attacking the database.")
		int threads = 1;

		@Option(desc = "Interval for performance feedback, in seconds.")
		long displayinterval = 10;

		@Option(desc = "Benchmark duration, in seconds.")
		long duration = 120;

		@Option(desc = "List of VoltDB servers to connect to.")
		String servers = "localhost";

		@Option(desc = "Client port to connect to on cluster nodes.")
		int port = 21212;

		@Option(desc = "Ratio of GET versus PUT operations: 1.0 => 100% GETs; 0.0 => 0% GETs; 0.95 => 95% GETs, 5% PUTs. Value between 0 and 1")
		double getputratio = 0.95;

		@Option(desc = "Size of the pool of keys to work with (10,00, 10,000, 100,000 items, etc.).")
		int poolsize = 100000;

		@Option(desc = "Whether the data store should be initialized with default values before the benchmark is run (true|false).")
		boolean preload = true;

		@Option(desc = "Size of the keys in number of characters. Max: 250")
		int keysize = 50;

		@Option(desc = "Minimum size for the value blob (in bytes, uncompressed). Max: 1048576")
		int minvaluesize = 1000;

		@Option(desc = "Maximum size for the value blob (in bytes, uncompressed) - set equal to min-value-size for constant size. Max: 1048576")
		int maxvaluesize = 1000;

		@Option(desc = "Whether value blobs should be compressed (GZip) for storage in the database (true|false).")
		boolean usecompression = false;
    	
    	
        @Override
        public void validate() {
            if (threads  < 1) exitWithMessageAndUsage("threadcount must be greater than 0");
            if (displayinterval < 1) exitWithMessageAndUsage("displayInterval must be greater than 0");
            if (duration < 1) exitWithMessageAndUsage("durations must be greater than 0");
            if (servers != null && servers.trim().length() ==  0) exitWithMessageAndUsage("Must specify servers");
            if (port <= 0) exitWithMessageAndUsage("port must be greater than 0");
            if (getputratio < 0 || getputratio > 1 ) exitWithMessageAndUsage("getputration must be between 0.0 and 1 exclusive.");
            if (minvaluesize < 0 && minvaluesize > 1048576) exitWithMessageAndUsage("minvaluesize must be greater than 0 and less than 1048576");
            if (maxvaluesize < 0 && maxvaluesize > 1048576 || maxvaluesize < minvaluesize ) exitWithMessageAndUsage("maxvaluesize must be greater than 0,less than 1048576 and greater than minvaluesize." );
        }
    }

	
    
    public Benchmark(VoltCacheConfig config) {
    	this.config = config;
    	
		System.out.print(HORIZONTAL_RULE);
		System.out.println(" Command Line Configuration");
		System.out.println(HORIZONTAL_RULE);
		System.out.println(config.getConfigDumpString());
    }
    	
    

    // Application entry point
    public static void main(String[] args) {
       
        	VoltCacheConfig config = new VoltCacheConfig();
        	config.parse(Benchmark.class.getName(), args);
        	Benchmark benchmark = new Benchmark(config);
        	benchmark.runBenchmark();
    }
    
    public void runBenchmark() {
    	connect();
    	
        final PayloadProcessor processor = initPayload();
        Timer timer = startStatsTimer();

        try {
			executePayload(processor);
		} catch (Exception e) {
			// Threads failed to execute to completion
			e.printStackTrace();
		}

        timer.cancel();

        // Now print application results:
        displayStatistics();

        // initialized on connect();
        this.cache.close();
    }
	protected void displayStatistics() {
		ClientStats stats = fullStatsContext.fetch().getStats();
		
		// 1. Store statistics as tracked by the application (ops counts, payload traffic)
		System.out.printf(
		HORIZONTAL_RULE
		+ " Store Results\n"
		+ HORIZONTAL_RULE
		+ "A total of %,d operations was posted...\n"
		+ " - GETs: %,9d Operations (%,9d Misses/Failures)\n"
		+ "         %,9d MB in compressed store data\n"
		+ "         %,9d MB in uncompressed application data\n"
		+ "         Network Throughput: %6.3f Gbps*\n\n"
		+ " - PUTs: %,9d Operations (%,9d Failures)\n"
		+ "         %,9d MB in compressed store data\n"
		+ "         %,9d MB in uncompressed application data\n"
		+ "         Network Throughput: %6.3f Gbps*\n\n"
		+ " - Total Network Throughput: %6.3f Gbps*\n\n"
		+ "* Figure includes key & value traffic but not database protocol overhead.\n"
		+ "\n"
		, GetStoreResults.get(0)+GetStoreResults.get(1)+PutStoreResults.get(0)+PutStoreResults.get(1)
		, GetStoreResults.get(0)
		, GetStoreResults.get(1)
		, GetCompressionResults.get(0)/1048576l
		, GetCompressionResults.get(1)/1048576l
		, ((double)GetCompressionResults.get(0) + 
				(GetStoreResults.get(0)+
				GetStoreResults.get(1))*
				this.config.keysize)/(134217728d*this.config.duration)
		, PutStoreResults.get(0)
		, PutStoreResults.get(1)
		, PutCompressionResults.get(0)/1048576l
		, PutCompressionResults.get(1)/1048576l
		, ((double)PutCompressionResults.get(0) +
				(PutStoreResults.get(0)+
				PutStoreResults.get(1))
				*this.config.keysize)/(134217728d*this.config.duration)
		, ((double)GetCompressionResults.get(0) + 
				(GetStoreResults.get(0)+
				GetStoreResults.get(1))*
				this.config.keysize)/(134217728d*this.config.duration)
		+ ((double)PutCompressionResults.get(0)+
				(PutStoreResults.get(0)
				+PutStoreResults.get(1))
				*this.config.keysize)/(134217728d*this.config.duration)
		);
		
		// 2. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        System.out.printf("Average latency:               %,9d ms\n", stats.getAverageLatency());
        System.out.printf("95th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.95));
        System.out.printf("99th percentile latency:       %,9d ms\n", stats.kPercentileLatency(.99));

		System.out.print("\n" + HORIZONTAL_RULE);
        System.out.println(" System Server Statistics");
        System.out.println(HORIZONTAL_RULE);
        System.out.printf("Reported Internal Avg Latency: %,9d ms\n", stats.getAverageInternalLatency());
	}



	protected void executePayload(final PayloadProcessor processor)
			throws Exception {
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < this.config.threads; i++)
				threads.add(new Thread(new ClientThread( processor, 
						this.config.duration, 
						this.config.getputratio)));

		// Start threads
		for (Thread thread : threads)
		    thread.start();

		// Wait for threads to complete
		for (Thread thread : threads)
		    thread.join();
	}



	protected Timer startStatsTimer() {
		// Create a Timer task to display performance data on the operating procedures
		long interval = config.displayinterval*1000l;
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			final ClientStatsContext periodicStatsContext = cache.getStatistics();
			final long startTime = periodicStatsContext.getStats().getStartTimestamp();
			
		    @Override
		    public void run() {
		    	printStatistics(periodicStatsContext.fetchAndResetBaseline().getStats(),startTime);
		    }
		}
		, interval
		, interval
		);
		return timer;
	}
	
	/**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics(ClientStats stats, long startTime) {    
        long time = Math.round((stats.getEndTimestamp() - startTime) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d, ",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        System.out.printf("Avg/95%% Latency %d/%dms\n", stats.getAverageLatency(),
                stats.kPercentileLatency(0.95));
    }



	protected PayloadProcessor initPayload() {
		// Get a payload generator to create random Key-Value pairs to store in the database and process (uncompress) pairs retrieved from the database.
		final PayloadProcessor processor = 
				new PayloadProcessor(this.config.keysize, 
						this.config.minvaluesize, 
						this.config.maxvaluesize, 
						this.config.poolsize, 
						this.config.usecompression);

		// Initialize the store
		if (this.config.preload)
		{
		    System.out.print("Initializing data store... ");
		    for(int i=0; i< this.config.poolsize; i++)
		    {
		        final PayloadProcessor.Pair pair = processor.generateForStore(i);
		        cache.set(pair.Key, 0, 0, pair.getStoreValue(), true);
		    }
		    System.out.println(" Done.");
		}
		return processor;
	}
	
	


	protected void connect() {
		System.out.print(HORIZONTAL_RULE);
        System.out.println(" Setup & Initialization");
        System.out.println(HORIZONTAL_RULE);
		
		// Get a client connection - we retry for a while in case the server hasn't started yet
		System.out.printf("Connecting to servers: %s at port: %d\n", this.config.servers, this.config.port);
	    try
	    {
	        cache = new VoltCache( this.config.servers, this.config.port);
	        this.fullStatsContext = cache.getStatistics();
	    }
	    catch (Exception e)
	    {
	        e.printStackTrace();
	    }
	}
}
