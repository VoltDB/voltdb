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
/*
 * This samples uses the native asynchronous request processing protocol
 * to post requests to the VoltDB server, thus leveraging to the maximum
 * VoltDB's ability to run requests in parallel on multiple database
 * partitions, and multiple servers.
 *
 * While asynchronous processing is (marginally) more convoluted to work
 * with and not adapted to all workloads, it is the preferred interaction
 * model to VoltDB as it allows a single client with a small amount of
 * threads to flood VoltDB with requests, guaranteeing blazing throughput
 * performance.
 *
 * Note that this benchmark focuses on throughput performance and
 * not low latency performance.  This benchmark will likely 'firehose'
 * the database cluster (if the cluster is too slow or has too few CPUs)
 * and as a result, queue a significant amount of requests on the server
 * to maximize throughput measurement. To test VoltDB latency, run the
 * SyncBenchmark client, also found in the voter sample directory.
 */

package iwdemo;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.prefs.CsvPreference;
import org.voltdb.CLIConfig;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ClientStatusListenerExt;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import com.google_voltpatches.common.geometry.S2LatLng;

public class Benchmark {

    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
            "----------" + "----------" + "----------" + "----------" +
            "----------" + "----------" + "----------" + "----------" + "\n";

    // validated command line configuration
    final Config config;
    // Reference to the database connection we will use
    final Client client;
    // Timer for periodic stats printing
    Timer timer;
    // Benchmark start time
    long benchmarkStartTS;
    // Statistics manager objects from the client
    final ClientStatsContext periodicStatsContext;
    final ClientStatsContext fullStatsContext;

    // For periodic actions
    final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    /**
     * Uses included {@link CLIConfig} class to
     * declaratively state command line options with defaults
     * and validation.
     */
    static class Config extends CLIConfig {
        @Option(desc = "Interval for performance feedback, in seconds.")
        long displayinterval = 5;

        @Option(desc = "Benchmark duration, in seconds.")
        int duration = 120;

        @Option(desc = "Warmup duration in seconds.")
        int warmup = 2;

        @Option(desc = "Comma separated list of the form server[:port] to connect to.")
        String servers = "localhost";

        @Option(desc = "Maximum TPS rate for benchmark.")
        int ratelimit = Integer.MAX_VALUE;

        @Option(desc = "Report latency for async benchmark run.")
        boolean latencyreport = false;

        @Option(desc = "Filename to write raw summary statistics to.")
        String statsfile = "";

        @Option(desc = "User name for connection.")
        String user = "";

        @Option(desc = "Password for connection.")
        String password = "";

        @Override
        public void validate() {
            if (duration <= 0) exitWithMessageAndUsage("duration must be > 0");
            if (warmup < 0) exitWithMessageAndUsage("warmup must be >= 0");
            if (displayinterval <= 0) exitWithMessageAndUsage("displayinterval must be > 0");
            if (ratelimit <= 0) exitWithMessageAndUsage("ratelimit must be > 0");
        }
    }

    /**
     * Provides a callback to be notified on node failure.
     * This example only logs the event.
     */
    class StatusListener extends ClientStatusListenerExt {
        @Override
        public void connectionLost(String hostname, int port, int connectionsLeft, DisconnectCause cause) {
            // if the benchmark is still active
            if ((System.currentTimeMillis() - benchmarkStartTS) < (config.duration * 1000)) {
                System.err.printf("Connection to %s:%d was lost.\n", hostname, port);
            }
        }
    }

    /**
     * Constructor for benchmark instance.
     * Configures VoltDB client and prints configuration.
     *
     * @param config Parsed & validated CLI options.
     */
    public Benchmark(Config config) {
        this.config = config;

        ClientConfig clientConfig = new ClientConfig(config.user, config.password, new StatusListener());
        clientConfig.setMaxTransactionsPerSecond(config.ratelimit);

        client = ClientFactory.createClient(clientConfig);

        periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Command Line Configuration");
        System.out.println(HORIZONTAL_RULE);
        System.out.println(config.getConfigDumpString());
        if(config.latencyreport) {
            System.out.println("NOTICE: Option latencyreport is ON for async run, please set a reasonable ratelimit.\n");
        }
    }

    /**
     * Connect to a single server with retry. Limited exponential backoff.
     * No timeout. This will run until the process is killed if it's not
     * able to connect.
     *
     * @param server hostname:port or just hostname (hostname can be ip).
     */
    void connectToOneServerWithRetry(String server) {
        int sleep = 1000;
        while (true) {
            try {
                client.createConnection(server);
                break;
            }
            catch (Exception e) {
                System.err.printf("Connection failed - retrying in %d second(s).\n", sleep / 1000);
                try { Thread.sleep(sleep); } catch (Exception interruted) {}
                if (sleep < 8000) sleep += sleep;
            }
        }
        System.out.printf("Connected to VoltDB node at: %s.\n", server);
    }

    /**
     * Connect to a set of servers in parallel. Each will retry until
     * connection. This call will block until all have connected.
     *
     * @param servers A comma separated list of servers using the hostname:port
     * syntax (where :port is optional).
     * @throws InterruptedException if anything bad happens with the threads.
     */
    void connect(String servers) throws InterruptedException {
        System.out.println("Connecting to VoltDB...");

        String[] serverArray = servers.split(",");
        final CountDownLatch connections = new CountDownLatch(serverArray.length);

        // use a new thread to connect to each server
        for (final String server : serverArray) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    connectToOneServerWithRetry(server);
                    connections.countDown();
                }
            }).start();
        }
        // block until all have connected
        connections.await();
    }

    /**
     * Create a Timer task to display performance data on the Vote procedure
     * It calls printStatistics() every displayInterval seconds
     */
    public void schedulePeriodicStats() {
        timer = new Timer();
        TimerTask statsPrinting = new TimerTask() {
            @Override
            public void run() { printStatistics(); }
        };
        timer.scheduleAtFixedRate(statsPrinting,
                                  config.displayinterval * 1000,
                                  config.displayinterval * 1000);
    }

    /**
     * Prints a one line update on performance that can be printed
     * periodically during a benchmark.
     */
    public synchronized void printStatistics() {
        ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
        long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

        System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
        System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
        System.out.printf("Aborts/Failures %d/%d",
                stats.getInvocationAborts(), stats.getInvocationErrors());
        if(this.config.latencyreport) {
            System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
                stats.kPercentileLatencyAsDouble(0.95));
        }
        System.out.printf("\n");
    }

    /**
     * Prints the results of the voting simulation and statistics
     * about performance.
     *
     * @throws Exception if anything unexpected happens.
     */
    public synchronized void printResults() throws Exception {
        ClientStats stats = fullStatsContext.fetch().getStats();

        // 1. Voting Board statistics, Voting results and performance statistics
        String display = "\n" +
                         HORIZONTAL_RULE +
                         " Results\n" +
                         HORIZONTAL_RULE +
                         "\nAstounding success\n";
        System.out.printf(display);

        // 3. Performance statistics
        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Client Workload Statistics");
        System.out.println(HORIZONTAL_RULE);

        System.out.printf("Average throughput:            %,9d txns/sec\n", stats.getTxnThroughput());
        if(this.config.latencyreport) {
            System.out.printf("Average latency:               %,9.2f ms\n", stats.getAverageLatency());
            System.out.printf("10th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.1));
            System.out.printf("25th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.25));
            System.out.printf("50th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.5));
            System.out.printf("75th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.75));
            System.out.printf("90th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.9));
            System.out.printf("95th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.95));
            System.out.printf("99th percentile latency:       %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.99));
            System.out.printf("99.5th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.995));
            System.out.printf("99.9th percentile latency:     %,9.2f ms\n", stats.kPercentileLatencyAsDouble(.999));

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" System Server Statistics");
            System.out.println(HORIZONTAL_RULE);
            System.out.printf("Reported Internal Avg Latency: %,9.2f ms\n", stats.getAverageInternalLatency());

            System.out.print("\n" + HORIZONTAL_RULE);
            System.out.println(" Latency Histogram");
            System.out.println(HORIZONTAL_RULE);
            System.out.println(stats.latencyHistoReport());
        }
        // 4. Write stats to file if requested
        client.writeSummaryCSV(stats, config.statsfile);
    }

    static class ReportingCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse)
                throws Exception {
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(clientResponse.getStatusString());
                System.exit(1);
            }
        }

    }

    /**
     * Core benchmark code.
     * Connect. Initialize. Run the loop. Cleanup. Print Results.
     *
     * @throws Exception if anything unexpected happens.
     */
    public void runBenchmark() throws Exception {

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" S2 Demo");
        System.out.println(HORIZONTAL_RULE);

        // connect to one or more servers, loop until success
        connect(config.servers);

        System.out.print(HORIZONTAL_RULE);
        System.out.println(" Starting Benchmark");
        System.out.println(HORIZONTAL_RULE);
        insertStates();
        insertCounties();
        insertCities();
        setUpTaxis();

        // Show reports every second.
        scheduler.scheduleWithFixedDelay(new Reporter(client), 0, 1, TimeUnit.SECONDS);

        // sleep for configured time
        // Periodic activity registered with the scheduler will happen during this time.
        Thread.sleep(config.duration * 1000);

        scheduler.shutdown();
        client.close();
    }

    private static CellProcessor[] getCountyAttributeCellProcessors() {
        final CellProcessor[] processors = new CellProcessor[] {
                new NotNull(), // SHAPEID;
                new ParseInt(), // STATEFP;
                new ParseInt(), // COUNTYFP;
                new ParseInt(), // COUNTYNS;
                new NotNull(), // AFFGEOID;
                new NotNull(), // GEOID;
                new NotNull(), //  NAME;
                new NotNull(), // LSAD;
                new NotNull(), // ALAND;
                new NotNull() // AWATER;
        };
        return processors;
    }

    private static CellProcessor[] getCountyBoundaryProcessors() {
        final CellProcessor[] processors = new CellProcessor[] {
            new NotNull(), // SHAPEID;
            new ParseDouble(), // X
            new ParseDouble() // Y
        };
        return processors;
    }

    private static class CountyData {
        /*
         * Map from shapeid strings into countyid.  A countyid is the
         * concatenation of a state fip and a county fip id.
         */
        Map<String, Long>     shapeidMap = new HashMap<String, Long>();
        /*
         * Map from a countyid to an attribute bean.
         * Return true if we have already seen this countyid.
         */
        Map<Long, CountyAttributeBean>       idMap = new HashMap<Long, CountyAttributeBean>();
        public boolean addCounty(CountyAttributeBean bean) {
            boolean newBean = false;
            Long oldcid = shapeidMap.get(bean.getSHAPEID());
            CountyAttributeBean oldBean = idMap.get(bean.getCOUNTYID());
            if (oldcid == null) {
                shapeidMap.put(bean.getSHAPEID(), bean.getCOUNTYID());
            }
            if (oldBean == null) {
                idMap.put(bean.getCOUNTYID(), bean);
                newBean = true;
            }
            if (oldcid != null) {
                if (oldcid != bean.getCOUNTYID()) {
                    System.out.printf("County %s (%d) has cid %d and %d\n",
                                      oldcid, bean.getCOUNTYID());
                }
            }
            return newBean;
        }

        public Set<Map.Entry<Long, CountyAttributeBean>> getIDEntrySet() {
            return idMap.entrySet();
        }

        public Long getCountyId(String shapeId) {
            return shapeidMap.get(shapeId);
        }

        public CountyAttributeBean getByShapeID(String shapeId) {
            Long cid = shapeidMap.get(shapeId);
            if (cid == null) {
                return null;
            }
            return idMap.get(cid);
        }

        public CountyAttributeBean getByID(Long id) {
            return idMap.get(id);
        }
    }

    private void insertCounties() throws NoConnectionsException, IOException, ProcCallException {
        CountyData cattrs = readCountyAttributes();
        insertCountyAttributes(cattrs);
        insertCountyBoundaries(cattrs);
    }

    private void insertCountyAttributes(CountyData cattrs) throws NoConnectionsException, IOException, ProcCallException {
        for (Map.Entry<Long, CountyAttributeBean> cabe : cattrs.getIDEntrySet()) {
            CountyAttributeBean cab = cabe.getValue();
            // System.out.printf("Key: %s, id: %d, name: %s\n", cabe.getKey(), cab.getCOUNTYID(), cab.getNAME());
            ClientResponse cr = client.callProcedure("COUNTY.INSERT", cab.getCOUNTYID(), cab.getNAME(), cab.getSTATEFP().longValue());
            if (cr.getStatus() != ClientResponse.SUCCESS) {
                System.err.printf("Insertion into county table failed, status %d\n", cr.getStatus());
                System.exit(100);
            }
        }
    }

    /**
     * Read the county attributes.
     * @return A map from county shapeids and shape numbers to attributes.
     */
    private CountyData readCountyAttributes() {
        CsvBeanReader beanReader = null;
        CountyData answer = new CountyData();
        long nread = 0;
        long ndistinct = 0;
        try {
            InputStream is = Benchmark.class.getResourceAsStream("data/counties_attributes.csv");
            beanReader = new CsvBeanReader(new InputStreamReader(is),
                                           CsvPreference.TAB_PREFERENCE);
            final String[] headers = beanReader.getHeader(true);
            final CellProcessor[] processors = getCountyAttributeCellProcessors();
            CountyAttributeBean county;
            while ( (county = beanReader.read(CountyAttributeBean.class, headers, processors)) != null) {
                if (answer.addCounty(county)) {
                    ndistinct += 1;
                }
                nread += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(100);
        } finally {
            try {
                beanReader.close();
            } catch (IOException e) {
                ;
            }
        }
        System.out.printf("Read %d county attributes (%d distinct)\n", nread, ndistinct);
        return answer;
    }

    private byte[] convertBoundary(ArrayList<ArrayList<S2LatLng>> boundary) {
        // Save enough space for a 4 byte integer and
        // two doubles per coordinate.  Replicate the first
        // and last coordinate needlessly.
        int bytecount = 4;
        for (ArrayList<S2LatLng> loop : boundary) {
            bytecount += 4 + 16 * loop.size();
        }
        ByteBuffer fbuf = ByteBuffer.allocate(bytecount);
        int idx = 0;
        fbuf.putInt(idx, boundary.size());
        idx += 4;
        for (List<S2LatLng> loop : boundary) {
            fbuf.putInt(idx, loop.size());
            idx += 4;
            for (S2LatLng ll : loop) {
                fbuf.putDouble(idx, ll.latDegrees());
                fbuf.putDouble(idx + 8, ll.lngDegrees());
                idx += 16;
            }
        }
        return fbuf.array();
    }

    private void insertOneCountyBoundary(long                   regionId,
                                         int                    componentCount,
                                         CountyAttributeBean    cattrs,
                                         ArrayList<ArrayList<S2LatLng>>   vertices) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = null;
        byte[] vertarray = convertBoundary(vertices);
        cr = client.callProcedure("InsertRegion",
                                  regionId,
                                  cattrs.getCOUNTYID(),
                                  componentCount,
                                  0,
                                  vertarray);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            System.err.printf("Cannot insert county boundary %d.%d (county %s)\n",
                              regionId, cattrs.getCOUNTYID(), cattrs.getNAME());
            System.exit(100);
        }
    }
    private static Integer regionId = 0;
    private void insertCountyBoundaries(CountyData cattrs)
            throws IOException, ProcCallException {
        CsvBeanReader beanReader = null;
        InputStream is = null;
        int numBoundaries = 0;
        int numCounties = 0;
        try {
            is = Benchmark.class.getResourceAsStream("data/counties_geometry.csv");
            beanReader = new CsvBeanReader(new InputStreamReader(is),
                                           CsvPreference.TAB_PREFERENCE);
            final String[] header = beanReader.getHeader(true);
            final CellProcessor[] processors = getCountyBoundaryProcessors();
            CountyBoundaryBean countyBoundary;
            String lastShapeId = null;
            ArrayList<S2LatLng> currentLoop = null;
            ArrayList<ArrayList<S2LatLng>> currentPolygon = null;
            Long currentCountyId = -1L;
            int componentCount = 0;
            while ( (countyBoundary = beanReader.read(CountyBoundaryBean.class, header, processors)) != null) {
                if (lastShapeId == null) {
                    // First boundary.  Initialize everything.
                    currentPolygon = new ArrayList<ArrayList<S2LatLng>>();
                    currentLoop = new ArrayList<S2LatLng>();
                    lastShapeId = countyBoundary.getSHAPEID();
                    currentCountyId = cattrs.getCountyId(countyBoundary.getSHAPEID());
                    componentCount = 0;
                } else if (false == countyBoundary.getSHAPEID().equals(lastShapeId)) {
                    // New shapeid.
                    //   Add the current loop.
                    //   If it's a new county, then write the current boundary.
                    //   Initialize for the next loop.
                    currentPolygon.add(currentLoop);
                    // Is this a new county?
                    // Hope the counties are all in order in the
                    // csv file!!
                    if (currentCountyId != cattrs.getCountyId(countyBoundary.getSHAPEID())) {
                        insertOneCountyBoundary(++regionId, componentCount,
                                                cattrs.getByShapeID(lastShapeId),
                                                currentPolygon);
                        numCounties += 1;
                        currentPolygon = new ArrayList<ArrayList<S2LatLng>>();
                    }
                    componentCount += 1;
                    numBoundaries += 1;
                    currentLoop = new ArrayList<S2LatLng>();
                    lastShapeId = countyBoundary.getSHAPEID();
               }
                currentLoop.add(S2LatLng.fromDegrees(countyBoundary.getX(), countyBoundary.getY()));
            }
            if (currentLoop.size() > 0) {
                currentPolygon.add(currentLoop);
            }
            if (currentPolygon.size() > 0) {
                insertOneCountyBoundary(++regionId, componentCount,
                                        cattrs.getByShapeID(lastShapeId),
                                        currentPolygon);
                numBoundaries += 1;
            }
        } finally {
            try {
                if (beanReader != null) {
                    beanReader.close();
                }
            } catch (IOException e) {
                ;
            }
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                ;
            }
        }
        System.out.printf("Inserted %d counties, %d county boundaries.\n",
                          numCounties,
                          numBoundaries);
    }
    private void insertStates() {
        // TODO Auto-generated method stub

    }

    /**
     * Sets up the processors used for the examples. There are 10 CSV columns, so 10 processors are defined. Empty
     * columns are read as null (hence the NotNull() for mandatory columns).
     *
     * @return the cell processors
     */
    private static CellProcessor[] getCityProcessors() {

        final CellProcessor[] processors = new CellProcessor[] {
                new NotNull(), // USPS
                new UniqueHashCode(), // GEOID
                new UniqueHashCode(), // ANSICODE
                new NotNull(), // NAME
                new NotNull(), // LSAD
                new NotNull(), // FUNCSTAT
                new NotNull(), // ALAND
                new NotNull(), // AWATER
                new NotNull(), // ALAND_SQMI
                new NotNull(), // AWATER_SQMI
                new ParseDouble(), // INTPTLAT
                new ParseDouble() // INTPTLONG
        };
        return processors;
    }

    @SuppressWarnings("unused")
    public static class CityBean {
        String USPS;
        String GEOID;
        String ANSICODE;
        String NAME;
        String LSAD;
        String FUNCSTAT;
        String ALAND;
        String AWATER;
        String ALAND_SQMI;
        String AWATER_SQMI;
        Double INTPTLAT;
        Double INTPTLONG;
        public final String getUSPS() {
            return USPS;
        }
        public final void setUSPS(String uSPS) {
            USPS = uSPS;
        }
        public final String getGEOID() {
            return GEOID;
        }
        public final void setGEOID(String gEOID) {
            GEOID = gEOID;
        }
        public final String getANSICODE() {
            return ANSICODE;
        }
        public final void setANSICODE(String aNSICODE) {
            ANSICODE = aNSICODE;
        }
        public final String getNAME() {
            return NAME;
        }
        public final void setNAME(String nAME) {
            NAME = nAME;
        }
        public final String getLSAD() {
            return LSAD;
        }
        public final void setLSAD(String lSAD) {
            LSAD = lSAD;
        }
        public final String getFUNCSTAT() {
            return FUNCSTAT;
        }
        public final void setFUNCSTAT(String fUNCSTAT) {
            FUNCSTAT = fUNCSTAT;
        }
        public final String getALAND() {
            return ALAND;
        }
        public final void setALAND(String aLAND) {
            ALAND = aLAND;
        }
        public final String getAWATER() {
            return AWATER;
        }
        public final void setAWATER(String aWATER) {
            AWATER = aWATER;
        }
        public final String getALAND_SQMI() {
            return ALAND_SQMI;
        }
        public final void setALAND_SQMI(String aLAND_SQMI) {
            ALAND_SQMI = aLAND_SQMI;
        }
        public final String getAWATER_SQMI() {
            return AWATER_SQMI;
        }
        public final void setAWATER_SQMI(String aWATER_SQMI) {
            AWATER_SQMI = aWATER_SQMI;
        }
        public final Double getINTPTLAT() {
            return INTPTLAT;
        }
        public final void setINTPTLAT(Double iNTPTLAT) {
            INTPTLAT = iNTPTLAT;
        }
        public final Double getINTPTLONG() {
            return INTPTLONG;
        }
        public final void setINTPTLONG(Double iNTPTLONG) {
            INTPTLONG = iNTPTLONG;
        }
    }
    private void insertCities() {
        System.out.printf("Inserting cities...\n");
        CsvBeanReader beanReader = null;
        long id = 0;
        try {
            InputStream is = Benchmark.class.getResourceAsStream("data/places.csv");
            beanReader = new CsvBeanReader(new InputStreamReader(is),
                                                         CsvPreference.TAB_PREFERENCE);
            final String[] header = beanReader.getHeader(true);
            final CellProcessor[] processors = getCityProcessors();
            CityBean city;
            while ( (city = beanReader.read(CityBean.class, header, processors)) != null) {
                ClientResponse cr = client.callProcedure("InsertCity",
                                                         ++id,
                                                         city.getNAME(),
                                                         city.getINTPTLAT(),
                                                         city.getINTPTLONG());
                if (cr.getStatus() != ClientResponse.SUCCESS) {
                    System.err.printf("Cannot insert row %d (city %s)\n", id, city.getNAME());
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ProcCallException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                beanReader.close();
            } catch (IOException e) {
                ;
            }
        }
        System.out.printf("Inserted %d cities\n", id);
    }

    private void setUpTaxis() {
        TaxiManager tm = new TaxiManager(client);

        // Create the initial locations of all the taxis
        tm.createTaxis();

        // Update the location of taxis periodically
        scheduler.scheduleWithFixedDelay(tm, TaxiManager.getUpdateInterval(), TaxiManager.getUpdateInterval(), TimeUnit.MILLISECONDS);
    }

    /**
     * Main routine creates a benchmark instance and kicks off the run method.
     *
     * @param args Command line arguments.
     * @throws Exception if anything goes wrong.
     * @see {@link Config}
     */
    public static void main(String[] args) throws Exception {
        // create a configuration from the arguments
        Config config = new Config();
        config.parse(Benchmark.class.getName(), args);

        Benchmark benchmark = new Benchmark(config);
        benchmark.runBenchmark();
    }
}
