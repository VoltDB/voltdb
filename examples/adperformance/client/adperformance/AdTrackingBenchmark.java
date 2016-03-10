/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package adperformance;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Random;

import org.voltdb.types.TimestampType;

public class AdTrackingBenchmark extends BaseBenchmark {

    private Random rand = new Random();
    private MathContext mc = new MathContext(2);
    private BigDecimal bd0 = new BigDecimal(0);
    private long startTime = new TimestampType(System.currentTimeMillis()*1000).getTime();

    // inventory pre-sets
    private int sites = 1000;
    private int pagesPerSite = 20;

    // creatives pre-sets
    private int advertisers = 1000;
    private int campaignsPerAdvertiser = 10;
    private int creativesPerCampaign = 10;
    private int modulus = 100;

    // counters
    private int inventoryMaxID = 0;
    private int creativeMaxID = 0;
    private long iteration = 0L;

    // constructor
    public AdTrackingBenchmark(BenchmarkConfig config) {
        super(config);

        // set any instance attributes here
        sites = config.sites;
        pagesPerSite = config.pagespersite;
        advertisers = config.advertisers;
        campaignsPerAdvertiser = config.campaignsperadvertiser;
        creativesPerCampaign = config.creativespercampaign;
        modulus = creativesPerCampaign*3;
        creativeMaxID = advertisers * campaignsPerAdvertiser * creativesPerCampaign;
    }

    @Override
    public void initialize() throws Exception {

        // generate inventory
        System.out.println("Loading Inventory table based on " + sites +
                           " sites and " + pagesPerSite + " pages per site...");
        for (int i=1; i<=sites; i++) {
            for (int j=1; j<=pagesPerSite; j++) {
                inventoryMaxID++;
                client.callProcedure(new BenchmarkCallback("INVENTORY.insert"),
                                     "INVENTORY.insert",
                                     inventoryMaxID,
                                     i,
                                     j);
                // show progress
                if (inventoryMaxID % 5000 == 0) System.out.println("  " + inventoryMaxID);
            }
        }

        // generate creatives
        System.out.println("Loading Creatives table based on " + advertisers +
                           " advertisers, each with " + campaignsPerAdvertiser +
                           " campaigns, each with " + creativesPerCampaign + " creatives...");
        client.callProcedure(new BenchmarkCallback("InitializeCreatives"),
                             "InitializeCreatives",
                             advertisers,
                             campaignsPerAdvertiser,
                             creativesPerCampaign);
    }

    @Override
    public void iterate() throws Exception {

        // generate an impression

        // each iteration is 1 millisecond later
        // the faster the throughput rate, the faster time flies!
        // this is to get more interesting hourly or minutely results
        iteration++;
        TimestampType ts = new TimestampType(startTime+(iteration*1000));

        // random IP address
        int ipAddress =
            rand.nextInt(256)*256*256*256 +
            rand.nextInt(256)*256*256 +
            rand.nextInt(256)*256 +
            rand.nextInt(256);

        long cookieUID = rand.nextInt(1000000000);
        int creative = rand.nextInt(creativeMaxID)+1;
        int inventory = rand.nextInt(inventoryMaxID)+1;
        BigDecimal cost = new BigDecimal(rand.nextDouble()/5,mc);

        client.callProcedure(new BenchmarkCallback("TrackEvent"),
                             "TrackEvent",
                             ts,
                             ipAddress,
                             cookieUID,
                             creative,
                             inventory,
                             0,
                             cost);

        int i = rand.nextInt(100);
        int r = creative % modulus;
        // sometimes generate a click-through
        if ( (r==0 && i<10) || i == 0) { // 1% of the time at least, for 1/3 of campaigns 10% of the time
            client.callProcedure(new BenchmarkCallback("TrackEvent"),
                                 "TrackEvent",
                                 ts,
                                 ipAddress,
                                 cookieUID,
                                 creative,
                                 inventory,
                                 1,
                                 bd0);

            // 33% conversion rate
            if ( rand.nextInt(2) == 0 ) {
                client.callProcedure(new BenchmarkCallback("TrackEvent"),
                                     "TrackEvent",
                                     ts,
                                     ipAddress,
                                     cookieUID,
                                     creative,
                                     inventory,
                                     2,
                                     bd0);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("AdTrackingBenchmark",args);

        BaseBenchmark benchmark = new AdTrackingBenchmark(config);
        benchmark.runBenchmark();

    printHeading("Note: The database must be restarted before running this benchmark again.");

    }
}
