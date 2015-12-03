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

package geospatial;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.PointType;
import org.voltdb.types.TimestampType;

/**
 * This class is a Runnable subclass that will be scheduled by the AdBrokerBenchmark instance.
 *
 * It simulates the generation of bids by advertisers.  That is, its runnable method will insert
 * rows into the BIDS table, indicating how much a business will pay for an ad show during a
 * particular duration of time to a device within a specified region (represented in the
 * BIDS table as GEOGRAPHY value).
 */
public class BidGenerator implements Runnable {

    private final AdBrokerBenchmark.AdBrokerConfig m_config;
    private final Client m_client;
    private final List<GeographyValue> m_bidRegions;

    BidGenerator(AdBrokerBenchmark.AdBrokerConfig config) throws Exception {
        m_config = config;

        ClientConfig clientConfig = new ClientConfig(m_config.user, m_config.password, null);
        clientConfig.setMaxTransactionsPerSecond(m_config.ratelimit);
        m_client = ClientFactory.createClient(clientConfig);
        AdBrokerBenchmark.connect(m_client, config.servers);

        m_bidRegions = getRandomRegions();
    }

    /**
     * For now, just produce rectangular regions.
     */
    private List<GeographyValue> getRandomRegions() {
        Random rand = new Random(777);
        List<GeographyValue> regions = new ArrayList<GeographyValue>();

        // Units here are degrees.  This is around 100 yards near the equator.
        final double MIN_SIDE_LENGTH = 0.000823451910;
        final double MAX_SIDE_LENGTH = MIN_SIDE_LENGTH * 5.0;

        final double LNG_MIN = AdBrokerBenchmark.BID_AREA_LNG_MIN;
        final double LNG_MAX = AdBrokerBenchmark.BID_AREA_LNG_MAX - MAX_SIDE_LENGTH;
        final double LAT_MIN = AdBrokerBenchmark.BID_AREA_LAT_MIN;
        final double LAT_MAX = AdBrokerBenchmark.BID_AREA_LAT_MAX - MAX_SIDE_LENGTH;

        for (int i = 0; i < AdBrokerBenchmark.NUM_BID_REGIONS; ++i) {

            double d = rand.nextDouble(); // between 0 (inclusive) and 1 (exclusive)
            double regLngMin = LNG_MIN + d * (LNG_MAX - LNG_MIN);
            d = rand.nextDouble();
            double regLatMin = LAT_MIN + d * (LAT_MAX - LAT_MIN);

            // Region between 100 and 500 yards.
            d = rand.nextDouble();
            double lngSideLength = MIN_SIDE_LENGTH + d * (MAX_SIDE_LENGTH - MIN_SIDE_LENGTH);
            d = rand.nextDouble();
            double latSideLength = MIN_SIDE_LENGTH + d * (MAX_SIDE_LENGTH - MIN_SIDE_LENGTH);

            // These (lat, lng) pairs will need to be swapped to (lng, lat).
            List<PointType> ring = new ArrayList<PointType>();
            ring.add(new PointType(regLatMin,                 regLngMin));
            ring.add(new PointType(regLatMin,                 regLngMin + lngSideLength));
            ring.add(new PointType(regLatMin + latSideLength, regLngMin + lngSideLength));
            ring.add(new PointType(regLatMin + latSideLength, regLngMin));
            ring.add(new PointType(regLatMin,                 regLngMin));
            List<List<PointType>> rings = new ArrayList<List<PointType>>();
            rings.add(ring);
            regions.add(new GeographyValue(rings));
        }

        return regions;
    }

    /** Just to test. Remove this later. */
    void generateOneBid() throws Exception {
        TimestampType now = new TimestampType();
        TimestampType later = new TimestampType(now.getTime() + 5 * 60 * 1000000);
        m_client.callProcedure("bids.Insert", 0, 0, m_bidRegions.get(0), now, later, 1.25);

        System.out.println("Successfully inserted test row!");
    }

    @Override
    public void run() {
        // Code to generate bids periodically will go here.
    }

    public void dumpRegions() {
        for (GeographyValue gv : m_bidRegions) {
            System.out.println(gv.toString());
        }
    }

}
