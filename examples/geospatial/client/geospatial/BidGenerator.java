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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.GeographyPointValue;
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

    private final Random m_rand;
    private final Client m_client;
    private final List<GeographyValue> m_bidRegions;
    private long m_bidId;

    private final long NUM_ADVERTISERS;

    BidGenerator(AdBrokerBenchmark.AdBrokerConfig config, Client client) throws Exception {
        m_rand = new Random(777);
        //m_config = config;
        m_client = client;
        m_bidRegions = getRandomRegions();

        NUM_ADVERTISERS = m_client.callProcedure("@AdHoc", "select count(*) from advertisers")
                .getResults()[0].asScalarLong();
        m_bidId = m_client.callProcedure("@AdHoc", "select max(id) from bids")
                .getResults()[0].asScalarLong() + 1;
    }

    /**
     * For now, just produce rectangular regions.
     */
    private List<GeographyValue> getRandomRegions() {
        List<GeographyValue> regions = new ArrayList<GeographyValue>();

        // Units here are degrees.  This is around 100 yards near the equator.
        final double MIN_SIDE_LENGTH = 0.000823451910;
        final double MAX_SIDE_LENGTH = MIN_SIDE_LENGTH * 5.0;

        final double LNG_MIN = AdBrokerBenchmark.BID_AREA_LNG_MIN;
        final double LNG_MAX = AdBrokerBenchmark.BID_AREA_LNG_MAX - MAX_SIDE_LENGTH;
        final double LAT_MIN = AdBrokerBenchmark.BID_AREA_LAT_MIN;
        final double LAT_MAX = AdBrokerBenchmark.BID_AREA_LAT_MAX - MAX_SIDE_LENGTH;

        for (int i = 0; i < AdBrokerBenchmark.NUM_BID_REGIONS; ++i) {

            double d = m_rand.nextDouble(); // between 0 (inclusive) and 1 (exclusive)
            double regLngMin = LNG_MIN + d * (LNG_MAX - LNG_MIN);
            d = m_rand.nextDouble();
            double regLatMin = LAT_MIN + d * (LAT_MAX - LAT_MIN);

            // Sides of region are between 100 and 500 yards.
            d = m_rand.nextDouble();
            double lngSideLength = MIN_SIDE_LENGTH + d * (MAX_SIDE_LENGTH - MIN_SIDE_LENGTH);
            d = m_rand.nextDouble();
            double latSideLength = MIN_SIDE_LENGTH + d * (MAX_SIDE_LENGTH - MIN_SIDE_LENGTH);

            List<GeographyPointValue> ring = new ArrayList<GeographyPointValue>();
            ring.add(new GeographyPointValue(regLngMin,                 regLatMin));
            ring.add(new GeographyPointValue(regLngMin + lngSideLength, regLatMin));
            ring.add(new GeographyPointValue(regLngMin + lngSideLength, regLatMin + latSideLength));
            ring.add(new GeographyPointValue(regLngMin,                 regLatMin + latSideLength));
            ring.add(new GeographyPointValue(regLngMin,                 regLatMin));
            List<List<GeographyPointValue>> rings = new ArrayList<List<GeographyPointValue>>();
            rings.add(ring);
            regions.add(new GeographyValue(rings));
        }

        return regions;
    }

    /**
     * Generate one new row for the bids table, and insert it.
     * Also delete any expired bids.
     */
    @Override
    public void run() {
        long bidId = m_bidId++;
        long advertiserId = m_rand.nextLong() % NUM_ADVERTISERS;
        GeographyValue bidRegion = m_bidRegions.get(m_rand.nextInt(m_bidRegions.size()));
        TimestampType bidStartTime = new TimestampType();
        TimestampType bidEndTime = new TimestampType(
                bidStartTime.getTime() + AdBrokerBenchmark.BID_DURATION_SECONDS * 1000000);

        // Amount of bid: at least a tenth of a penny, up to a max of $1.001.
        double amount = 0.001 + m_rand.nextDouble();
        DecimalFormat df = new DecimalFormat("#.###");
        amount = Double.valueOf(df.format(amount));

        try {
            m_client.callProcedure(new NullCallback(), "bids.Insert",
                    bidId,
                    advertiserId,
                    bidRegion,
                    bidStartTime,
                    bidEndTime,
                    amount);

            m_client.callProcedure(new NullCallback(), "DeleteExpiredBids");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void dumpRegions() {
        for (GeographyValue gv : m_bidRegions) {
            System.out.println(gv.toString());
        }
    }

}
