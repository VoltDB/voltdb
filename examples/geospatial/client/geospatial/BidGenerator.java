/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

/**
 * This class is a Runnable subclass that will be scheduled by the AdBrokerBenchmark instance.
 *
 * It simulates the generation of bids by advertisers.  That is, its runnable method will insert
 * rows into the BIDS table, indicating how much a business will pay for an ad shown during a
 * particular duration of time to a device within a specified region (represented in the
 * BIDS table as GEOGRAPHY value, i.e., a polygon).
 */
public class BidGenerator implements Runnable {

    // A random number generator for generating bids
    private final Random m_rand;

    // A connection to the database, initialized by whoever invokes constructor
    private final Client m_client;

    // The current highest bid id
    private long m_bidId;

    // The number of advertisers in the ADVERTISERS table
    private final long NUM_ADVERTISERS;

    /*
     * Construct a bid generator instance.
     */
    BidGenerator(Client client) throws Exception {
        m_rand = new Random(777);
        m_client = client;

        NUM_ADVERTISERS = m_client.callProcedure("@AdHoc", "select count(*) from advertisers")
                .getResults()[0].asScalarLong();
        m_bidId = getMaxBidId(m_client);
    }

    /**
     * Find the current highest bid id in the bids table.  We'll start generating
     * new bids at this number plus one.
     * @param client    A connection to the database
     * @return current highest bid id
     */
    private static long getMaxBidId(Client client) {
        long currentMaxBidId = 0;
        try {
            VoltTable vt = client.callProcedure("@AdHoc", "select max(id) from bids").getResults()[0];
            vt.advanceRow();
            currentMaxBidId = vt.getLong(0);
            if (vt.wasNull()) {
                currentMaxBidId = 0;
            }
        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }

        return currentMaxBidId;
    }

    /**
     * This is the "run" method for this Runnable subclass.
     *
     * Generate one new row for the bids table, and insert it.
     */
    @Override
    public void run() {
        long bidId = m_bidId++;
        long advertiserId = Math.abs(m_rand.nextLong()) % NUM_ADVERTISERS;
        GeographyValue bidRegion = Regions.pickRandomRegion();
        TimestampType bidStartTime = new TimestampType();
        TimestampType bidEndTime = new TimestampType(
                bidStartTime.getTime() + AdBrokerBenchmark.BID_DURATION_SECONDS * 1000000);

        // Amount of bid: a hundredth of a penny up to around a tenth of a penny.
        double amount = 0.00001 + 0.01 * m_rand.nextDouble();
        DecimalFormat df = new DecimalFormat("#.####");
        amount = Double.valueOf(df.format(amount));

        try {
            m_client.callProcedure(new NullCallback(), "bids.Insert",
                    bidId,
                    advertiserId,
                    bidRegion,
                    bidStartTime,
                    bidEndTime,
                    amount);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
