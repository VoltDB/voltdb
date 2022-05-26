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

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;

/**
 * A subclass of Runnable to be invoked periodically to purge
 * no-longer-useful records from the database.
 */
public class NibbleDeleter implements Runnable {

    // A connection to the database.  Initialized by whoever invokes the constructor.
    private final Client m_client;

    // The age after which to consider old data purgeable.
    private final long m_expiredAgeInSeconds;

    /**
     * Construct an instance of this class
     */
    NibbleDeleter(Client client, long expiredAge) {
        m_client = client;
        m_expiredAgeInSeconds = expiredAge;
    }

    /**
     * Remove aged-out data from the ad_requests table.  This table is partitioned,
     * and may be large, so use the "run-everywhere" pattern to minimize impact
     * to throughput.
     *
     * Also remove old expired bids from the bids table.
     */
    @Override
    public void run() {
        try {
            VoltTable partitionKeys = null;
            partitionKeys = m_client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];

            while (partitionKeys.advanceRow()) {
                m_client.callProcedure(new NullCallback(), "DeleteOldAdRequests",
                        partitionKeys.getLong("PARTITION_KEY"),
                        m_expiredAgeInSeconds);
            }

            m_client.callProcedure(new NullCallback(), "DeleteExpiredBids");
        }
        catch (IOException | ProcCallException ex) {
            ex.printStackTrace();
        }
    }
}
