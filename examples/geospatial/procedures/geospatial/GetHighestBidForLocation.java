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

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.GeographyPointValue;

/**
 * This subclass of VoltProcedure is invoked when simulating a request
 * from a mobile device for an ad.  Given the location of the device,
 * it finds the bid with the highest dollar amount where the device is within
 * the ad's bid region, and the current time is within the bids start
 * and end parameters
 *
 * It also updates the historical table ad_requests with info on the winning
 * bid (if any---nulls are added to the ad_requests row for ad requests that
 * are not met).
 *
 * The bid id for the successful bid is returned, or -1 if no such bids exist.
 *
 * This is a single-partition stored procedure, so it can execute on many sites
 * in parallel.
 */
public class GetHighestBidForLocation extends VoltProcedure {

    final SQLStmt getHighestBidStmt = new SQLStmt(
            "select id, advertiser_id, bid_amount "
            + "from bids "
            + "where current_timestamp between ts_start and ts_end "
            + "  and contains(region, ?) "
            + "order by bid_amount desc, id "
            + "limit 1;");

    final SQLStmt insertRequestStmt = new SQLStmt(
            "insert into ad_requests values (?, current_timestamp, ?, ?, ?);");

    public long run(long deviceId, GeographyPointValue point) {
        long matchingBidId = -1;

        // Find a matching bid for this location and the current time.
        voltQueueSQL(getHighestBidStmt, point);
        VoltTable vt = voltExecuteSQL()[0];
        if (vt.getRowCount() > 0) {
            // We found a match!
            vt.advanceRow();

            matchingBidId = vt.getLong(0);
            long advertiserId = vt.getLong(1);
            double bidAmount = vt.getDouble(2);

            voltQueueSQL(insertRequestStmt, deviceId, matchingBidId, advertiserId, bidAmount);
        }
        else {
            // No applicable bids.  Insert a row containing
            // null values to show that a device appeared
            // but no ad was served.
            voltQueueSQL(insertRequestStmt, deviceId, null, null, null);
        }

        // Update ad_requests with the result of the request.
        voltExecuteSQL(true);

        return matchingBidId;
    }
}
