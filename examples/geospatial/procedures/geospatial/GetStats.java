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

/**
 * Get statistics on ad requests for the most recent time period
 * in seconds, specified by the caller.
 *
 * These SQL statements make use of a materialized view so they
 * can execute data quickly.
 */
public class GetStats extends VoltProcedure {

    /*
     * Find out the ratio of successful bids to unsuccessful ones.
     */
    final SQLStmt selectHitsAndNonHits = new SQLStmt(
            "select "
            + "  case when advertiser_id is not null "
            + "    then 1 else 0 end as hit_or_no_hit, "
            + "  sum(request_count) "
            + "from requests_by_second_by_advertiser "
            + "where ts_second > dateadd(second, ?, current_timestamp) "
            + "group by hit_or_no_hit "
            + "order by hit_or_no_hit");

    /*
     * Find the top 5 advertisers of the past <n> seconds, ordered
     * by the sum of all the winning bids.
     */
    final SQLStmt selectRecentAdvertisers = new SQLStmt(
            "select "
            + "  adv.name as advertiser, "
            + "  sum(vw.sum_of_ad_revenue) total_revenue, "
            + "  sum(vw.request_count) as num_requests "
            + "from requests_by_second_by_advertiser as vw "
            + "inner join advertisers as adv "
            + "on vw.advertiser_id = adv.id "
            + "where ts_second > dateadd(second, ?, current_timestamp) "
            + "group by adv.name "
            + "order by total_revenue desc "
            + "limit 5");

    public VoltTable[] run(long numSeconds) {
        voltQueueSQL(selectHitsAndNonHits, -numSeconds);
        voltQueueSQL(selectRecentAdvertisers, -numSeconds);
        return voltExecuteSQL(true);
    }
}
