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
