package org.voltdb.twitter.database.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
        singlePartition = true,
        partitionInfo = "hashtags.hashtag: 0"
)
public class Insert extends VoltProcedure {
    
    public final SQLStmt SQL = new SQLStmt(
            "INSERT INTO hashtags (hashtag, tweet_timestamp) " +
            "VALUES (?, ?);");
    
    public VoltTable[] run(String hashTag, long tweetTimestamp) throws VoltAbortException {
        // execute query
        voltQueueSQL(SQL, hashTag, tweetTimestamp);
        return voltExecuteSQL(true);
    }
    
}