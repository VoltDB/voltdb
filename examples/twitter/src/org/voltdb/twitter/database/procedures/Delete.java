package org.voltdb.twitter.database.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;


@ProcInfo(
        singlePartition = false
)
public class Delete extends VoltProcedure {
    
    public final SQLStmt SQL = new SQLStmt(
            "DELETE " +
            "FROM hashtags " +
            "WHERE tweet_timestamp < ?;");
    
    public VoltTable[] run(long deleteAllEarlierThan) throws VoltAbortException {
        // execute query
        voltQueueSQL(SQL, deleteAllEarlierThan);
        return voltExecuteSQL(true);
    }
    
}