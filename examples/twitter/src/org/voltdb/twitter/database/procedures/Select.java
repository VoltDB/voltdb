package org.voltdb.twitter.database.procedures;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.twitter.util.HashTag;

@ProcInfo(
        singlePartition = true,
        partitionInfo = "partitioner.gotoall: 0"
)
public class Select extends VoltProcedure {
    
    public final SQLStmt SQL = new SQLStmt(
            "SELECT hashtag, COUNT(*) AS hashcount " +
            "FROM hashtags " +
            "WHERE tweet_timestamp > ? " +
            "GROUP BY hashtag;");
    
    public VoltTable[] run(int partition, long maxAgeMicros, int limit) throws VoltAbortException {
        // execute query
        voltQueueSQL(SQL, maxAgeMicros);
        VoltTable table = voltExecuteSQL(true)[0];
        
        // stop here if the database is empty
        int rowCount = table.getRowCount();
        if (rowCount == 0) {
            return null;
        }
        
        // convert the VoltTable into a java list for sorting purposes
        List<HashTag> hashTags = new LinkedList<HashTag>();
        VoltTableRow row = table.fetchRow(0);
        for (int i = 0; i < rowCount; i++) {
            hashTags.add(new HashTag(row.getString(0), (int) row.getLong(1), row.cloneRow()));
            row.advanceRow();
        }
        
        // sort hashtags by count
        Collections.sort(hashTags);
        
        // construct a new VoltTable using the schema of the above VoltTable
        int columnCount = table.getColumnCount();
        String[] columnNames = new String[columnCount];
        VoltType[] columnTypes = new VoltType[columnCount];
        for (int i = 0; i < columnNames.length; i++) {
            columnNames[i] = table.getColumnName(i);
            columnTypes[i] = table.getColumnType(i);
        }
        VoltTable.ColumnInfo[] columnInfo = new VoltTable.ColumnInfo[columnCount];
        for (int i = 0; i < columnNames.length; i++) {
            columnInfo[i] = new VoltTable.ColumnInfo(columnNames[i], columnTypes[i]);
        }
        VoltTable tableWithLimit = new VoltTable(columnInfo);
        
        // populate the new table (up to the limit)
        int hashTagsSize = hashTags.size();
        for (int i = 0; i < limit && i < hashTagsSize; i++) {
            tableWithLimit.add(hashTags.get(i).getRow());
        }
        
        return new VoltTable[] {tableWithLimit};
    }
    
}