package game.procedures;

import org.voltdb.*;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo(
    singlePartition = true,
    partitionInfo = "cells.cell_id: 0"
)
public class GetGeneration extends VoltProcedure
{
    public static final SQLStmt SQLquery = new SQLStmt
    (
        "SELECT generation " +
        "FROM metadata " +
        "WHERE pk = ?;"
    );

    //returns 1 if cell now alive, 0 if cell now dead
    public long run(int pk)
        throws VoltAbortException
    {
        voltQueueSQL(SQLquery, pk);

        VoltTable[] tables = voltExecuteSQL();
        assert(tables.length == 1);

        VoltTable table = tables[0];
        assert(table.getRowCount() == 1 && table.getColumnCount() == 1);

        VoltTableRow row = table.fetchRow(0);
        //assert type is TINYINT

        return row.getLong(0);
    }
}