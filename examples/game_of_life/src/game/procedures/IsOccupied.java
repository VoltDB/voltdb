package game.procedures;

import org.voltdb.*;

@ProcInfo
(
    singlePartition = true,
    partitionInfo = "cells.cell_id: 0"
)
public class IsOccupied extends VoltProcedure
{
    public static final SQLStmt SQLquery = new SQLStmt
    (
        "SELECT occupied " +
        "FROM cells " +
        "WHERE cell_id = ?;"
    );

    //returns 1 if cell now alive, 0 if cell now dead
    public long run(int id)
        throws VoltAbortException
    {
        voltQueueSQL(SQLquery, id);

        VoltTable[] tables = voltExecuteSQL();
        assert(tables.length == 1);

        VoltTable table = tables[0];
        assert(table.getRowCount() == 1 && table.getColumnCount() == 1);

        VoltTableRow row = table.fetchRow(0);
        //assert type is TINYINT

        return row.getLong(0);
    }
}