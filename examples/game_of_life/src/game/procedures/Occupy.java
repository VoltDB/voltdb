package game.procedures;

import org.voltdb.*;

@ProcInfo
(
    singlePartition = true,
    partitionInfo = "cells.cell_id: 1"
)
public class Occupy extends VoltProcedure
{
    public static final SQLStmt SQLupdate = new SQLStmt
    (
        "UPDATE cells " +
        "SET occupied = ? " +
        "WHERE cell_id = ?;"
    );

    public long run(int occupy, int id)
        throws VoltAbortException
    {
        voltQueueSQL(SQLupdate, occupy, id);

        VoltTable[] tables = voltExecuteSQL();
        //assert correctness
        return 1;
    }
}