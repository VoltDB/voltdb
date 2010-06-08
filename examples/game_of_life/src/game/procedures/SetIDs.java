package game.procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = true,
    partitionInfo = "cells.cell_id: 0"
)

public class SetIDs extends VoltProcedure
{
    public static final SQLStmt SQLinsert = new SQLStmt
    (
        "INSERT INTO cells " +
        "VALUES (?, ?);"
    );

    public long run(int cell_id, int occupy)
        throws VoltAbortException
    {
        voltQueueSQL(SQLinsert, cell_id, occupy);

        VoltTable[] tables = voltExecuteSQL();
        //assert correctness
        return 1;
    }
}