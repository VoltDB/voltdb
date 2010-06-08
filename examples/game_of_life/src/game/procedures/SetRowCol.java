package game.procedures;

import org.voltdb.*;

@ProcInfo
(
    singlePartition = true,
    partitionInfo = "metadata.pk: 0"
)
public class SetRowCol extends VoltProcedure
{
    public static final SQLStmt SQLinsert = new SQLStmt
    (
        "INSERT INTO metadata " +
        "VALUES (?, ?, ?, ?);"
    );

    public long run(int pk, int numRows, int numCols, int generation)
        throws VoltAbortException
    {
        voltQueueSQL(SQLinsert, pk, numRows, numCols, generation);

        VoltTable[] tables = voltExecuteSQL();
        //assert correctness
        return 1;
    }
}