package game.procedures;

import org.voltdb.*;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo
(
    singlePartition = true,
    partitionInfo = "metadata.pk: 0"
)
public class UpdateGeneration extends VoltProcedure
{
    public static final SQLStmt SQLupdate = new SQLStmt
    (
        "UPDATE metadata " +
        "SET generation = generation + 1 " +
        "WHERE pk = ?;"
    );

    public long run(int pk)
        throws VoltAbortException
    {
        voltQueueSQL(SQLupdate, pk);

        VoltTable[] tables = voltExecuteSQL();
        //assert correctness
        return 1;
    }
}