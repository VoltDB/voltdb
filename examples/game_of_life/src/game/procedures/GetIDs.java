package game.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;

@ProcInfo
(
    singlePartition = false
)
public class GetIDs extends VoltProcedure
{
    public static final SQLStmt SQLquery = new SQLStmt
    (
        "SELECT cell_id, occupied " +
        "FROM cells;"
    );

    //returns VoltTable with column of neighbor ids
    public VoltTable run()
        throws VoltAbortException
    {
        voltQueueSQL(SQLquery);

        VoltTable[] tables = voltExecuteSQL();
        assert(tables.length == 1);

        return tables[0];
    }
}