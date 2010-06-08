package game.procedures;

import org.voltdb.*;

@ProcInfo
(
    singlePartition = true,
    partitionInfo = "metadata.pk: 0"
)
public class GetRowCol extends VoltProcedure
{
    public static final SQLStmt SQLquery = new SQLStmt
    (
        "SELECT numrows, numcols " +
        "FROM metadata " +
        "WHERE pk = ?;"
    );

    public VoltTable run(int pk)
        throws VoltAbortException
    {
        voltQueueSQL(SQLquery, pk);

        VoltTable[] tables = voltExecuteSQL();
        assert(tables.length == 1);

        VoltTable table = tables[0];
        assert(table.getRowCount() == 1 && table.getColumnCount() == 2);

        return table;
    }
}