package procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)
public class GetLocationData extends VoltProcedure {

    public final SQLStmt selectAll =
        new SQLStmt("SELECT ID, LATITUDE, LONGITUDE " +
                    "FROM LOCATION;");

    public VoltTable[] run() throws VoltAbortException {
        // Add a SQL statement to the current execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( selectAll );

        // Run all queued queries.
        return voltExecuteSQL();
    }
}
