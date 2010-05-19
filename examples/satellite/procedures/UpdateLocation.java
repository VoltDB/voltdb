package procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = true,
    partitionInfo = "LOCATION.ID: 0"
)
public class UpdateLocation extends VoltProcedure {

    public final SQLStmt updateItem =
        new SQLStmt("UPDATE LOCATION " +
                    "SET LATITUDE=?, LONGITUDE=? " +
                    "WHERE ID=?;");

    public VoltTable[] run( int id, double latitude, double longitude) throws VoltAbortException {
        // Add a SQL statement to the execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( updateItem, latitude, longitude, id);

        // Run all queued queries.
        VoltTable[] queryresults = voltExecuteSQL();
        return queryresults;
    }
}
