package procedures;

import org.voltdb.*;

@ProcInfo(
   singlePartition = true,
   partitionInfo = "SATELLITE.ID: 0"
)
public class GetSatelliteData extends VoltProcedure {

    public final SQLStmt selectItem =
        new SQLStmt("SELECT MODEL_NUMBER, COUNTRY " +
                    "FROM SATELLITE WHERE ID=?;");

    public VoltTable[] run(long id) throws VoltAbortException {
        // Add a SQL statement to the current execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( selectItem, id );

        // Run all queued queries.
        return voltExecuteSQL();
    }
}

