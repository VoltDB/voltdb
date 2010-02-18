package procedures;

import org.voltdb.*;

@ProcInfo(
    singlePartition = false
)
public class AddSatellite extends VoltProcedure {

    public final SQLStmt getLastId =
        new SQLStmt("SELECT ID FROM SATELLITE " +
                    " ORDER BY ID DESC LIMIT 1;");
    public final SQLStmt insertSatItem =
        new SQLStmt("INSERT INTO SATELLITE (ID,MODEL_NUMBER,COUNTRY) " +
                    "VALUES (?,?,?);");
    public final SQLStmt insertLocItem =
        new SQLStmt("INSERT INTO LOCATION (ID,LATITUDE,LONGITUDE) " +
                    "VALUES (?,?,?);");

    public long run( long id, String model, String country,
                     double latitude, double longitude) {
        long newid;

        // First, get the last ID, add 1, then use this as the new id.
        voltQueueSQL( getLastId);
        VoltTable[] queryresults = voltExecuteSQL();
        VoltTable result = queryresults[0];
        if (result.getRowCount() == 1) {
            newid = result.fetchRow(0).getLong(0);
            newid++;
        }
        else {
            newid = 0;
        }

        // Add a SQL statement to the execution queue. Queries
        // and DMLs may not be mixed in one batch.
        voltQueueSQL( insertSatItem, newid, model, country );
        voltQueueSQL( insertLocItem, newid, latitude, longitude );

        // Run all queued queries.
        queryresults = voltExecuteSQL();

        return newid;
    }
}
