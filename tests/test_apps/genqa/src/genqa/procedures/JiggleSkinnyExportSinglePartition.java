package genqa.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class JiggleSkinnyExportSinglePartition extends VoltProcedure {
    public final SQLStmt export = new SQLStmt(
            "INSERT INTO export_skinny_partitioned_table (rowid, txnid) VALUES (?,?)"
            );

    public long run(long rowid, int reversed) {
        @SuppressWarnings("deprecation")
        long txnid = getVoltPrivateRealTransactionIdDontUseMe();

        voltQueueSQL(export, rowid, txnid);

        // Execute last statement batch
        voltExecuteSQL(true);

        // Return to caller
        return txnid;
    }
}
