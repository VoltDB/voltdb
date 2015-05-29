package approxcountdistinct;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public class DoCount extends VoltProcedure {

    final SQLStmt countApprox = new SQLStmt("select approx_count_distinct(value) from data;");
    final SQLStmt countExact = new SQLStmt("select count(distinct value) from data;");

    public VoltTable run() {

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countApprox);
        }
        voltExecuteSQL(); // warm up

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countApprox);
        }
        long startTime = System.currentTimeMillis();
        VoltTable vt = voltExecuteSQL()[0];
        long approxMillis = System.currentTimeMillis() - startTime;
        vt.advanceRow();
        double approxAnswer = vt.getDouble(0);

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countExact);
        }
        voltExecuteSQL(); // warm up

        for (int i = 0; i < 10; ++i) {
            voltQueueSQL(countExact);
        }

        startTime = System.currentTimeMillis();
        vt = voltExecuteSQL()[0];
        long exactMillis = System.currentTimeMillis() - startTime;
        vt.advanceRow();
        long exactAnswer = vt.getLong(0);

        VoltTable.ColumnInfo[] cols = new VoltTable.ColumnInfo[] {
                new VoltTable.ColumnInfo("approx answer", VoltType.FLOAT),
                new VoltTable.ColumnInfo("approx elapsed millis", VoltType.FLOAT),
                new VoltTable.ColumnInfo("exact answer", VoltType.BIGINT),
                new VoltTable.ColumnInfo("exact elapsed millis", VoltType.FLOAT)
        };

        VoltTable retTable = new VoltTable(cols);
        retTable.addRow(approxAnswer, approxMillis / 10.0, exactAnswer, exactMillis / 10.0);

        return retTable;
    }
}
