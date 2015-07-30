package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

public class DeterminismRubsOff extends VoltProcedure {

    public static final SQLStmt queryAmbiguousRows = new SQLStmt("select * from multikey where col2=? order by col1, col3");

    public long run() {
        voltQueueSQL(queryAmbiguousRows);
        voltExecuteSQL();
        // zero is a successful return
        return 0;
    }

}
