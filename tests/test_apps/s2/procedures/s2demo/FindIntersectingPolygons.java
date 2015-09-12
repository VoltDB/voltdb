package s2demo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

public class FindIntersectingPolygons extends VoltProcedure {

    final SQLStmt selectCellId = new SQLStmt(
            "select cellid "
            + "from points "
            + "where id = ?");

    public VoltTable run(long ptId) {
        voltQueueSQL(selectCellId, ptId);
        //long ptCellId = voltExecuteSQL()[0].asScalarLong();

        // for each level ...

        return null;
    }
}
