package s2demo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.geometry.S2CellId;

public class FindContainedPoints extends VoltProcedure {

    final SQLStmt selPolyCellIds = new SQLStmt(
            "select cellid "
            + "from cellid_state_map "
            + "where stateid = ?");

    final SQLStmt selPointsByCellRange = new SQLStmt(
            "select id, name "
            + "from cities "
            + "where cellid between ? and ?");

    public VoltTable run(long polyId) {

        voltQueueSQL(selPolyCellIds, polyId);
        VoltTable vt = voltExecuteSQL()[0];
        while (vt.advanceRow()) {
            S2CellId cellId = new S2CellId(vt.getLong(0));
            long rangeMin = cellId.rangeMin().id();
            long rangeMax = cellId.rangeMax().id();
            voltQueueSQL(selPointsByCellRange, rangeMin, rangeMax);
        }

        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("name", VoltType.STRING));

        VoltTable[] pointsInRanges = voltExecuteSQL(true);
        for (VoltTable ptsInOneRange : pointsInRanges) {
            while (ptsInOneRange.advanceRow()) {
                result.addRow(ptsInOneRange.getLong(0), ptsInOneRange.getString(1));
            }
        }

        return result;
    }

}
