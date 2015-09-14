package s2demo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.geometry.S2CellId;

public class FindIntersectingPolygons extends VoltProcedure {

    final SQLStmt selectCellId = new SQLStmt(
            "select cellid "
            + "from points "
            + "where id = ?");

    final SQLStmt selectPolysByCells = new SQLStmt(
            "select cpm.polyid, pg.name "
            + "from cellid_poly_map as cpm "
            + "inner join polygons as pg "
            + "on pg.id = cpm.polyid "
            + "where cellid in ?");

    public VoltTable run(long ptId) {
        voltQueueSQL(selectCellId, ptId);
        long ptCellId = voltExecuteSQL()[0].asScalarLong();
        S2CellId cell = new S2CellId(ptCellId);

        long[] parentCells = new long[Utils.getCovererMaxLevel()];
        for (int i = 0; i < parentCells.length; ++i) {
            parentCells[i] = cell.parent(i).id();
        }

        voltQueueSQL(selectPolysByCells, parentCells);
        return voltExecuteSQL(true)[0];
    }
}
