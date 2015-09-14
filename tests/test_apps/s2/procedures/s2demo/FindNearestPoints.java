package s2demo;

import java.util.ArrayList;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.geometry.S1Angle;
import com.google_voltpatches.common.geometry.S2;
import com.google_voltpatches.common.geometry.S2Cap;
import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;
import com.google_voltpatches.common.geometry.S2Point;

public class FindNearestPoints extends VoltProcedure {

    final SQLStmt selPointsByCellRange = new SQLStmt(
            "select id, name "
            + "from points "
            + "where cellid between ? and ?");

    public VoltTable run(double lat, double lng, double rangeInKm) {

        S2Point capAxis = S2LatLng.fromDegrees(lat, lng).toPoint();
        double rangeOnUnitSphere = rangeInKm / Utils.radiusOfEarth();
        assert (rangeOnUnitSphere < S2.M_PI); // i.e., the cap is smaller than a hemisphere.

        S2Cap cap = S2Cap.fromAxisAngle(capAxis, S1Angle.radians(rangeOnUnitSphere));
        ArrayList<S2CellId> covering = new ArrayList<>();
        Utils.getCoverer().getCovering(cap, covering);

        for (S2CellId cell : covering) {
            voltQueueSQL(selPointsByCellRange, cell.rangeMin().id(), cell.rangeMax().id());
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
