package iwdemo;

import org.voltdb.SQLStmt;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;

import org.voltdb.VoltProcedure;

public class InsertCity extends VoltProcedure {
    final SQLStmt insStmt = new SQLStmt("insert into cities values ("
            + "?, "                // id
            + "?, "                // name
            + "pointfromtext(?), " // location
            + "?"                  // cellid
            + ")");

    public long run(long id, String name, double lat, double lng) {
        S2LatLng ll = S2LatLng.fromDegrees(lat, lng);
        S2CellId cell = S2CellId.fromLatLng(ll);

        String wkt = "point(" + lat + " " + lng + ")";
        voltQueueSQL(insStmt, id, name, wkt, cell.id());
        VoltTable vt = voltExecuteSQL(true)[0];
        vt.advanceRow();
        return vt.getLong(0);
    }
}
