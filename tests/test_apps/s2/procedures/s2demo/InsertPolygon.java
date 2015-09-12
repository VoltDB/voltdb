package s2demo;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;

import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;
import com.google_voltpatches.common.geometry.S2Loop;
import com.google_voltpatches.common.geometry.S2Point;
import com.google_voltpatches.common.geometry.S2Polygon;

public class InsertPolygon extends VoltProcedure {

    final SQLStmt insPolygon = new SQLStmt("insert into polygons values ("
            + "?, " // state id
            + "?"   // name
            + ")");

    final SQLStmt insPolygonCellMap = new SQLStmt("insert into cellid_poly_map values ("
            + "?, " // cellid
            + "?"   // polyid
            + ")");

    public long run(long id, String name, double[] points) {
        List<S2Point> vertices = new ArrayList<>();
        for (int i = 0; i < points.length; i += 2) {
            double lat = points[i];
            double lng = points[i + 1];
            S2LatLng ll = S2LatLng.fromDegrees(lat, lng);

            vertices.add(ll.toPoint());
        }

        S2Polygon poly = new S2Polygon(new S2Loop(vertices));

        ArrayList<S2CellId> coveringCells = new ArrayList<>();
        Utils.getCoverer().getCovering(poly, coveringCells);

        voltQueueSQL(insPolygon, id, name);
        for (S2CellId cell : coveringCells) {
            voltQueueSQL(insPolygonCellMap, cell.id(), id);
        }

        voltExecuteSQL(true);
        return coveringCells.size();
    }
}
