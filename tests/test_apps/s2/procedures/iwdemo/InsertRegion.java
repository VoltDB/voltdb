package iwdemo;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;
import com.google_voltpatches.common.geometry.S2Loop;
import com.google_voltpatches.common.geometry.S2Point;
import com.google_voltpatches.common.geometry.S2Polygon;
import com.google_voltpatches.common.geometry.S2RegionCoverer;

public class InsertRegion extends VoltProcedure {
    final SQLStmt insStmt = new SQLStmt("insert into region values ("
            + "?, "                // id
            + "?, "                // containerid
            + "?, "                // componentnum
            + "?, "                // kind
            + "?"                  // boundary
            + ")");
    final SQLStmt insMapStmt = new SQLStmt("insert into cellid_region_map values ("
            + "?, "                // cellid
            + "?"                  // regionid
            + ")");

    public long run(long id, int containerid, int componentNumber, int kind, byte varboundary[]) {
        ArrayList<S2Loop> loops = new ArrayList<S2Loop>();
        ByteBuffer buf = ByteBuffer.wrap(varboundary);
        Integer numLoops = buf.getInt(0);
        int idx = 4;
        for (int loopidx = 0; loopidx < numLoops; loopidx += 1) {
            int numVerts = buf.getInt(idx);
            idx += 4;
            ArrayList<S2Point> verts = new ArrayList<S2Point>();
            for (int vertidx = 0; vertidx < numVerts; vertidx += 1) {
                Double lat = buf.getDouble(idx);
                Double lng = buf.getDouble(idx + 8);
                idx += 16;
                verts.add(S2LatLng.fromDegrees(lat, lng).toPoint());
            }
            loops.add(new S2Loop(verts));
        }
        S2Polygon poly = new S2Polygon(loops);
        S2RegionCoverer coverer = new S2RegionCoverer();
        ArrayList<S2CellId> covering = new ArrayList<S2CellId>();
        coverer.getCovering(poly, covering);
        varboundary = new byte[0];
        voltQueueSQL(insStmt, id, containerid, componentNumber, kind, varboundary);
        for (S2CellId cellid : covering) {
            voltQueueSQL(insMapStmt, cellid.id(), id);
        }
        VoltTable vt = voltExecuteSQL(true)[0];
        vt.advanceRow();
        return vt.getLong(0);
    }
}
