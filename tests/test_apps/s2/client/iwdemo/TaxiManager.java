package iwdemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NullCallback;
import org.voltdb.client.ProcCallException;
import org.voltdb.types.PointType;

import com.google_voltpatches.common.geometry.S2Cell;
import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;
import com.google_voltpatches.common.geometry.S2Point;

public class TaxiManager implements Runnable {

    final private static int NUM_STARTING_CITIES = 14;
    final private static int NUM_DIRECTIONS = 8;
    final private static int NUM_TAXIS = NUM_STARTING_CITIES * NUM_DIRECTIONS;

    /**
     * The time in milliseconds between calls to the run() method,
     * which updates the location of one taxi.
     */
    final private static int UPDATE_INTERVAL = 50;

    /**
     * This value controls the amount that one taxi will move during an update.
     * We find the lat/lng of a neighboring cell in this level.  Lower levels have
     * larger cells, so setting this number lower will make the taxis move faster.
     */
    final private static int S2_UPDATE_LEVEL = 8;

    private final Client m_client;
    private final Random m_random;
    private final PointType[] m_taxiLocations;

    TaxiManager(Client client) {
        m_client = client;
        m_random = new Random(777);
        m_taxiLocations = new PointType[NUM_TAXIS];
    }

    /**
     *
     * @return interval that taxi locations should be updated, in milliseconds
     */
    public static long getUpdateInterval() {
        return UPDATE_INTERVAL;
    }

    public void createTaxis() {
        try {
            System.out.println("Creating taxis in starting cities:");
            final long NUM_CITIES = m_client.callProcedure("@AdHoc", "select max(id) from cities")
                    .getResults()[0].asScalarLong();

            int taxiId = 0;
            for (int i = 0; i < NUM_STARTING_CITIES; ++i) {

                // pick a city at random
                VoltTable vt = m_client.callProcedure("@AdHoc", "select location, name from cities where id = ?",
                        m_random.nextInt((int)NUM_CITIES)).getResults()[0];
                vt.advanceRow();
                PointType pt = vt.getPoint(0);
                System.out.println("  " + vt.getString(1));

                // Allocate 8 cabs to this point
                for (int j = 0; j < NUM_DIRECTIONS; ++j) {
                    m_taxiLocations[taxiId] = pt;
                    S2LatLng ll = S2LatLng.fromDegrees(pt.getLatitude(), pt.getLongitude());
                    S2CellId cell = S2CellId.fromLatLng(ll);
                    m_client.callProcedure("taxis.Insert", taxiId, pt, cell.id());
                    ++taxiId;
                }
            }

            System.out.println("Created " + taxiId + " taxis.");

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
    }

    private final Map<String, List<Integer> > dirValMap = new HashMap<>();

    private void debugUpdate(int whichTaxi, PointType origPt, PointType newPt){
        String dir = "";

        float nsDelta = newPt.getLatitude() - origPt.getLatitude();
        if (nsDelta > 0.1) {
            dir += "N";
        }
        else if (nsDelta < -0.1) {
            dir += "S";
        }

        float ewDelta = newPt.getLongitude() - origPt.getLongitude();
        if (ewDelta > 0.1) {
            dir += "E";
        }
        else if (ewDelta < -0.1) {
            dir += "W";
        }

        assert(dir != "");

        if (dirValMap.get(dir) == null) {
            dirValMap.put(dir, new ArrayList<Integer>());
        }

        dirValMap.get(dir).add(whichTaxi % 8);

        for (String k : dirValMap.keySet()) {
            System.out.println(k + ": " + dirValMap.get(k));
        }
        System.out.println("\n");
    }

    @Override
    public void run() {
        // Choose a taxi at random
        int whichTaxi = m_random.nextInt(NUM_TAXIS);
        PointType origPt = m_taxiLocations[whichTaxi];

        S2LatLng ll = S2LatLng.fromDegrees(
                origPt.getLatitude(),
                origPt.getLongitude());
        S2CellId cell = S2CellId.fromLatLng(ll);

        // Go up to a coarser level of granularity,
        // choose an adjacent cell.
        S2CellId parent = cell.parent(S2_UPDATE_LEVEL);
        List<S2CellId> parentNeighbors = new ArrayList<>();
        parent.getAllNeighbors(S2_UPDATE_LEVEL, parentNeighbors);

        // Hopefully, directions are always returned in the same order...
        // Empirically this seems mostly true.  Uncomment call to debugUpdate below
        // to verify.
        S2CellId newParent = parentNeighbors.get(whichTaxi % parentNeighbors.size());
        S2Point s2Pt = (new S2Cell(newParent)).getCenter();
        S2CellId newCell = S2CellId.fromPoint(s2Pt);
        PointType newPt = new PointType(
                (float)S2LatLng.latitude(s2Pt).degrees(),
                (float)S2LatLng.longitude(s2Pt).degrees());
        //debugUpdate(whichTaxi, origPt, newPt);

        try {
            m_client.callProcedure(new NullCallback(), "@AdHoc",
                    "update taxis set location = ?, cellid = ? "
                    + "where id = ?", newPt, newCell.id(), whichTaxi);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        m_taxiLocations[whichTaxi] = newPt;
    }
}
