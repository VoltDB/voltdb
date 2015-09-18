package iwdemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    // choose fairly large cities
    // (which are generally near other stuff)
    // that are evenly dispersed over the contiguous US.
    final private long[] cityIds = new long[] {
      1259, // san jose
      11472, // burlington
      15682, // las vegas
      3565, // denver
      12928, // minneapolis

      24884, // dallas
      7698, // indianapolis
      23991, // rapid city, SD
      9417, // Kansas City
      5080, // atlanta

      14727, // butte, mt
      27834, // spokane
      4133, // washington
      21394, // portland or
    };

    /**
     * The time in milliseconds between calls to the run() method,
     * which updates the location of one taxi.
     */
    final private static int UPDATE_INTERVAL = 25;

    /**
     * This value controls the amount that one taxi will move during an update.
     * We find the lat/lng of a neighboring cell in this level.  Lower levels have
     * larger cells, so setting this number lower will make the taxis move faster.
     */
    final private static int S2_UPDATE_LEVEL = 12;

    private final Client m_client;
    private final Random m_random;
    private final double[][] m_taxiLocations;

    TaxiManager(Client client) {
        m_client = client;
        m_random = new Random(8888);
        m_taxiLocations = new double[NUM_TAXIS][2];
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
            int taxiId = 0;
            for (int i = 0; i < NUM_STARTING_CITIES; ++i) {

                VoltTable vt = m_client.callProcedure("@AdHoc", "select location, name from cities where id = ?",
                        cityIds[i]).getResults()[0];
                vt.advanceRow();
                PointType pt = vt.getPoint(0);
                System.out.println("  " + vt.getString(1));

                // Allocate 8 cabs to this point
                for (int j = 0; j < NUM_DIRECTIONS; ++j) {
                    m_taxiLocations[taxiId][0] = pt.getLatitude();
                    m_taxiLocations[taxiId][1] = pt.getLongitude();
                    S2LatLng ll = S2LatLng.fromDegrees(pt.getLatitude(), pt.getLongitude());
                    S2CellId cell = S2CellId.fromLatLng(ll);
                    m_client.callProcedure("taxis.Insert", taxiId, pt.getLatitude(), pt.getLongitude(), cell.id());
                    ++taxiId;
                }
            }

            System.out.println("Created " + taxiId + " taxis.");

        } catch (IOException | ProcCallException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        // Choose a taxi at random
        int whichTaxi = m_random.nextInt(NUM_TAXIS);
        double origLat = m_taxiLocations[whichTaxi][0];
        double origLng = m_taxiLocations[whichTaxi][1];

        S2LatLng ll = S2LatLng.fromDegrees(origLat, origLng);
        S2CellId cell = S2CellId.fromLatLng(ll);

        // Go up to a coarser level of granularity,
        // choose an adjacent cell.
        S2CellId parent = cell.parent(S2_UPDATE_LEVEL);
        List<S2CellId> parentNeighbors = new ArrayList<>();
        parent.getAllNeighbors(S2_UPDATE_LEVEL, parentNeighbors);

        // Hopefully, directions are always returned in the same order...
        // Empirically this seems mostly true.
        S2CellId newParent = parentNeighbors.get(whichTaxi % parentNeighbors.size());
        S2Point s2Pt = (new S2Cell(newParent)).getCenter();
        S2CellId newCell = S2CellId.fromPoint(s2Pt);
        double newLat = S2LatLng.latitude(s2Pt).degrees();
        double newLng = S2LatLng.longitude(s2Pt).degrees();

        try {
            m_client.callProcedure(new NullCallback(), "@AdHoc",
                    "update taxis set lat = ?, lng = ?, cellid = ? "
                    + "where id = ?",
                    newLat, newLng, newCell.id(), whichTaxi);
            m_taxiLocations[whichTaxi][0] = newLat;
            m_taxiLocations[whichTaxi][1] = newLng;
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }


    }
}
