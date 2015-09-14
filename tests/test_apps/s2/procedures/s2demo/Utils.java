package s2demo;

import com.google_voltpatches.common.geometry.S2Cell;
import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2RegionCoverer;

public class Utils {

    /**
     * @return radius of earth in KM^2
     */
    public static double radiusOfEarth() {
        return 6371.0;
    }

    /**
     * @return radius of earth squared, in KM^2
     */
    public static double radiusOfEarthSquared() {
        return 6371.0 * 6371.0;
    }

    private static final int COVERER_MAX_LEVEL = 14; // avg cell size 0.317 km^2

    public static int getCovererMaxLevel() {
        return COVERER_MAX_LEVEL;
    }

    public static S2RegionCoverer getCoverer() {
        S2RegionCoverer coverer = new S2RegionCoverer();
        coverer.setMaxLevel(COVERER_MAX_LEVEL);
        return coverer;
    }

    //    avg area of cells in level 0: 85021153.482 km^2.
    //    avg area of cells in level 1: 21255288.370 km^2.
    //    avg area of cells in level 2: 5313822.093 km^2.
    //    avg area of cells in level 3: 1328455.523 km^2.
    //    avg area of cells in level 4: 332113.881 km^2.
    //    avg area of cells in level 5: 83028.470 km^2.
    //    avg area of cells in level 6: 20757.118 km^2.
    //    avg area of cells in level 7: 5189.279 km^2.
    //    avg area of cells in level 8: 1297.320 km^2.
    //    avg area of cells in level 9: 324.330 km^2.
    //    avg area of cells in level 10:  81.082 km^2.
    //    avg area of cells in level 11:  20.271 km^2.
    //    avg area of cells in level 12:   5.068 km^2.
    //    avg area of cells in level 13:   1.267 km^2.
    //    avg area of cells in level 14:   0.317 km^2.
    //    avg area of cells in level 15:   0.079 km^2.
    //    avg area of cells in level 16:   0.020 km^2.
    //    avg area of cells in level 17:   0.005 km^2.
    //    avg area of cells in level 18:   0.001 km^2.
    //    avg area of cells in level 19:   0.000 km^2.
    //    avg area of cells in level 20:   0.000 km^2.
    //    avg area of cells in level 21:   0.000 km^2.
    //    avg area of cells in level 22:   0.000 km^2.
    //    avg area of cells in level 23:   0.000 km^2.
    //    avg area of cells in level 24:   0.000 km^2.
    //    avg area of cells in level 25:   0.000 km^2.
    //    avg area of cells in level 26:   0.000 km^2.
    //    avg area of cells in level 27:   0.000 km^2.
    //    avg area of cells in level 28:   0.000 km^2.
    //    avg area of cells in level 29:   0.000 km^2.
    //    avg area of cells in level 30:   0.000 km^2.
    //
    public static void showLevels() {
        for (int i = 0; i <= S2CellId.MAX_LEVEL; ++i) {
            System.out.println("  avg area of cells in level " + i + ": "
                    + "" + String.format("%7.3f", S2Cell.averageArea(i) * radiusOfEarthSquared()) + " km^2.");
        }
    }

}
