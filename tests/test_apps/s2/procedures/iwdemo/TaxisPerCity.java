package iwdemo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.geometry.S1Angle;
import com.google_voltpatches.common.geometry.S2Cap;
import com.google_voltpatches.common.geometry.S2CellId;
import com.google_voltpatches.common.geometry.S2LatLng;
import com.google_voltpatches.common.geometry.S2Point;

public class TaxisPerCity extends VoltProcedure {

    final SQLStmt selTaxiLocations = new SQLStmt("select lat, lng from taxis");

    final SQLStmt selCitiesInCellRange = new SQLStmt(
            "select name "
            + "from cities "
            + "where cellid between ? and ?");
    public VoltTable run(long partitioningParam, double rangeInKm) {

        voltQueueSQL(selTaxiLocations);
        VoltTable taxiLocations = voltExecuteSQL()[0];
        S1Angle capAngle = S1Angle.radians(rangeInKm / Utils.radiusOfEarth());
        while (taxiLocations.advanceRow()) {

            double lat = taxiLocations.getDouble(0);
            double lng = taxiLocations.getDouble(1);
            S2Point capAxis = S2LatLng.fromDegrees(lat, lng).toPoint();
            S2Cap cap = S2Cap.fromAxisAngle(capAxis, capAngle);
            ArrayList<S2CellId> coveringCells = new ArrayList<>();
            Utils.getCoverer().getCovering(cap, coveringCells);

            for (S2CellId cell : coveringCells) {
                voltQueueSQL(selCitiesInCellRange, cell.rangeMin().id(), cell.rangeMax().id());
            }
        }

        Map<String, Integer> citiesToTaxiCount = new HashMap<>();
        VoltTable[] citiesInCells = voltExecuteSQL(true);
        for (VoltTable citiesInOneCell : citiesInCells) {
            while (citiesInOneCell.advanceRow()) {
                String city = citiesInOneCell.getString(0);
                Integer val = citiesToTaxiCount.get(city);
                if (val == null) {
                    citiesToTaxiCount.put(city, 1);
                }
                else {
                    citiesToTaxiCount.put(city,  val + 1);
                }
            }
        }

        VoltTable result = new VoltTable(
                new VoltTable.ColumnInfo("Place name", VoltType.STRING),
                new VoltTable.ColumnInfo("Number of drivers", VoltType.BIGINT));

        for (String city : citiesToTaxiCount.keySet()) {
            result.addRow(city, citiesToTaxiCount.get(city));
        }

        return result;

    }
}
