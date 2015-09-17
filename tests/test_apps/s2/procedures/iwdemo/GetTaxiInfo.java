package iwdemo;

import java.util.ArrayList;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.geometry.S2Cap;
import com.google_voltpatches.common.geometry.S2CellId;

public class GetTaxiInfo extends VoltProcedure {

    final SQLStmt getTaxiCell = new SQLStmt("select lat, lng from taxis where id = ?");

    final SQLStmt getTaxisByCellRange = new SQLStmt(
            "select id from taxis "
            + "where cellid between ? and ?");

    final SQLStmt getCitiesByCellRange = new SQLStmt(
            "select name from cities where cellid between ? and ?");

    public VoltTable[] run(long taxiId) {
        voltQueueSQL(getTaxiCell, taxiId);
        VoltTable latLng = voltExecuteSQL()[0];
        latLng.advanceRow();
        double lat = latLng.getDouble(0);
        double lng = latLng.getDouble(1);

        // A cap with radius of 8km (5 miles)
        S2Cap cap = Utils.getCap(lat, lng, 8.0);
        ArrayList<S2CellId> coveringCells = new ArrayList<>();
        Utils.getCoverer().getCovering(cap, coveringCells);

        for (S2CellId cell : coveringCells) {
            voltQueueSQL(getTaxisByCellRange, cell.rangeMin().id(), cell.rangeMax().id());
        }

        VoltTable[] taxisInCells = voltExecuteSQL();
        VoltTable nearbyTaxis = new VoltTable(new VoltTable.ColumnInfo("Taxi id", VoltType.BIGINT));
        for (VoltTable taxisInOneCell : taxisInCells) {
            while (taxisInOneCell.advanceRow()) {
                nearbyTaxis.addRow(taxisInOneCell.getLong(0));
            }
        }

        for (S2CellId cell : coveringCells) {
            voltQueueSQL(getCitiesByCellRange, cell.rangeMin().id(), cell.rangeMax().id());
        }

        VoltTable[] citiesInCells = voltExecuteSQL(true);
        VoltTable nearbyCities = new VoltTable(new VoltTable.ColumnInfo("City name", VoltType.STRING));
        for (VoltTable citiesInOneCell : citiesInCells) {
            while (citiesInOneCell.advanceRow()) {
                nearbyCities.addRow(citiesInOneCell.getString(0));
            }
        }

        return new VoltTable[] {nearbyTaxis, nearbyCities};
    }
}
