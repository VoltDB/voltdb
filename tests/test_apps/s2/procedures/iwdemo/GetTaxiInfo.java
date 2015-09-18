package iwdemo;

import java.util.ArrayList;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.geometry.S2Cap;
import com.google_voltpatches.common.geometry.S2CellId;

public class GetTaxiInfo extends VoltProcedure {

    final SQLStmt getTaxiCell = new SQLStmt("select lat, lng, cellid from taxis where id = ?");

    final SQLStmt findEnclosingCounty = new SQLStmt(
            "select cty.name "
            + "from cellid_region_map as crm "
            + "inner join region as reg "
            + "on crm.regionid = reg.id "
            + "inner join county as cty "
            + "on cty.id = reg.containerid "
            + "where crm.cellid in ? "
            + "and reg.kind = 0");

    final SQLStmt findEnclosingState = new SQLStmt(
            "select st.name "
            + "from cellid_region_map as crm "
            + "inner join region as reg "
            + "on crm.regionid = reg.id "
            + "inner join state as st "
            + "on st.id = reg.containerid "
            + "where crm.cellid in ? "
            + "and reg.kind = 1");

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
        S2CellId taxiCell = new S2CellId(latLng.getLong(2));

        long[] parentCells = new long[Utils.getCovererMaxLevel() + 1];
        for (int i = 0; i <= Utils.getCovererMaxLevel(); ++i) {
            parentCells[i] = taxiCell.parent(i).id();
        }

        String county = null;
        String state = null;
        voltQueueSQL(findEnclosingCounty, parentCells);
        voltQueueSQL(findEnclosingState, parentCells);
        VoltTable[] countyStateTables = voltExecuteSQL();
        if (countyStateTables[0].advanceRow()) {
            // We are working from approximations using cells,
            // so there might be more than one match.  Just
            // take the first one for now.
            county = countyStateTables[0].getString(0);
        }
        if (countyStateTables[1].advanceRow()) {
            state = countyStateTables[1].getString(0);
        }

        VoltTable countyAndState = new VoltTable(
                new VoltTable.ColumnInfo("county name", VoltType.STRING),
                new VoltTable.ColumnInfo("state name", VoltType.STRING));
        countyAndState.addRow(county, state);

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

        return new VoltTable[] {countyAndState, nearbyTaxis, nearbyCities};
    }
}
