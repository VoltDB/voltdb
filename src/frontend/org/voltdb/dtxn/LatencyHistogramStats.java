package org.voltdb.dtxn;

import java.util.ArrayList;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class LatencyHistogramStats extends LatencyStats {

    public LatencyHistogramStats(long siteId) {
        super(siteId);
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("UNCOMPRESSED_HISTOGRAM", VoltType.VARBINARY));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        rowValues[columnNameToIndex.get("UNCOMPRESSED_HISTOGRAM")] = getSerializedCache();
        super.updateStatsRow(rowKey, rowValues);
    }
}
