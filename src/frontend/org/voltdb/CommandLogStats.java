package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.VoltTable.ColumnInfo;

public class CommandLogStats extends StatsSource {

	private static final String OUTSTANDING_BYTES_NAME = "OUTSTANDING_BYTES";
	private static final String OUTSTANDING_TXNS_NAME = "OUTSTANDING_TXNS";
	
	public CommandLogStats() {
		super(false);
	}
	
	@Override
	protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
		super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo(OUTSTANDING_BYTES_NAME, VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(OUTSTANDING_TXNS_NAME, VoltType.BIGINT));
	}

	@Override
	protected void updateStatsRow(Object rowKey, Object[] rowValues) {
		// TODO stub here
		rowValues[columnNameToIndex.get(OUTSTANDING_BYTES_NAME)] = 0;
		rowValues[columnNameToIndex.get(OUTSTANDING_TXNS_NAME)] = 0;
		super.updateStatsRow(rowKey, rowValues);
	}

	@Override
	protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
		return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
	}

}
