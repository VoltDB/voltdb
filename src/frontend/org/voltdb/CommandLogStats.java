package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;

import org.voltdb.VoltTable.ColumnInfo;

public class CommandLogStats extends StatsSource {

	private final CommandLog m_commandLog;

	public enum StatName {
		OUTSTANDING_BYTES,
		OUTSTANDING_TXNS
	};

	public CommandLogStats(CommandLog commandLog) {
		super(false);
		m_commandLog = commandLog;
	}

	@Override
	protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
		super.populateColumnSchema(columns);
        columns.add(new VoltTable.ColumnInfo(StatName.OUTSTANDING_BYTES.name(), VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(StatName.OUTSTANDING_TXNS.name(), VoltType.BIGINT));
	}

	@Override
	protected void updateStatsRow(Object rowKey, Object[] rowValues) {
		m_commandLog.populateCommandLogStats(columnNameToIndex, rowValues);
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
