package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.AtomicHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltdb.ClientInterface;
import org.voltdb.SiteStatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

public class LatencyHistogramStats extends SiteStatsSource {

    /**
     * A dummy iterator that wraps and int and provides the
     * Iterator<Object> necessary for getStatsRowKeyIterator()
     *
     */
    private static class DummyIterator implements Iterator<Object> {
        boolean oneRow = false;

        @Override
        public boolean hasNext() {
            if (!oneRow) {
                oneRow = true;
                return true;
            }
            return false;
        }

        @Override
        public Object next() {
            return null;
        }

        @Override
        public void remove() {

        }
    }

    public static AbstractHistogram constructHistogram(boolean threadSafe) {
        final long highestTrackableValue = 60L * 60L * 1000000L;
        final int numberOfSignificantValueDigits = 2;
        if (threadSafe) {
            return new AtomicHistogram( highestTrackableValue, numberOfSignificantValueDigits);
        } else {
            return new Histogram( highestTrackableValue, numberOfSignificantValueDigits);
        }
    }

    private AbstractHistogram m_totals = constructHistogram(false);

    public LatencyHistogramStats(long siteId) {
        super(siteId, false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_totals.reset();
        ClientInterface ci = VoltDB.instance().getClientInterface();
        if (ci != null) {
            List<AbstractHistogram> thisci = ci.getLatencyStats();
            for (AbstractHistogram info : thisci) {
                m_totals.add(info);
            }
        }
        return new DummyIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("UNCOMPRESSED_HISTOGRAM", VoltType.VARBINARY));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        rowValues[columnNameToIndex.get("UNCOMPRESSED_HISTOGRAM")] = m_totals.toUncompressedBytes();
        super.updateStatsRow(rowKey, rowValues);
    }
}
