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
                info.reset();
            }
        }
        return new DummyIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo("HISTOGRAM-50", VoltType.INTEGER));
        columns.add(new ColumnInfo("HISTOGRAM-75", VoltType.INTEGER));
        columns.add(new ColumnInfo("HISTOGRAM-99", VoltType.INTEGER));
        columns.add(new ColumnInfo("HISTOGRAM-999", VoltType.INTEGER));
        columns.add(new ColumnInfo("HISTOGRAM-9999", VoltType.INTEGER));
        columns.add(new ColumnInfo("HISTOGRAM-99999", VoltType.INTEGER));
    }

    @Override
    protected void updateStatsRow(Object rowKey, Object[] rowValues) {
        long count = m_totals.getHistogramData().getTotalCount();
        rowValues[columnNameToIndex.get("HISTOGRAM-50")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.5f);
        rowValues[columnNameToIndex.get("HISTOGRAM-75")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.75f);
        rowValues[columnNameToIndex.get("HISTOGRAM-99")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.99f);
        rowValues[columnNameToIndex.get("HISTOGRAM-999")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.999f);
        rowValues[columnNameToIndex.get("HISTOGRAM-9999")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.9999f);
        rowValues[columnNameToIndex.get("HISTOGRAM-99999")] = count == 0 ? 0: m_totals.getHistogramData().getValueAtPercentile(0.99999f);
        super.updateStatsRow(rowKey, rowValues);
    }
}
