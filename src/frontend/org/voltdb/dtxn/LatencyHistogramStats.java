/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.dtxn;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram_voltpatches.AbstractHistogram;
import org.HdrHistogram_voltpatches.AtomicHistogram;
import org.HdrHistogram_voltpatches.Histogram;
import org.voltcore.utils.CompressionStrategySnappy;
import org.voltdb.ClientInterface;
import org.voltdb.SiteStatsSource;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;

/** Source of @Statistics LATENCY_COMPRESSED.
 * Latency information is provided in buckets. Each bucket contains the
 * number of procedures with latencies in the range.
 */
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
        final int numberOfSignificantValueDigits = 3;
        if (threadSafe) {
            return new AtomicHistogram( highestTrackableValue, numberOfSignificantValueDigits);
        } else {
            return new Histogram( highestTrackableValue, numberOfSignificantValueDigits);
        }
    }

    private WeakReference<byte[]> m_compressedCache = null;
    private WeakReference<byte[]> m_serializedCache = null;

    private AbstractHistogram m_totals = constructHistogram(false);

    private final static int EXPIRATION = Integer.getInteger("LATENCY_CACHE_EXPIRATION", 900);

    public enum LatencyCompressed {
        HISTOGRAM                   (VoltType.VARBINARY);

        public final VoltType m_type;
        LatencyCompressed(VoltType type) { m_type = type; }
    }

    private Supplier<AbstractHistogram> getHistogramSupplier() {
        return Suppliers.memoizeWithExpiration(new Supplier<AbstractHistogram>() {
            @Override
            public AbstractHistogram get() {
                m_totals.reset();
                ClientInterface ci = VoltDB.instance().getClientInterface();
                if (ci != null) {
                    List<AbstractHistogram> thisci = ci.getLatencyStats();
                    for (AbstractHistogram info : thisci) {
                        m_totals.add(info);
                    }
                }
                m_compressedCache = null;
                m_serializedCache = null;

                return m_totals;
            }
        }, EXPIRATION, TimeUnit.MILLISECONDS);
    }
    private Supplier<AbstractHistogram> m_histogramSupplier = getHistogramSupplier();

    public byte[] getSerializedCache() {
        byte[] retval = null;
        if (m_serializedCache == null || (retval = m_serializedCache.get()) == null) {
            retval = m_histogramSupplier.get().toUncompressedBytes();
            m_serializedCache = new WeakReference<byte[]>(retval);
        }
        return retval;
    }

    public byte[] getCompressedCache() {
        byte[] retval = null;
        if (m_compressedCache == null || (retval = m_compressedCache.get()) == null) {
            retval = AbstractHistogram.toCompressedBytes(getSerializedCache(), CompressionStrategySnappy.INSTANCE);
            m_compressedCache = new WeakReference<byte[]>(retval);
        }
        return retval;
    }

    public AbstractHistogram getHistogram() {
        return m_histogramSupplier.get();
    }

    public LatencyHistogramStats(long siteId) {
        super(siteId, false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        m_histogramSupplier.get();
        return new DummyIterator();
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns, LatencyCompressed.class);
    }

    @Override
    protected int updateStatsRow(Object rowKey, Object[] rowValues) {
        int offset = super.updateStatsRow(rowKey, rowValues);
        rowValues[offset + LatencyCompressed.HISTOGRAM.ordinal()] = getCompressedCache();
        return offset + LatencyCompressed.values().length;
    }
}
