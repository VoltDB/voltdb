/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
package org.voltdb.utils;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;

public class LatencyLogger {
    private static Histogram m_histogramData;

    static class Histogram {
        final private Long lowestTrackableValue;
        final private Long highestTrackableValue;
        final private Integer nSVD;
        public Long totalCount;
        public Long[] count;
        final private Long unitMagnitude;
        final private Long subBucketHalfCountMagnitude;
        final private int subBucketCount;
        final private int subBucketHalfCount;
        final private int bucketCount;
        final private int countsArrayLength;

        Histogram(Long lowestTrackableValue, Long highestTrackableValue, Integer nSVD, Long totalCount) {
            this.lowestTrackableValue = lowestTrackableValue;
            this.highestTrackableValue = highestTrackableValue;
            this.nSVD = nSVD;
            this.totalCount = totalCount;

            double largestValueWithSingleUnitResolution = 2 * Math.pow(10, this.nSVD);
            this.unitMagnitude = new Double(Math.floor(Math.log(this.lowestTrackableValue)/Math.log(2))).longValue();
            Long subBucketCountMagnitude = new Double(Math.ceil(Math.log(largestValueWithSingleUnitResolution)/Math.log(2))).longValue();
            this.subBucketHalfCountMagnitude = ((subBucketCountMagnitude > 1) ? subBucketCountMagnitude : 1) - 1;
            this.subBucketCount = new Double(Math.pow(2, (this.subBucketHalfCountMagnitude + 1))).intValue();
            this.subBucketHalfCount = new Double(this.subBucketCount / 2).intValue();
            Long trackableValue = new Long((this.subBucketCount - 1) << this.unitMagnitude);
            int bucketsNeeded = 1;
            while (trackableValue < this.highestTrackableValue) {
                trackableValue *= 2;
                bucketsNeeded++;
            }
            this.bucketCount = bucketsNeeded;

            this.countsArrayLength = (this.bucketCount + 1) * this.subBucketHalfCount;
        }

        Histogram(Histogram copy) {
            this.lowestTrackableValue = new Long(copy.lowestTrackableValue);
            this.highestTrackableValue = new Long(copy.highestTrackableValue);
            this.nSVD = new Integer(copy.nSVD);
            this.totalCount = new Long(copy.totalCount);
            this.unitMagnitude = new Long(copy.unitMagnitude);
            this.subBucketHalfCountMagnitude = new Long(copy.subBucketHalfCountMagnitude);
            this.subBucketCount = copy.subBucketCount;
            this.subBucketHalfCount = copy.subBucketHalfCount;
            this.bucketCount = copy.bucketCount;
            this.countsArrayLength = copy.countsArrayLength;

            this.count = new Long[copy.countsArrayLength];
            for (int i=0; i < copy.countsArrayLength; i++)
                this.count[i] = new Long(copy.count[i]);
        }

        public Histogram diff (Histogram newer) {
            Histogram diffHist = new Histogram(newer);
            diffHist.totalCount = diffHist.totalCount - this.totalCount;
            for (int i = 0; i < diffHist.countsArrayLength; i++) {
                diffHist.count[i] = diffHist.count[i] - this.count[i];
            }
            return diffHist;
        }

        public Long getCountAt (int bucketIndex, int subBucketIndex) {
            int bucketBaseIndex = (bucketIndex + 1) << this.subBucketHalfCountMagnitude;
            int offsetInBucket = subBucketIndex - this.subBucketHalfCount;
            int countIndex = bucketBaseIndex + offsetInBucket;
            return this.count[countIndex];
        }

        public double valueFromIndex (int bucketIndex, int subBucketIndex) {
            return subBucketIndex * Math.pow(2, bucketIndex + this.unitMagnitude);
        }

        public double getValueAtPercentile (double percentile) {
            int totalToCurrentIJ = 0;
            int countAtPercentile = new Double(Math.floor(((percentile / 100.0D) * this.totalCount) + 0.5D)).intValue(); // round to nearest
            for (int i = 0; i < this.bucketCount; i++) {
                int j = (i == 0) ? 0 : (this.subBucketCount / 2);
                for (; j < this.subBucketCount; j++) {
                    totalToCurrentIJ += this.getCountAt(i, j);
                    if (totalToCurrentIJ >= countAtPercentile) {
                        double valueAtIndex = this.valueFromIndex(i, j);
                        return valueAtIndex / 1000.0D;
                    }
                }
            }
            return 0D;
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("Usage server port reportIntervalSeconds");
            return;
        }

        final Client c = ClientFactory.createClient();
        System.out.println("Connecting to " + args[0] + " port " + Integer.valueOf(args[1]));
        c.createConnection( args[0], Integer.valueOf(args[1]));

        System.out.printf("%12s, %10s, %10s, %10s, %10s, %10s, %10s\n", "TIMESTAMP", "COUNT", "95", "99", "99.9", "99.99", "99.999");

        final SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");

        ScheduledExecutorService ses = Executors.newSingleThreadScheduledExecutor();
        ses.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    VoltTable table = null;
                    try {
                        table = c.callProcedure("@Statistics", "LATENCY_HISTOGRAM", 0).getResults()[0];
                    } catch (IOException|ProcCallException e) {
                        System.out.println("Failed to get statistics:");
                        e.printStackTrace();
                        System.exit(0);
                    }
                    table.advanceRow();
                    Date now = new Date(table.getLong(0));
                    Histogram newHistogram = getHistogram(ByteBuffer.wrap(table.getVarbinary(5)));
                    Histogram diffHistogram;
                    if (m_histogramData == null)
                        diffHistogram = newHistogram;
                    else
                        diffHistogram = m_histogramData.diff(newHistogram);

                    System.out.printf("%12s, %10d, %8.2fms, %8.2fms, %8.2fms, %8.2fms, %8.2fms\n",
                                      sdf.format(now),
                                      diffHistogram.totalCount,
                                      diffHistogram.getValueAtPercentile(95),
                                      diffHistogram.getValueAtPercentile(99),
                                      diffHistogram.getValueAtPercentile(99.9),
                                      diffHistogram.getValueAtPercentile(99.99),
                                      diffHistogram.getValueAtPercentile(99.999));
                    m_histogramData = newHistogram;
                }
            }, 0, Integer.valueOf(args[2]), TimeUnit.SECONDS);
    }

    static Histogram getHistogram(ByteBuffer histBuf) {
        histBuf.order(ByteOrder.LITTLE_ENDIAN);
        Long min = histBuf.getLong();
        Long max = histBuf.getLong();
        Integer nSVD = histBuf.getInt();
        Long cnt = histBuf.getLong();
        Histogram hist = new Histogram(min, max, nSVD, cnt);

        int i = 0;
        hist.count = new Long[histBuf.remaining() / 8];
        while (0 < histBuf.remaining()) {
            hist.count[i] = histBuf.getLong();
            ++i;
        }

        return hist;
    }
}
