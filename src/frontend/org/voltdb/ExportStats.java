/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
package org.voltdb;
/* STAKUTIS CREATED THIS 'ExportStats' CLASS */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.voltdb.VoltTable.ColumnInfo;


public class ExportStats extends StatsSource {
  private HashMap<String, ExportStatsRow> m_stats=new HashMap<String, ExportStatsRow>();
  private boolean m_intervalCollection = false;
  static ExportStats singleton;  // STAKUTIS HACK for now

  public class ExportStatsRow {
    public long m_partitionId=0;
   public String m_streamName="";
   public String m_exportTarget="";
   public long m_exportActive=0;
   public long m_tupleCount=0;
   public long m_tuplePending=0;
   public long m_averageLatency=0; // for current counting-session
   public long m_maxLatency=0; // for current counting-session

   public long m_tuplesSentSinceClear=0;
   public long m_totalMSSentSinceClear=0;
   private long m_overallTuplesSent = 0;
   private long m_overallMSSent = 0;
   private long m_overallMaxLatency = 0;

   ExportStatsRow() {
   }
  }


    public static interface Columns {
        // column for both tables
        public static final String PARTITION_ID = "PARTITION_ID";
        public static final String STREAM_NAME = "STREAM_NAME";
        public static final String EXPORT_TARGET = "EXPORT_TARGET";
        public static final String EXPORT_ACTIVE = "EXPORT_ACTIVE";
        public static final String TUPLE_COUNT = "TUPLE_COUNT";
        public static final String TUPLE_PENDING = "TUPLE_PENDING";
        public static final String AVERAGE_LATENCY = "AVERAGE_LATENCY";
        public static final String MAX_LATENCY = "MAX_LATENCY";
    }

    /* Constructor*/
    public ExportStats() {
        super(false);
     assert(singleton == null);
        singleton = this;  // STAKUTIS hack for now
    }

    static public ExportStats get() {
     if (singleton == null) {
      // This happens during a restart and there are already stream tables
      singleton = new ExportStats();
     }
     return singleton;  // STAKUTIS hack for now
    }

    public ExportStatsRow get(String streamName) {
     // Will create a new row if necessary
     ExportStatsRow row=m_stats.get(streamName);
     if (row == null) m_stats.put(streamName,  row=new ExportStatsRow());
     row.m_streamName = streamName;
     return row;
    }

    public void remove(String streamName) {
     m_stats.remove(streamName);
     return;
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        columns.add(new ColumnInfo(Columns.PARTITION_ID, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.STREAM_NAME, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.EXPORT_TARGET, VoltType.STRING));
        columns.add(new ColumnInfo(Columns.EXPORT_ACTIVE, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.TUPLE_COUNT, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.TUPLE_PENDING, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.AVERAGE_LATENCY, VoltType.BIGINT));
        columns.add(new ColumnInfo(Columns.MAX_LATENCY, VoltType.BIGINT));
    }


    @Override
    protected void updateStatsRow(Object rowKey, Object rowValues[]) {
        super.updateStatsRow(rowKey, rowValues);
        Map.Entry<String, ExportStatsRow> mentry=(Map.Entry)rowKey;
        ExportStatsRow stat = mentry.getValue();
        rowValues[columnNameToIndex.get(Columns.PARTITION_ID)] = stat.m_partitionId;
        rowValues[columnNameToIndex.get(Columns.STREAM_NAME)] = stat.m_streamName;
        rowValues[columnNameToIndex.get(Columns.EXPORT_TARGET)] = stat.m_exportTarget;
        rowValues[columnNameToIndex.get(Columns.EXPORT_ACTIVE)] = stat.m_exportActive;
        rowValues[columnNameToIndex.get(Columns.TUPLE_COUNT)] = stat.m_tupleCount;
        rowValues[columnNameToIndex.get(Columns.TUPLE_PENDING)] = stat.m_tuplePending;

  if (stat.m_maxLatency > stat.m_overallMaxLatency)
   stat.m_overallMaxLatency = stat.m_maxLatency;

  if (m_intervalCollection) {
            rowValues[columnNameToIndex.get(Columns.AVERAGE_LATENCY)] = stat.m_averageLatency;
            rowValues[columnNameToIndex.get(Columns.MAX_LATENCY)] = stat.m_maxLatency;
   stat.m_overallTuplesSent += stat.m_tuplesSentSinceClear;
   stat.m_overallMSSent += stat.m_totalMSSentSinceClear;
   stat.m_tuplesSentSinceClear = 0;
   stat.m_totalMSSentSinceClear = 0;
         stat.m_maxLatency = 0;
         stat.m_averageLatency = 0;
        } else {
         if (stat.m_tuplesSentSinceClear + stat.m_overallTuplesSent > 0)
          rowValues[columnNameToIndex.get(Columns.AVERAGE_LATENCY)] =
           (stat.m_totalMSSentSinceClear + stat.m_overallMSSent) /
           (stat.m_tuplesSentSinceClear + stat.m_overallTuplesSent);
         rowValues[columnNameToIndex.get(Columns.MAX_LATENCY)] = stat.m_overallMaxLatency;
        }
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(final boolean interval) {
        m_intervalCollection = interval;
     Set set = m_stats.entrySet();
     Iterator iterator=set.iterator();
     return iterator;

    }


}
