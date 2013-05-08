/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.io.IOException;

import java.util.ArrayList;

import java.util.HashSet;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Map.Entry;

import com.google.common.primitives.Longs;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;

import org.voltdb.catalog.Table;

import org.voltdb.dtxn.SiteTracker;

import org.voltdb.rejoin.StreamSnapshotDataTarget;

import org.voltdb.SnapshotDataFilter;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;

import org.voltdb.sysprocs.SnapshotRegistry;
import org.voltdb.SystemProcedureExecutionContext;

import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltTable;

/**
 * Create a snapshot write plan for snapshots streamed to other sites
 * (specified in the jsData).  Each source site specified in the streamPairs
 * key will write all of its tables, partitioned and replicated, to a target
 * per-site.
 */
public class StreamSnapshotWritePlan extends SnapshotWritePlan
{
    protected boolean createSetupInternal(
            String file_path, String file_nonce,
            long txnId, Map<Integer, Long> partitionTransactionIds,
            JSONObject jsData, SystemProcedureExecutionContext context,
            String hostname, final VoltTable result,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            SiteTracker tracker, long timestamp) throws IOException
    {
        // not empty if targeting only one site (used for rejoin)
        // set later from the "data" JSON string
        Map<Long, Long> streamPairs;

        assert(SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.isEmpty());

        final List<Table> tables = getTablesToInclude(jsData, context);

        final AtomicInteger numTables = new AtomicInteger(tables.size());
        final SnapshotRegistry.Snapshot snapshotRecord =
            SnapshotRegistry.startSnapshot(
                    txnId,
                    context.getHostId(),
                    file_path,
                    file_nonce,
                    SnapshotFormat.STREAM,
                    tables.toArray(new Table[0]));

        // table schemas for all the tables we'll snapshot on this partition
        Map<Integer, byte[]> schemas = new HashMap<Integer, byte[]>();
        for (final Table table : tables) {
            VoltTable schemaTable = CatalogUtil.getVoltTable(table);
            schemas.put(table.getRelativeIndex(), schemaTable.getSchemaBytes());
        }

        try {
            streamPairs = getStreamPairs(jsData, tracker);
        }
        catch (JSONException e) {
            // Can't proceed without valid JSON, Ned
            SnapshotRegistry.discardSnapshot(snapshotRecord);
            return true;
        }

        Map<Long, SnapshotDataTarget> sdts = new HashMap<Long, SnapshotDataTarget>();
        if (streamPairs.size() > 0) {
            SNAP_LOG.debug("Sites to stream from: " +
                    CoreUtils.hsIdCollectionToString(streamPairs.keySet()));
            for (Entry<Long, Long> entry : streamPairs.entrySet()) {
                sdts.put(entry.getKey(), new StreamSnapshotDataTarget(entry.getValue(), schemas));
            }
        }
        else
        {
            // There's no work to do on this host, just claim success, return an empty plan, and things
            // will sort themselves out properly
            return false;
        }

        for (Entry<Long, SnapshotDataTarget> entry : sdts.entrySet()) {
            final ArrayList<SnapshotTableTask> partitionedSnapshotTasks =
                new ArrayList<SnapshotTableTask>();
            final ArrayList<SnapshotTableTask> replicatedSnapshotTasks =
                new ArrayList<SnapshotTableTask>();
            SnapshotDataTarget sdt = entry.getValue();
            m_targets.add(sdt);
            for (final Table table : tables)
            {
                final Runnable onClose = new TargetStatsClosure(sdt, table.getTypeName(),
                        numTables, snapshotRecord);
                sdt.setOnCloseHandler(onClose);

                final SnapshotTableTask task =
                    new SnapshotTableTask(
                            table.getRelativeIndex(),
                            sdt,
                            new SnapshotDataFilter[0], // This task no longer needs partition filtering
                            table.getIsreplicated(),
                            table.getTypeName());

                if (table.getIsreplicated()) {
                    replicatedSnapshotTasks.add(task);
                } else {
                    partitionedSnapshotTasks.add(task);
                }
                result.addRow(context.getHostId(),
                        hostname,
                        table.getTypeName(),
                        "SUCCESS",
                        "");
            }

            // Stream snapshots need to write all partitioned tables to all
            // selected partitions and all replicated tables to all selected
            // partitions
            List<Long> thisOne = new ArrayList<Long>();
            thisOne.add(entry.getKey());
            placePartitionedTasks(partitionedSnapshotTasks, thisOne);
            placeReplicatedTasks(replicatedSnapshotTasks, thisOne);
        }
        return false;
    }

    private List<Table> getTablesToInclude(JSONObject jsData,
                                           SystemProcedureExecutionContext context)
    {
        final List<Table> tables = SnapshotUtil.getTablesToSave(context.getDatabase());
        final Set<Integer> tableIdsToInclude = new HashSet<Integer>();

        if (jsData != null) {
            JSONArray tableIds = jsData.optJSONArray("tableIds");
            if (tableIds != null) {
                for (int i = 0; i < tableIds.length(); i++) {
                    try {
                        tableIdsToInclude.add(tableIds.getInt(i));
                    } catch (JSONException e) {
                        SNAP_LOG.warn("Unable to parse tables to include for stream snapshot", e);
                    }
                }
            }
        }

        if (tableIdsToInclude.isEmpty()) {
            // It doesn't make any sense to take a snapshot that doesn't include any table,
            // it must be that the request doesn't specify a table filter,
            // so default to all tables.
            return tables;
        }

        ListIterator<Table> iter = tables.listIterator();
        while (iter.hasNext()) {
            Table table = iter.next();
            if (!tableIdsToInclude.contains(table.getRelativeIndex())) {
                // If the table index is not in the list to include, remove it
                iter.remove();
            }
        }

        return tables;
    }

    private Map<Long, Long> getStreamPairs(JSONObject jsData, SiteTracker tracker) throws JSONException
    {
        Map<Long, Long> streamPairs = new HashMap<Long, Long>();

        if (jsData != null) {
            List<Long> localHSIds = Longs.asList(tracker.getLocalSites());
            JSONObject sp = jsData.getJSONObject("streamPairs");
            @SuppressWarnings("unchecked")
            Iterator<String> it = sp.keys();
            while (it.hasNext()) {
                String key = it.next();
                long sourceHSId = Long.valueOf(key);
                // See whether this source HSID is a local site, if so, we need
                // the partition ID
                if (localHSIds.contains(sourceHSId)) {
                    Long destHSId = Long.valueOf(sp.getString(key));
                    streamPairs.put(sourceHSId, destHSId);
                }
            }
        }

        return streamPairs;
    }
}
