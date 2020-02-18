/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.util.List;
import java.util.ListIterator;
import java.util.Set;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.collect.Sets;

public class SnapshotRequestConfig {
    protected static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");
    public static final String JKEY_NEW_PARTITION_COUNT = "newPartitionCount";
    public static final String JKEY_TRUNCATION_REQUEST_ID = "truncReqId";
    public static final String JKEY_TABLES = "tables";
    public static final String JKEY_SCHEMA_BUILDER = "schemaBuilder";

    public final boolean emptyConfig;
    public final Table[] tables;
    public final Integer newPartitionCount;
    public final String truncationRequestId;
    public final HiddenColumnFilter hiddenColumnFilter;

    public static HiddenColumnFilter getHiddenColumnFilter(JSONObject jsonObject) {
        if (jsonObject != null) {
            String schemaBuilderType = jsonObject.optString(JKEY_SCHEMA_BUILDER, null);
            if (schemaBuilderType != null) {
                return HiddenColumnFilter.valueOf(schemaBuilderType);
            }
        }
        return HiddenColumnFilter.NONE;
    }

    /**
     * @param tables    Tables to snapshot, cannot be null.
     */
    public SnapshotRequestConfig(List<Table> tables) {
        this(Preconditions.checkNotNull(tables), (Integer) null);
    }

    public SnapshotRequestConfig(List<Table> tables, HiddenColumnFilter schemaFilterType) {
        this(Preconditions.checkNotNull(tables), null, schemaFilterType);
    }

    public SnapshotRequestConfig(int newPartitionCount) {
        this((Table[]) null, Integer.valueOf(newPartitionCount), HiddenColumnFilter.NONE);
    }

    public SnapshotRequestConfig(int newPartitionCount, Database catalogDatabase) {
        this(getTablesToInclude(null, catalogDatabase), Integer.valueOf(newPartitionCount),
                HiddenColumnFilter.NONE);
    }

    protected SnapshotRequestConfig(List<Table> tables, Integer newPartitionCount) {
        this(tables.toArray(new Table[tables.size()]), newPartitionCount, HiddenColumnFilter.NONE);
    }

    protected SnapshotRequestConfig(List<Table> tables, Integer newPartitionCount,
            HiddenColumnFilter schemaFilterType) {
        this(tables.toArray(new Table[tables.size()]), newPartitionCount, schemaFilterType);
    }

    private SnapshotRequestConfig(Table[] tables, Integer newPartitionCount,
            HiddenColumnFilter schemaFilterType) {
        emptyConfig = false;
        this.tables = tables;
        this.newPartitionCount = newPartitionCount;
        truncationRequestId = null;
        this.hiddenColumnFilter = schemaFilterType;
    }

    public SnapshotRequestConfig(JSONObject jsData, Database catalogDatabase)
    {
        tables = getTablesToInclude(jsData, catalogDatabase);
        if (jsData == null) {
            emptyConfig = true;
            newPartitionCount = null;
            truncationRequestId = null;
            hiddenColumnFilter = HiddenColumnFilter.NONE;
        } else {
            emptyConfig = false;
            newPartitionCount = (Integer) jsData.opt(JKEY_NEW_PARTITION_COUNT);
            truncationRequestId = (String) jsData.opt(JKEY_TRUNCATION_REQUEST_ID);
            hiddenColumnFilter = getHiddenColumnFilter(jsData);
        }
    }

    private static Table[] getTablesToInclude(JSONObject jsData,
                                              Database catalogDatabase)
    {
        final List<Table> tables = SnapshotUtil.getTablesToSave(catalogDatabase);
        Set<String> tableNamesToInclude = null;
        Set<String> tableNamesToExclude = null;

        if (jsData != null) {
            JSONArray tableNames = jsData.optJSONArray(JKEY_TABLES);
            if (tableNames != null) {
                tableNamesToInclude = Sets.newHashSet();
                for (int i = 0; i < tableNames.length(); i++) {
                    try {
                        final String s = tableNames.getString(i).trim().toUpperCase();
                        if (!s.isEmpty()) {
                            tableNamesToInclude.add(s);
                        }
                    } catch (JSONException e) {
                        SNAP_LOG.warn("Unable to parse tables to include for snapshot", e);
                    }
                }
            }

            JSONArray excludeTableNames = jsData.optJSONArray("skiptables");
            if (excludeTableNames != null) {
                tableNamesToExclude = Sets.newHashSet();
                for (int i = 0; i < excludeTableNames.length(); i++) {
                    try {
                        final String s = excludeTableNames.getString(i).trim().toUpperCase();
                        if (!s.isEmpty()) {
                            tableNamesToExclude.add(s);
                        }
                    } catch (JSONException e) {
                        SNAP_LOG.warn("Unable to parse tables to exclude for snapshot", e);
                    }
                }
            }
        }

        if (tableNamesToInclude != null && tableNamesToInclude.isEmpty()) {
            // Stream snapshot may specify empty snapshot sometimes.
            tables.clear();
        } else if (tableNamesToInclude != null || tableNamesToExclude != null) {
            ListIterator<Table> iter = tables.listIterator();
            while (iter.hasNext()) {
                Table table = iter.next();
                if ((tableNamesToInclude != null && !tableNamesToInclude.remove(table.getTypeName())) ||
                    (tableNamesToExclude != null && tableNamesToExclude.remove(table.getTypeName()))) {
                    // If the table index is not in the list to include or
                    // is in the list to exclude, remove it
                    iter.remove();
                }
            }
        }

        if (tableNamesToInclude != null && !tableNamesToInclude.isEmpty()) {
            throw new IllegalArgumentException(
                    "The following tables were specified to include in the snapshot, but are not present in the database: " +
                    Joiner.on(", ").join(tableNamesToInclude));
        }
        if (tableNamesToExclude != null && !tableNamesToExclude.isEmpty()) {
            throw new IllegalArgumentException(
                    "The following tables were specified to exclude from the snapshot, but are not present in the database: " +
                    Joiner.on(", ").join(tableNamesToExclude));
        }

        return tables.toArray(new Table[tables.size()]);
    }

    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        if (tables != null) {
            stringer.key(JKEY_TABLES);
            stringer.array();
            for (Table table : tables) {
                stringer.value(table.getTypeName());
            }
            stringer.endArray();
        }
        if (newPartitionCount != null) {
            stringer.keySymbolValuePair(JKEY_NEW_PARTITION_COUNT, newPartitionCount.longValue());
        }
        stringer.keySymbolValuePair(JKEY_SCHEMA_BUILDER, hiddenColumnFilter.name());
    }
}
