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

package org.voltdb.sysprocs.saverestore;

import java.util.HashSet;
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

import com.google_voltpatches.common.base.Preconditions;

public class SnapshotRequestConfig {
    protected static final VoltLogger SNAP_LOG = new VoltLogger("SNAPSHOT");

    public final Table[] tables;

    /**
     * @param tables    Tables to snapshot, cannot be null.
     */
    public SnapshotRequestConfig(List<Table> tables)
    {
        Preconditions.checkNotNull(tables);
        this.tables = tables.toArray(new Table[0]);
    }

    public SnapshotRequestConfig(JSONObject jsData, Database catalogDatabase)
    {
        tables = getTablesToInclude(jsData, catalogDatabase);
    }

    private static Table[] getTablesToInclude(JSONObject jsData,
                                              Database catalogDatabase)
    {
        final List<Table> tables = SnapshotUtil.getTablesToSave(catalogDatabase);
        final Set<String> tableNamesToInclude = new HashSet<String>();

        if (jsData != null) {
            JSONArray tableNames = jsData.optJSONArray("tableNames");
            if (tableNames != null) {
                for (int i = 0; i < tableNames.length(); i++) {
                    try {
                        tableNamesToInclude.add(tableNames.getString(i));
                    } catch (JSONException e) {
                        SNAP_LOG.warn("Unable to parse tables to include for stream snapshot", e);
                    }
                }
            }
        }

        ListIterator<Table> iter = tables.listIterator();
        while (iter.hasNext()) {
            Table table = iter.next();
            if (!tableNamesToInclude.contains(table.getTypeName())) {
                // If the table index is not in the list to include, remove it
                iter.remove();
            }
        }

        return tables.toArray(new Table[0]);
    }

    public void toJSONString(JSONStringer stringer) throws JSONException
    {
        if (tables != null) {
            stringer.key("tableNames");
            stringer.array();
            for (Table table : tables) {
                stringer.value(table.getTypeName());
            }
            stringer.endArray();
        }
    }
}
