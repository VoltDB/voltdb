/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import org.json_voltpatches.JSONObject;
import org.voltdb.catalog.Database;

/**
 * Snapshot request configuration which is for CSV snapshots to enforce exclusion of all hidden columns
 */
class CsvSnapshotRequestConfig extends SnapshotRequestConfig {
    CsvSnapshotRequestConfig(JSONObject jsData, Database catalogDatabase) {
        super(jsData, catalogDatabase);
    }

    @Override
    public HiddenColumnFilter getHiddenColumnFilter() {
        // CSV snapshots never include hidden columns
        return HiddenColumnFilter.ALL;
    }

    @Override
    protected boolean includeSystemTables() {
        // CSV snapshots never include system tables
        return false;
    }
}
