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

package org.voltdb.sysprocs.saverestore;

/**
 * This is enumeration of "path" that is passed around when path is other than SNAP_PATH the path is locally picked
 * by the Scan, Delete, Save methods.
 * @author akhanzode
 */
public enum SnapshotPathType {
    SNAP_PATH, // For direct path based
    SNAP_CL, // For snapshots in command log snapshots
    SNAP_AUTO, // For auto snapshots.
    SNAP_NO_PATH // For streaming snapshots
}
