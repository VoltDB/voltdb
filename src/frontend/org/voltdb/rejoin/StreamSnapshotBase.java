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

package org.voltdb.rejoin;

/**
 * Base class for reading and writing snapshot streams over the network.
 */
public abstract class StreamSnapshotBase {
    protected static final int typeOffset = 0; // 1 byte
    protected static final int tableIdOffset = typeOffset + 1; // 4 bytes
    protected static final int blockIndexOffset = tableIdOffset + 4; // 4 bytes
    protected static final int contentOffset = blockIndexOffset + 4;
}
