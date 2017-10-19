/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.dr2;

public interface DRProtocol {
    // Also update DRTupleStream.h if version changes
    public static final int PROTOCOL_VERSION = 8;
    public static final int COMPATIBLE_PROTOCOL_VERSION = 7;

    // constant versions that don't change across releases
    public static final int MIXED_SIZE_PROTOCOL_VERSION = 4;
    public static final int MULTICLUSTER_PROTOCOL_VERSION = 7;
    public static final int ELASTICADD_PROTOCOL_VERSION = 8;

    // all partial MP txns go into SP streams
    public static final int DR_NO_MP_START_PROTOCOL_VERSION = 3;
    // all partial MP txns except those with table truncation record go to MP stream separately without coordination
    public static final int DR_UNCOORDINATED_MP_START_PROTOCOL_VERSION = 4;
    // partial MP txns of the same MP txn coordinated and combined before going to MP stream
    public static final int DR_COORDINATED_MP_START_PROTOCOL_VERSION = 6;

    // all previous compatible protocol versions
    public static final int FIRST_COMPATIBLE_PROTOCOL_VERSION = 3;
}
