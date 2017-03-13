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
    public static final int PROTOCOL_VERSION = Integer.getInteger("DR_PROTOCOL_VERSION", 7);
    public static final int COMPATIBLE_PROTOCOL_VERSION = 3;
    public static final int MIXED_SIZE_PROTOCOL_VERSION = 4;
    public static final int MUTLICLUSTER_PROTOCOL_VERSION = 7;
}
