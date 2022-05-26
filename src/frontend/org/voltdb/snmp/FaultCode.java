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

package org.voltdb.snmp;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.List;

import com.google_voltpatches.common.collect.ImmutableList;

public enum FaultCode {

    __unsmnp__, // ordinal 0 doesn't map to SNMP enumerations
    HOST_DOWN,
    HOST_UP,
    RESOURCE_TRIGGER,
    RESOURCE_CLEAR,
    CRASH,
    STATISTICS_TRIGGER,
    STATISTICS_CLEAR,
    PAUSE,
    RESUME,
    STREAM_BLOCKED;

    public final static List<FaultCode> values = ImmutableList.copyOf(values());

    static final public FaultCode valueOf(int ordinal) {
        checkArgument(ordinal > __unsmnp__.ordinal() && ordinal < values.size());
        return values.get(ordinal);
    }
}
