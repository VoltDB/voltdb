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

package org.voltdb.common;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;

import java.util.EnumSet;
import java.util.List;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * Enum representing state of the node
 * WAITINGFORLEADER, SHUTTINGDOWN, STOPPED appear to be unused [dp]
 */
public enum NodeState {

    INITIALIZING, //Initial state
    WAITINGFORLEADER, //Waiting for leader none of the nodes are up
    RECOVERING,
    REJOINING,
    PAUSED,
    UPDATING,
    UP,
    SHUTTINGDOWN,
    STOPPED;

    static final List<NodeState> values = ImmutableList.copyOf(values());
    static final EnumSet<NodeState> meshed = EnumSet.of(RECOVERING,REJOINING,PAUSED,UPDATING,UP);
    static final EnumSet<NodeState> unmeshed = EnumSet.of(INITIALIZING,WAITINGFORLEADER);
    static final EnumSet<NodeState> operational = EnumSet.of(PAUSED,UPDATING,UP);
    static final EnumSet<NodeState> catalogued = EnumSet.of(RECOVERING,REJOINING,PAUSED,UPDATING,UP);

    public static final String toListString() {
        return values.toString();
    }

    public boolean meshed() {
        return meshed.contains(this);
    }

    public boolean unmeshed() {
        return unmeshed.contains(this);
    }

    public boolean operational() {
        return operational.contains(this);
    }

    public boolean catalogued() {
        return catalogued.contains(this);
    }

    public byte byteOrdinal() {
        return new Integer(ordinal()).byteValue();
    }

    public static NodeState fromOrdinal(int ordinal) {
        checkArgument(ordinal >= 0 || ordinal < values.size(),
                "ordinal %s is out of range", ordinal);
        return values.get(ordinal);
    }

    public static NodeState fromOrdinal(byte oridinal) {
        return fromOrdinal(new Byte(oridinal).intValue());
    }
}
