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

package org.voltdb.task;

/**
 * Enum to represent the different scopes in which a {@code Task} can run
 */
public enum TaskScope {
    DATABASE, HOSTS, PARTITIONS;

    /**
     * Convert from an ID returned by {@link #getId()} back to the {@link TaskScope} instance
     *
     * @param id of a {@link TaskScope}
     * @return Instance of {@link TaskScope} {@code id} references
     * @throws IllegalArgumentException If {@code id} is not a valid ID
     */
    public static TaskScope fromId(byte id) {
        switch (id) {
        case 0:
            return DATABASE;
        case 1:
            return HOSTS;
        case 2:
            return PARTITIONS;
        default:
            throw new IllegalArgumentException("Unknown TaskScope ID: " + id);
        }
    }

    /**
     * Convert the name of a scope to a {@link TaskScope} instance in a case insensitive way. If {@code name} is
     * {@code null} return the default scope, {@link #DATABASE}
     *
     * @param name of scope or {@code null}
     * @return Instance of {@link TaskScope}
     * @throws IllegalArgumentException If {@code name} is not a valid scope
     */
    public static TaskScope fromName(String name) {
        return name == null ? DATABASE : valueOf(name.toUpperCase());
    }

    /**
     * Convert a scope ID to the name of that scope
     *
     * @param id of a {@link TaskScope}
     * @return Name of scope {@code id} references
     * @throws IllegalArgumentException If {@code id} is not a valid ID
     */
    public static String translateIdToName(byte id) {
        return fromId(id).name();
    }

    /**
     * @return The ID of this scope
     */
    public byte getId() {
        return (byte) ordinal();
    }
}
