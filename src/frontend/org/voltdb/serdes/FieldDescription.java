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
package org.voltdb.serdes;

import org.voltdb.VoltType;

/**
 * Simple class that describes a field that can be serialized
 */
public final class FieldDescription {
    private final String m_name;
    private final VoltType m_type;
    private final boolean m_nullable;

    public FieldDescription(String name, VoltType type) {
        this(name, type, true);
    }

    public FieldDescription(String name, VoltType type, boolean nullable) {
        m_name = name;
        m_type = type;
        m_nullable = nullable;
    }

    /**
     * @return The name of the field
     */
    public String name() {
        return m_name;
    }

    /**
     * @return The type of the field
     */
    public VoltType type() {
        return m_type;
    }

    /**
     * @return {@code true} if the field is allowed to be {@code null}
     */
    public boolean isNullable() {
        return m_nullable;
    }

    @Override
    public String toString() {
        return "FieldDescription [name=" + m_name + ", type=" + m_type + ", nullable=" + m_nullable + "]";
    }

}
