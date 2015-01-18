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

package org.voltcore.messaging;

public enum Subject {
    DEFAULT,                 // All original message types are in the default subject
    SITE_FAILURE_UPDATE,     // Execution site data exchange when processing post-failure
    FAILURE,                 // Notification of node failures
    SITE_FAILURE_FORWARD;    // Agreement site forwards for failures detected on other sites

    private final byte m_id;

    private Subject() {
        final int ordinal = ordinal();
        assert(ordinal < Byte.MAX_VALUE);
        m_id = (byte)ordinal;
    }

    public byte getId() {
        return m_id;
    }
}
