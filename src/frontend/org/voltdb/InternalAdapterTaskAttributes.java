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

package org.voltdb;

import org.voltdb.client.Priority;

final class InternalAdapterTaskAttributes implements InvocationClientHandler, InternalConnectionContext {

    final boolean m_isAdmin;
    final long m_connectionId;
    final String m_name;
    final InternalConnectionContext m_proxy;
    final int m_priority;

    public InternalAdapterTaskAttributes(String adapterName, boolean isAdmin, long connectionId) {
        m_isAdmin = isAdmin;
        m_connectionId = connectionId;
        m_name = adapterName;
        m_proxy = null;
        m_priority = Priority.SYSTEM_PRIORITY;
    }

    public InternalAdapterTaskAttributes(InternalConnectionContext ctx, long connectionId) {
        m_name = ctx.getName();
        m_priority = ctx.getPriority();
        m_proxy = ctx;
        m_connectionId = connectionId;
        m_isAdmin = false;
    }

    @Override
    final public boolean isAdmin() {
        return m_isAdmin;
    }

    @Override
    final public long connectionId() {
        return m_connectionId;
    }

    @Override
    final public String getName() {
        return m_name;
    }

    @Override
    public int getPriority() {
        return m_priority;
    }

    final public InvocationClientHandler asHandler() {
        return this;
    }

    final public InternalConnectionContext asContext() {
        return this;
    }

    final public boolean isImporter() {
        return m_proxy != null;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + (int) (m_connectionId ^ (m_connectionId >>> 32));
        result = prime * result + (m_isAdmin ? 1231 : 1237);
        result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
        result = prime * result + m_priority;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InternalAdapterTaskAttributes other = (InternalAdapterTaskAttributes) obj;
        if (m_connectionId != other.m_connectionId)
            return false;
        if (m_isAdmin != other.m_isAdmin)
            return false;
        if (m_name == null) {
            if (other.m_name != null)
                return false;
        } else if (!m_name.equals(other.m_name))
            return false;
        if (m_priority != other.m_priority)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return String.format(
                "InternalAdapterTaskAttributes [isAdmin=%b, connectionId=%d, name=%s, priority=%d]",
                m_isAdmin, m_connectionId, m_name, m_priority);
    }
}
