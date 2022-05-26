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

package org.voltdb.client;

import static com.google_voltpatches.common.base.Preconditions.checkArgument;
import static com.google_voltpatches.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.security.Principal;

import org.voltdb.common.Constants;

public final class DelegatePrincipal implements Principal {
    public final static int MAX_DELEGATE_NAME_SIZE = 4096;

    private final String m_name;
    private final int m_id;

    public DelegatePrincipal(String name, int id) {
        checkArgument(
                name != null && !name.trim().isEmpty()
                && name.length() < MAX_DELEGATE_NAME_SIZE,
                "passed name is null, blank, empty, or too large"
                );
        m_name = name;
        m_id = id;
    }

    public DelegatePrincipal(ByteBuffer bb) {
        checkArgument(
                checkNotNull(bb, "passed byte buffer is null").remaining() >= 8,
                "unexpected byte buffer size"
                );
        m_id = bb.getInt();
        int size = bb.getInt();
        checkArgument(
                size >= 0 && size <= MAX_DELEGATE_NAME_SIZE,
                "delegate name size %s is negative or too large", size
                );
        if (bb.hasArray()) {
            m_name = new String(
                    bb.array(),
                    bb.arrayOffset() + bb.position(),
                    size,
                    Constants.UTF8ENCODING
                    );
            bb.position(bb.position() + size);
        } else {
            byte [] namebuff = new byte[size];
            bb.get(namebuff);
            m_name = new String(namebuff, Constants.UTF8ENCODING);
        }
    }

    public DelegatePrincipal(byte [] payload) {
        this(ByteBuffer.wrap(checkNotNull(payload, "passed payload is null")));
    }

    @Override
    public String getName() {
        return m_name;
    }

    public int getId() {
        return m_id;
    }

    public int wrappedSize() {
        return 4 + 4 + m_name.getBytes(Constants.UTF8ENCODING).length;
    }

    public void wrap(ByteBuffer bb) {
        byte [] bytes = m_name.getBytes(Constants.UTF8ENCODING);
        bb.putInt(m_id);
        bb.putInt(bytes.length);
        bb.put(bytes);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + m_id;
        result = prime * result + ((m_name == null) ? 0 : m_name.hashCode());
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
        DelegatePrincipal other = (DelegatePrincipal) obj;
        if (m_id != other.m_id)
            return false;
        if (m_name == null) {
            if (other.m_name != null)
                return false;
        } else if (!m_name.equals(other.m_name))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "DelegatePrincipal [m_name=" + m_name + ", m_id=" + m_id + "]";
    }
}
