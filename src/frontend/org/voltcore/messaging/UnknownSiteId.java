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
package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.utils.CoreUtils;

public class UnknownSiteId extends VoltMessage {
    private VoltMessage m_message;

    public UnknownSiteId() {}

    UnknownSiteId(VoltMessage message) {
        m_message = message;
    }

    @Override
    protected void initFromBuffer(VoltMessageFactory factory, ByteBuffer buf) throws IOException {
        m_message = factory.createMessageFromBuffer(buf, -1);
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltMessageFactory.UNKNOWN_SITE_ID);
        m_message.flattenToBuffer(buf);
    }

    @Override
    public int getSerializedSize() {
        return super.getSerializedSize() + m_message.getSerializedSize();
    }

    public VoltMessage getMessage() {
        return m_message;
    }

    void setMessage(VoltMessage message) {
        m_message = message;
    }

    @Override
    public String toString() {
        return "UnknownSiteId [ m_sourceHSId=" + CoreUtils.hsIdToString(m_sourceHSId) + " m_message=" + m_message + "]";
    }
}
