/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.export;
/**
 * m_mailbox queue. Make this a VoltMessage, though it is sort of
 * meaningless, to satisfy ExecutionSite mailbox requirements.
 */

import java.nio.ByteBuffer;

import org.voltcore.messaging.VoltMessage;
import org.voltdb.utils.NotImplementedException;

/**
 * Silly pair to couple a protostate block with a message for the
 * m_mailbox queue. Make this a VoltMessage, though it is sort of
 * meaningless, to satisfy ExecutionSite mailbox requirements.
 */
public class ExportInternalMessage extends VoltMessage {
    public final ExportStateBlock m_sb;
    public final ExportProtoMessage m_m;

    public ExportInternalMessage(ExportStateBlock sb, ExportProtoMessage m) {
        m_sb = sb;
        m_m = m;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) {
        throw new NotImplementedException("Invalid serialization request.");
    }

    @Override
    protected void initFromBuffer(ByteBuffer buf) {
        throw new NotImplementedException("Invalid serialization request.");
    }
}
