/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltProtocolHandler;

/**
 * Read listing commands from a client and respond with the
 * requested advertisements.
 *
 *
 */
public class ExportListingService extends VoltProtocolHandler {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    private Connection m_cxn = null;

    private void handleMessage(ExportProtoMessage m) {
        FastSerializer fs = new FastSerializer();
        ExportWindowDirectory lib = ExportManager.instance().m_windowDirectory;

        // serialize the advertisment list as:
        // <count of advertisements>
        // <fast serialized strings>*
        if (m.isPoll()) {
            List<ExportAdvertisement> listing = lib.createListing();
            try {
                fs.writeInt(listing.size());
                for (ExportAdvertisement ad : listing) {
                    ad.serialize(fs);
                }
                m_cxn.writeStream().enqueue(fs.getBBContainer());
            } catch (IOException e) {
                exportLog.error("Failed to create export advertisement listing.");
            }
        }

    }

    @Override
    public void started(Connection c) {
        m_cxn = c;
    }

    @Override
    public void handleMessage(ByteBuffer message, Connection c) {
        try {
            FastDeserializer fds = new FastDeserializer(message);
            final ExportProtoMessage m = ExportProtoMessage.readExternal(fds);
            handleMessage(m);
        } catch (IOException e) {
            exportLog.error("Failed to deserialize export protocol message.", e);
        }
    }

    @Override
    public int getMaxRead() {
        return 1020;
    }

    @Override
    public int getExpectedOutgoingMessageSize() {
        return 1020;
    }

    @Override
    public Runnable onBackPressure() {
        return null;
    }

    @Override
    public Runnable offBackPressure() {
        return null;
    }

    @Override
    public QueueMonitor writestreamMonitor() {
        return null;
    }


}
