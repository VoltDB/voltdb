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

package org.voltdb.exportclient;

import java.util.Properties;

import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManagerInterface.ExportMode;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * An export client that pulls data and acks it back, but
 * never does anythign with it.
 *
 */
public class RejectingExportClient extends ExportClientBase {

    @Override
    public void configure(Properties config) throws Exception {
    }

    static class DiscardDecoder extends ExportDecoderBase {

        final ListeningExecutorService m_es;
        public DiscardDecoder(AdvertisedDataSource source) {
            super(source);
            if (VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                m_es = CoreUtils.getListeningSingleThreadExecutor(
                        "Kafka Export decoder for partition " + source.partitionId, CoreUtils.MEDIUM_STACK_SIZE);
            } else {
                m_es = null;
            }
        }

        @Override
        public boolean processRow(ExportRow rowinst) throws RestartBlockException {
            throw new RestartBlockException(true);
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }

        @Override
        public ListeningExecutorService getExecutor() {
            return m_es;
        }

    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new DiscardDecoder(source);
    }
}
