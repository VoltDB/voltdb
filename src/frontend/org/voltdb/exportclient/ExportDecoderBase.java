/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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


import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.export.AdvertisedDataSource;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;


/**
 * Provide the basic functionality of decoding tuples from our export wire
 * protocol into arrays of POJOs.
 *
 */
public abstract class ExportDecoderBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");
    public static final int INTERNAL_FIELD_COUNT = ExportRowData.INTERNAL_FIELD_COUNT;

    public static class RestartBlockException extends Exception {
        private static final long serialVersionUID = 1L;
        public final boolean requestBackoff;
        public RestartBlockException(boolean requestBackoff) {
            this.requestBackoff = requestBackoff;
        }
        public RestartBlockException(String msg, Throwable cause, boolean requestBackoff) {
            super(msg,cause);
            this.requestBackoff = requestBackoff;
        }
        public RestartBlockException(String msg, boolean requestBackoff) {
            super(msg);
            this.requestBackoff = requestBackoff;
        }
    }

    public static enum BinaryEncoding {
        BASE64,
        HEX
    }
    protected final int m_partition;
    protected final long m_startTS;
    public ExportDecoderBase(AdvertisedDataSource ads) {
        m_startTS = System.currentTimeMillis();
        m_partition = ads.partitionId;
    }

    /**
     * Process a row of octets from the Export stream. Overridden by subclasses
     * to provide whatever specific processing is desired by this ELClient
     *
     * @param row Decoded Export Data
     * @return whether or not the row processing was successful
     * @throws org.voltdb.exportclient.ExportDecoderBase.RestartBlockException
     */
    abstract public boolean processRow(ExportRowData row) throws RestartBlockException;

    abstract public void sourceNoLongerAdvertised(AdvertisedDataSource source);

    /**
     * Finalize operation upon block completion - provides a means for a
     * specific decoder to flush data to disk - virtual method
     */
    public void onBlockCompletion(ExportRowData row) throws RestartBlockException {
    }

    /**
     * Notify that a new block of data is going to be processed now
     */
    public void onBlockStart(ExportRowData row) throws RestartBlockException {

    }

    public ListeningExecutorService getExecutor() {
        return CoreUtils.LISTENINGSAMETHREADEXECUTOR;
    }

    public int getPartition() {
        return m_partition;
    }

}
