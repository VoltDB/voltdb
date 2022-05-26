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
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltdb.export.AdvertisedDataSource;

/**
 * Provides an extensible base class for writing Export clients.
 * Manages a set of connections to servers and a record of all of the partitions and tables that
 * are actively being exported.
 */
public class ExportClientBase {

    // object used to synchronize on so the shutdown hook can behave
    final java.util.concurrent.locks.ReentrantLock m_atomicWorkLock =
            new java.util.concurrent.locks.ReentrantLock(true);
    protected boolean m_hasPrintedAutodiscoveryWarning = false;
    private boolean m_runEveryWhere = false;
    private String m_targetName;

    /**
     * This enum defines how decoding should be threaded.
     */
    public static enum DecodingPolicy {
        /**
         * Decode each table and partition separately, i.e.
         * they may be decoded in separate threads.
         */
        BY_PARTITION_TABLE,
        /**
         * Decode all table partitions together, i.e. in the same thread.
         */
        BY_TABLE
    }

    /**
     * Override this to take in configuration properties and setup your export
     * client connector.
     *
     * @param config
     * @throws Exception
     */
    public void configure(Properties config) throws Exception {
        throw new UnsupportedOperationException("Configuration of onserver export client "
                + "should be export client specific and must be implemented.");
    }

    /**
     * Return the decoding policy.
     * <p>
     * By default the export clients allow decoding by table and partition.
     * Some export clients may require decoding  all table partitions together.
     *
     * @return the decoding policy
     */
    public DecodingPolicy getDecodingPolicy() {
        return DecodingPolicy.BY_PARTITION_TABLE;
    }

    public boolean isRunEverywhere() {
        return m_runEveryWhere;
    }

    public void setRunEverywhere(boolean flag) {
        m_runEveryWhere = flag;
    }

    public String getTargetName() {
        return m_targetName;
    }

    public void setTargetName(String targetName) {
        m_targetName = targetName;
    }

    /**
     * Called when the export client is going to shutdown.
     */
    public void shutdown() {
    }

    /**
     * Allow derived clients to implement their own construction of
     * ExportDecoders for the data sources provided by the server on this Export
     * connection.
     *
     * @param source
     * @return ExportDecoderBase corresponding to AdvertisedDataSource
     */
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        throw new UnsupportedOperationException("constructExportDecoder of onserver export client "
                + "should be export client specific and must be implemented.");
    }
}
