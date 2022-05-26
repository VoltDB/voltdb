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

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManagerInterface.ExportMode;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

/**
 * An export client that pulls data and acks it back, but
 * never does anything with it.
 *
 * An optional config option allows sleeping before acknowledging
 * rows, turning this export client into a "slow" export target for
 * testing pusposes.
 *
 */
public class DiscardingExportClient extends ExportClientBase {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    // Using common executor service to avoid using {@code ExportDataSource} executor
    private static volatile ListeningExecutorService s_es;

    private int m_ackDelayMs = 0;

    class DiscardDecoder extends ExportDecoderBase {

        @Override
        public ListeningExecutorService getExecutor() {
            m_atomicWorkLock.lock();
            try {
                return s_es != null ? s_es : super.getExecutor();
            } finally {
                m_atomicWorkLock.unlock();
            }
        }

        public DiscardDecoder(AdvertisedDataSource source) {
            super(source);
            m_atomicWorkLock.lock();
            try {
                if (s_es == null && VoltDB.getExportManager().getExportMode() == ExportMode.BASIC) {
                    s_es = CoreUtils.getListeningSingleThreadExecutor(
                            "Common Discarding Export decoder thread", CoreUtils.MEDIUM_STACK_SIZE);
                }
            } catch (Exception e) {
                m_logger.error("Failed to create executor: " + e);
            } finally {
                m_atomicWorkLock.unlock();
            }
        }

        @Override
        public boolean processRow(ExportRow row) {
            if (m_ackDelayMs > 0) {
                try {
                    Thread.sleep(m_ackDelayMs);
                } catch (InterruptedException e) {
                }
            }
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            m_atomicWorkLock.lock();
            try {
                if (s_es != null) {
                    s_es.shutdown();
                    try {
                        s_es.awaitTermination(365, TimeUnit.DAYS);
                    } catch (InterruptedException e) {
                    }
                    s_es = null;
                }
            } finally {
                m_atomicWorkLock.unlock();
            }
        }
    }

    @Override
    public void configure(Properties config) throws Exception {
        m_logger.info("Configuring DiscardingExportClient: " + config);

        String sleepValue = config.getProperty("ackdelay");
        if (sleepValue != null) {
            int ackDelay = 0;
            try {
                ackDelay = Integer.parseInt(sleepValue);
                m_ackDelayMs = ackDelay;
            } catch (Exception e) {
                m_logger.error("Failed to decode ackdelay value \'" + sleepValue + "\': " + e);
            }
        }
        setRunEverywhere(Boolean.parseBoolean(config.getProperty("replicated", "false")));
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new DiscardDecoder(source);
    }
}
