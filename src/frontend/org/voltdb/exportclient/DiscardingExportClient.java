/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManagerInterface;
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

    private int m_ackDelaySeconds = 0;

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
                if (s_es == null && ExportManagerInterface.instance().getExportMode() == ExportMode.BASIC) {
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
            if (m_ackDelaySeconds > 0) {
                m_logger.info("Sleep " + m_ackDelaySeconds + " before processing row ...");
                try {
                    Thread.sleep(m_ackDelaySeconds * 1000);
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
            int ackDelaySeconds = 0;
            try {
                ackDelaySeconds = Integer.parseInt(sleepValue);
                m_ackDelaySeconds = ackDelaySeconds;
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
