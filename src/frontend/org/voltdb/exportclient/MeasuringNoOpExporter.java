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

// -*- mode: java; c-basic-offset: 4; -*-

package org.voltdb.exportclient;

import java.util.Properties;
import org.voltcore.logging.VoltLogger;

import org.voltdb.export.AdvertisedDataSource;

public class MeasuringNoOpExporter extends ExportClientBase {
    private static final VoltLogger m_logger = new VoltLogger("HOST");
    //Dump time to export these many rows.
    long m_countTill = 4000000;
    //Dont count
    long m_primeTill = 40000;
    int m_perRowProcessingTimeNano = 0;
    long m_perRowProcessingTimeMs = 0;
    long m_firstBlockTimeMS = -1;
    long m_pcurCount = 0;
    long m_curCount = 0;

    @Override
    public void configure(Properties config) throws Exception {
        m_countTill = Integer.parseInt(config.getProperty("count", "4000000"));
        m_primeTill = Integer.parseInt(config.getProperty("primecount", "40000"));
        m_perRowProcessingTimeNano = Integer.parseInt(config.getProperty("rowprocessingtimenanos", "0"));
        m_perRowProcessingTimeMs = Integer.parseInt(config.getProperty("rowprocessingtimems", "0"));
    }

    class NoOpExportDecoder extends ExportDecoderBase {
        NoOpExportDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }

        @Override
        public void onBlockStart(ExportRow row) {
        }

        boolean logOnce = true;
        @Override
        public boolean processRow(ExportRow row) throws ExportDecoderBase.RestartBlockException {
            if (m_pcurCount < m_primeTill) {
                m_pcurCount++;
                //Priming
                return true;
            }
            if (logOnce) {
                m_logger.info("Priming done: " + m_pcurCount + " Cur: " + m_curCount);
                logOnce = false;
            }
            if (m_firstBlockTimeMS == -1) {
                m_firstBlockTimeMS = System.currentTimeMillis();
            }
            //Priming is done.
            if (++m_curCount == m_countTill) {
                long timeTaken = (System.currentTimeMillis() - m_firstBlockTimeMS);
                double eps = ((double )(m_curCount-m_primeTill)/((double )timeTaken/1000));
                m_logger.info("EPS:" + eps + " Time:" + timeTaken);
                m_firstBlockTimeMS = -1;
                m_curCount = 0;
            } else {
                //Only sleep when specified.
                if ((m_perRowProcessingTimeMs > 0) || (m_perRowProcessingTimeNano > 0)) {
                    try {
                        Thread.sleep(m_perRowProcessingTimeMs, m_perRowProcessingTimeNano);
                    } catch (InterruptedException ex) {
                        ;
                    }
                }
            }
            return true;
        }

        @Override
        public void onBlockCompletion(ExportRow row) {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new NoOpExportDecoder(source);
    }
}
