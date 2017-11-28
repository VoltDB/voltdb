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

// -*- mode: java; c-basic-offset: 4; -*-

package org.voltdb.exportclient;

import java.util.Properties;

import org.voltdb.export.AdvertisedDataSource;

public class MeasuringNoOpExporterLegacy extends ExportClientBase {
    //Dump time to export these many rows.
    long m_countTill = 4000000;
    //Dont count
    long m_primeTill = 40000;
    int m_perRowProcessingTimeNano = 0;
    long m_perRowProcessingTimeMs = 0;
    long m_firstBlockTimeMS = -1;
    long m_pcurCount = 0;
    long m_curCount = 0;
    long m_curRejectCount = 0;
    boolean m_reject = false;

    @Override
    public void configure(Properties config) throws Exception {
        m_countTill = Integer.parseInt(config.getProperty("count", "4000000"));
        m_primeTill = Integer.parseInt(config.getProperty("primecount", "40000"));
        m_perRowProcessingTimeNano = Integer.parseInt(config.getProperty("rowprocessingtimenanos", "0"));
        m_perRowProcessingTimeMs = Integer.parseInt(config.getProperty("rowprocessingtimems", "0"));
        m_reject = Boolean.parseBoolean(config.getProperty("reject", "false"));
    }

    class NoOpExportDecoder extends ExportDecoderBase {
        NoOpExportDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }

        @Override
        public void onBlockStart() {
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws ExportDecoderBase.RestartBlockException {
            if (m_reject) {
               if (++m_curRejectCount < m_countTill) {
                   throw new RestartBlockException(true);
               }
               //We have rejected all now process them from backlog.
            }
            if (++m_pcurCount <= m_primeTill) {
                //Priming
                return true;
            }
            if (m_firstBlockTimeMS == -1) {
                m_firstBlockTimeMS = System.currentTimeMillis();
            }
            //Priming is done.
            if (++m_curCount == m_countTill) {
                long timeTaken = (System.currentTimeMillis() - m_firstBlockTimeMS);
                double eps = ((double )(m_curCount-m_primeTill)/((double )timeTaken/1000));
                System.out.println("EPS:" + eps + " TimeTaken:" + timeTaken);
                //Reject again till we catchup again.
                m_curRejectCount = 0;
                m_firstBlockTimeMS = -1;
                m_curCount = 0;
            } else {
                //Only sleep when counting.
                if ((m_perRowProcessingTimeMs > 0) || (m_perRowProcessingTimeNano > 0)) {
                    try {
                        Thread.sleep(m_perRowProcessingTimeMs, m_perRowProcessingTimeNano);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            return true;
        }

        @Override
        public void onBlockCompletion() {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new NoOpExportDecoder(source);
    }
}
