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
    long m_countTill = 100000;
    long m_firstBlockTimeMS = -1;
    long m_curCount = 0;

    @Override
    public void configure(Properties config) throws Exception {
        m_countTill = Integer.parseInt(config.getProperty("count", "100000"));
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
            if (m_firstBlockTimeMS == -1) {
                m_firstBlockTimeMS = System.currentTimeMillis();
            }
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) throws ExportDecoderBase.RestartBlockException {
            if (++m_curCount == m_countTill) {
                System.out.println("Time taken to export " + m_countTill + " Is: " + (System.currentTimeMillis() - m_firstBlockTimeMS));
                m_firstBlockTimeMS = -1;
                m_curCount = 0;
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
