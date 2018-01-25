/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltdb.export.AdvertisedDataSource;

/**
 * An export client that pulls data and acks it back, but
 * never does anythign with it.
 *
 */
public class DiscardingExportClient extends ExportClientBase {

    static class DiscardDecoder extends ExportDecoderBase {

        public DiscardDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }
    }

    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new DiscardDecoder(source);
    }
}
