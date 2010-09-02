/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.twitter.hadoop.hdfs;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportDecoderBase;

public class ExportToHDFSDecoder extends ExportDecoderBase {

    private RollingAppender out;

    public ExportToHDFSDecoder(AdvertisedDataSource source, FileSystem hdfs, String uri) {
        super(source);
        out = new RollingAppender(hdfs, uri, source.tableName());
    }

    @Override
    public boolean processRow(int rowSize, byte[] rowData) {
        Object[] row = null;
        try {
            row = decodeRow(rowData);
        } catch (IOException e) {
            e.printStackTrace();
        }

        StringBuilder sb = new StringBuilder();
        for (Object col : row) {
            sb.append(col + ",");
        }
        sb.setLength(sb.length() - 1);

        out.append(sb.toString());

        return true;
    }
}
