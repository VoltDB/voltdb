/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.exportclient;

import java.util.ArrayList;
import java.util.Arrays;

import org.mockito.Mockito;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.export.ExportManagerInterface.ExportMode;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class ExportClientTestBase {

    static final GeographyPointValue GEOG_POINT = GeographyPointValue.fromWKT("point(-122 37)");
    static final GeographyValue GEOG = GeographyValue.fromWKT("polygon((0 0, 1 1, 0 1, 0 0))");


    static final String[] COLUMN_NAMES = {"tid", "ts", "sq", "pid", "site", "op",
            "tinyint", "smallint", "integer", "bigint", "float", "timestamp", "string", "decimal",
            "geog_point", "geog"};

    static final VoltType[] COLUMN_TYPES
            = {VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT, VoltType.BIGINT,
            VoltType.TINYINT, VoltType.SMALLINT, VoltType.INTEGER,
            VoltType.BIGINT, VoltType.FLOAT, VoltType.TIMESTAMP,VoltType.STRING, VoltType.DECIMAL,
            VoltType.GEOGRAPHY_POINT, VoltType.GEOGRAPHY};

    static final Integer COLUMN_LENGTHS[] = {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 2048
    };

    static VoltTable vtable = new VoltTable(
            new VoltTable.ColumnInfo("VOLT_TRANSACTION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_TIMESTAMP", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_EXPORT_SEQUENCE_NUMBER", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_PARTITION_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_OP", VoltType.BIGINT),
            new VoltTable.ColumnInfo("VOLT_SITE_ID", VoltType.BIGINT),
            new VoltTable.ColumnInfo("tinyint", VoltType.TINYINT),
            new VoltTable.ColumnInfo("smallint", VoltType.SMALLINT),
            new VoltTable.ColumnInfo("integer", VoltType.INTEGER),
            new VoltTable.ColumnInfo("bigint", VoltType.BIGINT),
            new VoltTable.ColumnInfo("float", VoltType.FLOAT),
            new VoltTable.ColumnInfo("timestamp", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("string", VoltType.STRING),
            new VoltTable.ColumnInfo("decimal", VoltType.DECIMAL),
            new VoltTable.ColumnInfo("geog_point", VoltType.GEOGRAPHY_POINT),
            new VoltTable.ColumnInfo("geog", VoltType.GEOGRAPHY)
    );

    static AdvertisedDataSource constructTestSource(boolean replicated, int partition) {
        return constructTestSource(replicated, partition, "yankeelover");
    }

    static AdvertisedDataSource constructTestSource(boolean replicated, int partition, String tableName) {
        ArrayList<String> col_names = new ArrayList<String>();
        ArrayList<VoltType> col_types = new ArrayList<VoltType>();
        for (int i = 0; i < COLUMN_TYPES.length; i++) {
            col_names.add(COLUMN_NAMES[i]);
            col_types.add(COLUMN_TYPES[i]);
        }
        String partCol = replicated ? null : "smallint";
        //clear the table
        vtable.clearRowData();
        AdvertisedDataSource source = new AdvertisedDataSource(partition, tableName,
                partCol, 0, 32, col_names, col_types, Arrays.asList(COLUMN_LENGTHS),
                AdvertisedDataSource.ExportFormat.SEVENDOTX);
        return source;
    }

    protected void setup() {
        ExportManagerInterface mockEM = Mockito.mock(ExportManagerInterface.class);
        Mockito.when(mockEM.getExportMode()).thenReturn(ExportMode.BASIC);
        ExportManagerInterface.setInstanceForTest(mockEM);
        vtable.clearRowData();
    }
}
