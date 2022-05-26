/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltdb.sysprocs.saverestore;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltSnapshotFile;

public class TestSnapshotUtil {
    @Test
    public void getRequiredSnapshotableTableNames() throws Exception {
        String schema = "CREATE TABLE NORMAL_A (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL);\n"
                + "CREATE TABLE NORMAL_B (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL);\n" +
                // NORMAL_C is partitioned on C1.
                "CREATE TABLE NORMAL_C (C1 TINYINT NOT NULL, C2 VARCHAR(3) NOT NULL);\n"
                + "CREATE VIEW VIEW_A (TOTAL_ROWS) AS SELECT COUNT(*) FROM NORMAL_A;\n" +
                // VIEW_C1 is an explicitly partitioned persistent table view (included in the snapshot).
                "CREATE VIEW VIEW_C1 AS SELECT C1, COUNT(*) FROM NORMAL_C GROUP BY C1;\n" +
                // VIEW_C2 is an implicitly partitioned persistent table view (not included in the snapshot).
                "CREATE VIEW VIEW_C2 AS SELECT C2, COUNT(*) FROM NORMAL_C GROUP BY C2;\n" +
                // EXPORT_A is partitioned on C1.
                "CREATE STREAM EXPORT_A (C1 BIGINT NOT NULL, C2 SMALLINT NOT NULL);\n" +
                // VIEW_E1 contains the partition column of its source streamed table (included in the snapshot).
                "CREATE VIEW VIEW_E1 AS SELECT C1, COUNT(*) FROM EXPORT_A GROUP BY C1;\n";
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final File file = VoltSnapshotFile.createTempFile("testGetNormalTableNamesFromInMemoryJar", ".jar", new File(testDir));

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("NORMAL_C", "C1");
        builder.addPartitionInfo("EXPORT_A", "C1");

        builder.compile(file.getPath());
        byte[] bytes = MiscUtils.fileToBytes(file);
        InMemoryJarfile jarfile = CatalogUtil.loadInMemoryJarFile(bytes);
        file.delete();

        Set<String> definedRequiredTableNames = new HashSet<>();
        definedRequiredTableNames.add("NORMAL_A");
        definedRequiredTableNames.add("NORMAL_B");
        definedRequiredTableNames.add("NORMAL_C");
        definedRequiredTableNames.add("VIEW_E1");

        Set<String> requiredTableNames = SnapshotUtil.getRequiredSnapshotableTableNames(jarfile);
        assertEquals(definedRequiredTableNames, requiredTableNames);
    }
}
