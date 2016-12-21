/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.catalog;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.UUID;

import org.junit.Test;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.MiscUtils;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestDRCatalogDiffs {
    @Test
    public void testHappyPath() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER, C3 VARCHAR(50) NOT NULL, " +
                "PRIMARY KEY (C1), LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 < 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C3, C1);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER, C3 VARCHAR(50) NOT NULL, " +
                "PRIMARY KEY (C1), LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 < 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C3, C1);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testMissingDRTableOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing DR table T2 on replica cluster"));
    }

    @Test
    public void testTableNotDROnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T2;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Table T1 has DR enabled on the master"));
    }

    @Test
    public void testMissingDRTableOnMaster() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testTableNotDROnMaster() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T2;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";
        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testExtraColumnOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Column{C2} from Table{T1} on master"));
    }

    @Test
    public void testMissingColumnOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Column{C2} from Table{T1} on replica"));
    }

    @Test
    public void testMissingColumnOnNonDRTable() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testAddedPartitionColumnOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field isreplicated in schema object Table{T1}"));
    }

    @Test
    public void testMissingPartitionColumnOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field isreplicated in schema object Table{T1}"));
    }

    @Test
    public void testDifferentPartitionColumn() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C2;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field partitioncolumn in schema object Table{T1}"));
    }

    @Test
    public void testExtraRowLimitOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testMissingRowLimitOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentRowLimit() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 50);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testAddedDeletePolicyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testMissingDeletePolicyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100);\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentDeletePolicy() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C2 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDeletePolicyWithDifferentWhereClauseConstant() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 100));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDeletePolicyWithDifferentWhereClauseOperator() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 = 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentRowLimitWithSameDeletePolicy() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 100 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, LIMIT PARTITION ROWS 50 EXECUTE (DELETE FROM T1 WHERE C1 > 50));\n" +
                "PARTITION TABLE T1 ON COLUMN C1;\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentColumnOrder() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C2 INTEGER NOT NULL, C1 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field index in schema object Column{C1}"));
    }

    @Test
    public void testDifferentColumnType() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 TIMESTAMP NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field type in schema object Column{C2}"));
    }

    @Test
    public void testDifferentVarcharColumnSize() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(100) NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field size in schema object Column{C2}"));
    }

    @Test
    public void testNullableOnlyOnMaster() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field nullable in schema object Column{C2}"));
    }

    @Test
    public void testNullableOnlyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field nullable in schema object Column{C2}"));
    }

    @Test
    public void testExtraUniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{FOO} from Table{T1} on master"));
    }

    @Test
    public void testMissingUniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{FOO} from Table{T1} on replica"));
    }

    @Test
    public void testReorderUniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C2, C1);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field index in schema object ColumnRef{C1}"));
    }

    @Test
    public void testIndexOnlyUniqueOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field unique in schema object Index{FOO}"));
    }

    @Test
    public void testIndexOnlyUniqueOnMaster() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("field unique in schema object Index{FOO}"));
    }

    @Test
    public void testUniqueIndexOnDifferentTable() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "CREATE UNIQUE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL);\n" +
                "CREATE UNIQUE INDEX foo ON T2 (C1, C2);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{FOO} from Table{T1} on replica"));
        assertTrue(diff.errors().contains("Missing Index{FOO} from Table{T2} on master"));
    }

    @Test
    public void testExtraPrimaryKeyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50), PRIMARY KEY (C1));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1} from Table{T1} on master"));
    }

    @Test
    public void testMissingPrimaryKeyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50), PRIMARY KEY (C1));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1} from Table{T1} on replica"));
    }

    @Test
    public void testExtraColumnInPrimaryKeyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C1));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C1, C2));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1_C2} from Table{T1} on master"));
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1} from Table{T1} on replica"));
    }

    @Test
    public void testMissingColumnInPrimaryKeyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C1, C2));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C1));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1_C2} from Table{T1} on replica"));
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1} from Table{T1} on master"));
    }

    @Test
    public void testReorderPrimaryKeyOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C1, C2));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50) NOT NULL, PRIMARY KEY (C2, C1));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C1_C2} from Table{T1} on replica"));
        assertTrue(diff.errors().contains("Missing Index{VOLTDB_AUTOGEN_IDX_PK_T1_C2_C1} from Table{T1} on master"));
    }

    @Test
    public void testExtraNonuniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C1);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testMissingNonuniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C1);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentNonuniqueIndexOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C1, C2);\n" +
                "DR TABLE T1;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 VARCHAR(50));\n" +
                "CREATE INDEX foo ON T1 (C2, C1);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testExtraViewOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE VIEW foo (C1, total) AS SELECT C1, COUNT(*) FROM T1 GROUP BY C1;\n" +
                "CREATE VIEW foo2 (C1, total) AS SELECT T1.C1, COUNT(*) FROM T1 JOIN T2 ON T1.C1 = T2.C1 GROUP BY T1.C1;\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testMissingViewOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE VIEW foo (C1, total) AS SELECT C1, COUNT(*) FROM T1 GROUP BY C1;\n" +
                "CREATE VIEW foo2 (C1, total) AS SELECT T1.C1, COUNT(*) FROM T1 JOIN T2 ON T1.C1 = T2.C1 GROUP BY T1.C1;\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;\n";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testDifferentViewOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE VIEW foo (C1, total) AS SELECT C1, COUNT(*) FROM T1 GROUP BY C1;\n" +
                "CREATE VIEW foo2 (C1, total) AS SELECT T1.C1, COUNT(*) FROM T1 JOIN T2 ON T1.C1 = T2.C1 GROUP BY T1.C1;\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";
        String replicaSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "CREATE VIEW foo (C1, C2, total) AS SELECT C1, C2, COUNT(*) FROM T1 WHERE C2 > 50 GROUP BY C1, C2;\n" +
                "CREATE VIEW foo2 (C1, C2, total) AS SELECT T1.C1, T2.C2, COUNT(*) FROM T1 JOIN T2 ON T1.C1 = T2.C1 GROUP BY T1.C1, T2.C2;\n" +
                "DR TABLE T1;\n" +
                "DR TABLE T2;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testNoDRTablesOnMaster() throws Exception {
        String replicaSchema =
                "CREATE TABLE T2 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T2;";

        CatalogDiffEngine diff = runCatalogDiff("", replicaSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testEmptySchemaOnReplica() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";

        CatalogDiffEngine diff = runCatalogDiff(masterSchema, "");
        assertFalse(diff.errors(), diff.supported());
        assertTrue(diff.errors().contains("Missing DR table T1 on replica cluster"));
    }

    @Test
    public void testNoDRTablesOnEitherSide() throws Exception {
        CatalogDiffEngine diff = runCatalogDiff("", "");
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testActiveActiveDR() throws Exception {
        String nodeOneSchema =
                "SET DR=ACTIVE;\n" +
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String nodeTwoSchema =
                "SET DR=ACTIVE;\n" +
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        CatalogDiffEngine diff = runCatalogDiff(nodeOneSchema, nodeTwoSchema);
        assertTrue(diff.errors(), diff.supported());
    }
    @Test
    public void testActivePassiveDR() throws Exception {
        String nodeOneSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String nodeTwoSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        CatalogDiffEngine diff = runCatalogDiff(nodeOneSchema, nodeTwoSchema);
        assertTrue(diff.errors(), diff.supported());
    }

    @Test
    public void testIncompatibleDRMode() throws Exception {
        String nodeOneSchema =
                "SET DR=ACTIVE;\n" +
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        String nodeTwoSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL);\n" +
                "DR TABLE T1;";
        CatalogDiffEngine diff = runCatalogDiff(nodeOneSchema, nodeTwoSchema);
        assertFalse(diff.supported());
        assertTrue(diff.errors().contains("Incompatible DR modes between two clusters"));
    }

    @Test
    public void testUnknownSchemaOption() throws Exception {
        String masterSchema =
                "CREATE TABLE T1 (C1 INTEGER NOT NULL, C2 BIGINT NOT NULL);\n" +
                "DR TABLE T1;";
        Catalog masterCatalog = createCatalog(masterSchema);

        String commands = DRCatalogDiffEngine.serializeCatalogCommandsForDr(masterCatalog).getSecond();
        String decodedCommands = Encoder.decodeBase64AndDecompress(commands);
        decodedCommands = decodedCommands.replaceFirst("set \\$PREV isDRed true", "set \\$PREV isDRed true\nset \\$PREV isASquirrel false");
        boolean threw = false;
        try {
            DRCatalogDiffEngine.deserializeCatalogCommandsForDr(Encoder.compressAndBase64Encode(decodedCommands));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("$PREV isASquirrel false"));
            threw = true;
        }
        assertTrue(threw);
    }

    private CatalogDiffEngine runCatalogDiff(String masterSchema, String replicaSchema) throws Exception {
        Catalog masterCatalog = createCatalog(masterSchema);
        Catalog replicaCatalog = createCatalog(replicaSchema);
        String commands = DRCatalogDiffEngine.serializeCatalogCommandsForDr(masterCatalog).getSecond();
        Catalog deserializedMasterCatalog = DRCatalogDiffEngine.deserializeCatalogCommandsForDr(commands);
        return new DRCatalogDiffEngine(replicaCatalog, deserializedMasterCatalog);
    }

    private Catalog createCatalog(String schema) throws Exception {
        File jarOut = new File(UUID.randomUUID() + ".jar");
        jarOut.deleteOnExit();

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();

        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileFromDDL(jarOut.getPath(), schemaPath);
        assertTrue("Compilation failed unexpectedly", success);

        Catalog catalog = new Catalog();
        catalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(MiscUtils.fileToBytes(new File(jarOut.getPath()))).getFirst()));
        return catalog;
    }
}
