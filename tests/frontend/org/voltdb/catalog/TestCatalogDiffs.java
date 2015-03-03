/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.TableHelper;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.CatalogBuilder.RoleInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestCatalogDiffs extends TestCase {

    private static final RoleInfo[] NO_GROUPS = { };

    private static final RoleInfo[] ONE_GROUP = {
        new RoleInfo("group1", true, true, true, true, false, false) };

    private static final RoleInfo[] SOME_GROUPS = {
        new RoleInfo("group1", true, true, true, true, false, false),
        new RoleInfo("group2", true, true, true, true, false, true) };

    private static final Class<?>[] BASEPROCS = {
            org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
            org.voltdb.benchmark.tpcc.procedures.delivery.class };

    private static final Class<?>[] EXPANDEDPROCS = {
            org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
            org.voltdb.benchmark.tpcc.procedures.delivery.class,
            org.voltdb.benchmark.tpcc.procedures.slev.class };

    private static final Class<?>[] FEWERPROCS = {
        org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class };

    private static final Class<?>[] CONFLICTPROCS = {
            org.voltdb.catalog.InsertNewOrder.class,
            org.voltdb.benchmark.tpcc.procedures.delivery.class };

    private static String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

    private static String compile(String name, RoleInfo[] gi, Class<?>... procList) {
        CatalogBuilder cb = TPCCProjectBuilder.catalogBuilderNoProcs()
        .addProcedures(procList)
        .addRoles(gi)
        ;
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String retval = testDir + File.separator + "tpcc-catalogcheck-" + name + ".jar";
        assertTrue(cb.compile(retval));
        return retval;
    }

    // Also used by TestCatalogUtil, consider moving this method into CatalogUtil
    // as a test support function.
    public static Catalog catalogForJar(String pathToJar) throws IOException {
        byte[] bytes = MiscUtils.fileToBytes(new File(pathToJar));
        Catalog catalog = CatalogUtil.deserializeCatalogFromJarFileBytes(bytes);
        assertNotNull(catalog);
        return catalog;
    }

    private Catalog catalogViaJar(CatalogBuilder cb, String name) throws IOException {
        String path = testDir + File.separator + name + ".jar";
        assertTrue(cb.compile(path));
        return catalogForJar(path);
    }

    private static String verifyDiff(
            Catalog catOriginal,
            Catalog catUpdated)
    {
        return verifyDiff(catOriginal, catUpdated, null, null);
    }

    private static String verifyDiff(
            Catalog catOriginal,
            Catalog catUpdated,
            Boolean expectSnapshotIsolation,
            Boolean worksWithElastic)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        String commands = diff.commands();
        System.out.println("DIFF COMMANDS:");
        System.out.println(commands);
        catOriginal.execute(commands);
        assertTrue(diff.supported());
        assertEquals(0, diff.tablesThatMustBeEmpty().length);
        if (expectSnapshotIsolation != null) {
            assertEquals((boolean) expectSnapshotIsolation, diff.requiresSnapshotIsolation());
        }
        if (worksWithElastic != null) {
            assertEquals((boolean)worksWithElastic, diff.worksWithElastic());
        }
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());

        String desc = diff.getDescriptionOfChanges();

        System.out.println("========================");
        System.out.println(desc);
        System.out.println("========================");

        return desc;
    }

    private static void verifyDiffRejected(Catalog catOriginal, Catalog catUpdated)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        String originalSerialized = catOriginal.serialize();
        catOriginal.execute(diff.commands());
        String updatedOriginalSerialized = catOriginal.serialize();
        if (diff.supported()) {
            System.out.println("TCD DEBUG Unexpectedly accepted difference:\n");
            System.out.println("TCD DEBUG BEFORE: " + originalSerialized);
            System.out.println("TCD DEBUG  AFTER: " + updatedOriginalSerialized);
        }
        assertFalse(diff.supported());
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());
    }

    private static void verifyDiffIfEmptyTable(Catalog catOriginal,Catalog catUpdated)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertTrue(diff.supported());
        assertTrue(diff.tablesThatMustBeEmpty().length > 0);
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());
    }


    public void testAddProcedure() throws IOException {
        String original = compile("base", NO_GROUPS, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("expanded", NO_GROUPS, EXPANDEDPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure slev added."));
    }

    public void testModifyProcedureCode() throws IOException {
        String original = compile("base", NO_GROUPS, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("conflict", NO_GROUPS, CONFLICTPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure InsertNewOrder has been modified."));
    }

    public void testDeleteProcedure() throws IOException {
        String original = compile("base", NO_GROUPS, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("fewer", NO_GROUPS, FEWERPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure delivery dropped."));
    }

    public void testAddGroup() throws IOException {
        String original = compile("base", NO_GROUPS, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        String updated = compile("group", ONE_GROUP, BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddSecondGroup() throws IOException {
        String original = compile("base", ONE_GROUP, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        String updated = compile("groups", SOME_GROUPS, BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteGroup() throws IOException {
        String original = compile("base", ONE_GROUP, BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no groups or users this time
        String updated = compile("base", NO_GROUPS, BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDiffOfIdenticalCatalogs() throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;" +
                "")
        .addProcedures(ProcedureA.class)
        ;
        Catalog c3 = catalogViaJar(cb, "identical3");

        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;" +
                "")
        .addProcedures(ProcedureA.class)
        ;
        Catalog c4 = catalogViaJar(cb, "identical4");

        CatalogDiffEngine diff = new CatalogDiffEngine(c3, c4);
        // don't reach this point.
        c3.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = c3.serialize();
        assertEquals(updatedOriginalSerialized, c4.serialize());
    }

    // N.B. Some of the testcases assume this exact table structure... if you change it,
    // check the callers.
    Catalog getCatalogForTable(String tableName, String catname) throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE " + tableName + " (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE " + tableName + " ON COLUMN C1;")
        .addProcedures(tableName.equals("A") ? ProcedureA.class : ProcedureB.class)
        ;
        return catalogViaJar(cb, "test-" + catname);
    }

    Catalog getCatalogForTable(String tableName, String catname, VoltTable t) throws IOException {
        return getCatalogForTable(tableName, catname, t, false);
    }

    Catalog getExportCatalogForTable(String tableName, String catname, VoltTable t) throws IOException {
        return getCatalogForTable(tableName, catname, t, true);
    }

    private Catalog getCatalogForTable(String tableName, String catname, VoltTable t, boolean export) throws IOException {
        CatalogBuilder cb = new CatalogBuilder(TableHelper.ddlForTable(t));
        if (export) {
            cb.addLiteralSchema("EXPORT TABLE " + TableHelper.getTableName(t) + ";");
        }
        return catalogViaJar(cb, "test-" + catname);
    }


    // N.B. Some of the testcases assume this exact table structure .. if you change it,
    // check the callers...
    private Catalog get2ColumnCatalogForTable(String tableName, String catname) throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "CREATE TABLE " + tableName +
                " (C1 BIGINT NOT NULL, C2 BIGINT DEFAULT 0 NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE " + tableName + " ON COLUMN C1;" +
                "")
        .addProcedures(tableName.equals("A") ? ProcedureA.class : ProcedureB.class)
        ;
        return catalogViaJar(cb, "test-" + catname);
    }


    public void testAddTable() throws IOException {
        // Start with table A.
        CatalogBuilder cb = new CatalogBuilder(
                "CREATE TABLE A (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testaddtable1");

        // Add table B and recompile
        cb.addLiteralSchema(
                "CREATE TABLE B (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE B ON COLUMN C1;")
        .addProcedures(ProcedureB.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "testaddtable2");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testDropTable() throws IOException {
        // Start with table A and B.
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nCREATE TABLE B (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;" +
                "\nPARTITION TABLE B ON COLUMN C1;")
        .addProcedures(ProcedureA.class, ProcedureB.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testdroptable1");

        // Create a catalog with just table A
        Catalog catUpdated = getCatalogForTable("A", "droptable2");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testViewConversion() throws IOException {
        // Start with table A.
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        ;
        Catalog catOriginal = catalogViaJar(cb, "convertmatview1");

        // Add table B and recompile
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE TABLE MATVIEW(C1 BIGINT NOT NULL, NUM INTEGER);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        ;
        Catalog catUpdated = catalogViaJar(cb, "convertmatview2");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testAddTableColumn() throws IOException {
        Catalog catOriginal = getCatalogForTable("A", "addtablecolumnrejected1");
        Catalog catUpdated = get2ColumnCatalogForTable("A", "addtablecolumnrejected2");
        verifyDiff(catOriginal, catUpdated, true, null);
    }

    public void testRemoveTableColumn() throws IOException {
        Catalog catOriginal = get2ColumnCatalogForTable("A", "removetablecolumn2");
        Catalog catUpdated = getCatalogForTable("A", "removetablecolumn1");
        verifyDiff(catOriginal, catUpdated, true, null);
    }

    public void testModifyTableColumn() throws IOException {
        // should pass
        VoltTable t1 = TableHelper.quickTable("(SMALLINT, VARCHAR30, VARCHAR80)");
        VoltTable t2 = TableHelper.quickTable("(INTEGER, VARCHAR40, VARCHAR120)");
        Catalog catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        Catalog catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiff(catOriginal, catUpdated, true, null);

        // even pass when crossing the inline/out-of-line boundary
        t1 = TableHelper.quickTable("(VARBINARY30)");
        t2 = TableHelper.quickTable("(VARBINARY70)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiff(catOriginal, catUpdated, true, null);

        // fail integer contraction if non-empty empty
        t1 = TableHelper.quickTable("(BIGINT)");
        t2 = TableHelper.quickTable("(INTEGER)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        // fail string contraction if non-empty table
        t1 = TableHelper.quickTable("(VARCHAR35)");
        t2 = TableHelper.quickTable("(VARCHAR34)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        // fail - change export schema if non-empty
        t1 = TableHelper.quickTable("(VARCHAR35)");
        t2 = TableHelper.quickTable("(VARCHAR34)");
        catOriginal = getExportCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getExportCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyVarcharColumns() throws IOException {
        Catalog catOriginal, catUpdated;
        CatalogBuilder cb;
        String report;

         // start with a table
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar(5 BYTES) ) ;");
        catOriginal = catalogViaJar(cb, "testVarchar0");

        // change from character to bytes
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar( 20 BYTES), v2 varchar(5 BYTES) );");
        catUpdated = catalogViaJar(cb, "testVarchar1");
        report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));

        // size not satisfied if non-empty table
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar( 15 BYTES), v2 varchar(5 BYTES) );");
        catUpdated = catalogViaJar(cb, "testVarchar2");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        // inline character to not in line bytes.
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(100 BYTES), v2 varchar(5 BYTES) );");
        catUpdated = catalogViaJar(cb, "testVarchar3");
        report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));

        // bytes to character
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar(  5 BYTES) ) ;");
        catOriginal = catalogViaJar(cb, "testVarchar0");

        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar(  5) );");
        catUpdated = catalogViaJar(cb, "testVarchar4");
        report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));

        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar( 15) );");
        catUpdated = catalogViaJar(cb, "testVarchar5");
        report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));

        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar(150) );");
        catUpdated = catalogViaJar(cb, "testVarchar6");
        report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));

        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT, v1 varchar(  5      ), v2 varchar(  3) );");
        catUpdated = catalogViaJar(cb, "testVarchar6");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testAddNonNullityRejected() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT         , PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddNonNullity1");

        // add a non-null constraint
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "testAddNonNullity2");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testDropNonNullity() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testDropNonNullity1");

        // add a non-null constraint
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT         , PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "testDropNonNullity2");

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Table A has been modified."));
    }

    public void testAddUniqueCoveringTableIndex() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddUniqueCoveringTableIndex1");

        // add an index
        cb.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C1,C2);");
        Catalog catUpdated = catalogViaJar(cb, "testAddUniqueCoveringTableIndex2");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testAddUniqueNonCoveringTableIndexRejectedIfNotEmpty() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected1");

        // add an index
        cb.addLiteralSchema("\nCREATE ASSUMEUNIQUE INDEX IDX ON A(C2);");
        Catalog catUpdated = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected2");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testShrinkUniqueNonCoveringTableIndexRejectedIfNonEmpty() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1, C2));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected1");

        // shrink the pkey index
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected2");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testExpandUniqueNonCoveringTableIndex() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected1");

        // shrink the pkey index
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1, C2));" +
                                 "PARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "testAddUniqueNonCoveringTableIndexRejected2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddNonUniqueTableIndex() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                                 "PARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddNonUniqueTableIndex1");

        // add an index
        cb.addLiteralSchema("\nCREATE INDEX IDX ON A(C1,C2);");
        Catalog catUpdated = catalogViaJar(cb, "testAddNonUniqueTableIndex2");

        verifyDiff(catOriginal, catUpdated);
    }

    private void renameUniqueIndexes() throws IOException {
        // start with a table
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nCREATE UNIQUE INDEX IDX ON A(C1,C2);" +
                "\nCREATE INDEX IDX2 ON A(C2);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "renameUniqueIndexes1");

        // rename an index
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nCREATE UNIQUE INDEX RYANLIKETHEYANKEES ON A(C1,C2);" +
                "\nCREATE INDEX GAGNAMSTYLE ON A(C2);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "renameUniqueIndexes2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveUniqueIndex() throws IOException {
        // start with a table with an index
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nCREATE UNIQUE INDEX IDX ON A(C1,C2);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testRemoveUniqueIndex1");

        // remove the index
        Catalog catUpdated = get2ColumnCatalogForTable("A", "testRemoveUniqueIndex2");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveNonUniqueIndex() throws IOException {
        // start with a table with an index
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                "\nCREATE INDEX IDX ON A(C1,C2);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testRemoveNonUniqueIndex1");

        // remove the index
        Catalog catUpdated = get2ColumnCatalogForTable("A", "testRemoveNonUniqueIndex2");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddTableConstraintRejectedIfNotEmpty() throws IOException {
        // start with a table without a PKEY
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "testAddTableConstraintRejected1");

        // add a constraint (this function creates a primary key)
        Catalog catUpdated = getCatalogForTable("A", "testAddTableConstraintRejected2");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testRemoveTableConstraint() throws IOException {
        // with the primary key
        Catalog catOriginal = getCatalogForTable("A", "dropconstraint1");

        // without the primary key
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT DEFAULT 0 NOT NULL);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "dropconstraint2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddMaterializedView() throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "addmatview1");

        cb.addLiteralSchema(
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        Catalog catUpdated = catalogViaJar(cb, "addmatview2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveMaterializedView() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "remmatview1");

        // without a view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "remmatview2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewColumnRejected() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C, NUM) AS " +
                "\n    SELECT C3, COUNT(*) FROM A GROUP BY C3;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "modmatview1");

        // with a slightly different view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C, NUM) AS " +
                "\n    SELECT C2, COUNT(*) FROM A GROUP BY C2;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "modmatview2");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewStructureRejectedIfEmpty() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "modmatview1");

        // with a quite different view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C2, C1, NUM) AS " +
                "\n    SELECT C2, C1, COUNT(*) FROM A GROUP BY C2, C1;" +
                "PARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "modmatview2");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewAddPredicateRejected() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class)
        ;
        Catalog catOriginal = catalogViaJar(cb, "addpredmatview1");

        // without a view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catUpdated = catalogViaJar(cb, "addpredmatview2");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewDropPredicateRejected() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catOriginal = catalogViaJar(cb, "droppredmatview1");

        // without a view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catUpdated = catalogViaJar(cb, "droppredmatview2");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewPredicateRejected() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A WHERE C1 < 0 GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catOriginal = catalogViaJar(cb, "modpredmatview1");

        // without a view
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catUpdated = catalogViaJar(cb, "modpredmatview2");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewSourceRejectedIfEmpty() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catOriginal = catalogViaJar(cb, "resrcmatview1");

        // without an added column (should work with empty table)
        cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catUpdated = catalogViaJar(cb, "resrcmatview2");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testRemoveTableAndMaterializedView() throws IOException {
        // with a view
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;" +
                "\nPARTITION TABLE A ON COLUMN C1;")
        .addProcedures(ProcedureA.class);
        Catalog catOriginal = catalogViaJar(cb, "remtablematview1");

        // without a view
        cb = new CatalogBuilder(
                "\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nPARTITION TABLE B ON COLUMN C1;")
        .addProcedures(ProcedureB.class)
        ;
        Catalog catUpdated = catalogViaJar(cb, "remtablematview2");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSetting() throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);")
        .addStmtProcedure("the_requisite_procedure", "select * from A;")
        ;
        Catalog catOriginal = catalogViaJar(cb, "addpart1");

        cb.addLiteralSchema("PARTITION TABLE A ON COLUMN C1;");
        Catalog catUpdated = catalogViaJar(cb, "addpart2");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSettingOfExportTable() throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                "\nEXPORT TABLE A;")
        .addStmtProcedure("the_requisite_procedure", "insert into A values (?, ?);")
        ;
        Catalog catOriginal = catalogViaJar(cb, "elastic1a");

        cb.addLiteralSchema("PARTITION TABLE A ON COLUMN C1;");
        Catalog catUpdated = catalogViaJar(cb, "elastic2a");
        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testUnchangedCatalogIsCompatibleWithElastic() throws IOException {
        CatalogBuilder cb = new CatalogBuilder(
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "CREATE PROCEDURE the_requisite_procedure AS select * from A;" +
                "");
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());
        Catalog catUpdated = catalogForJar(jar.getPath());
        verifyDiff(catOriginal, catUpdated, null, true);
    }

    public void testChangedCatalogNotCompatibleWithElasticAddProcedure() throws IOException {
        String originalDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "CREATE PROCEDURE the_requisite_procedure AS select * from A;\n" +
                "";
        CatalogBuilder cb = new CatalogBuilder(originalDDL);
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());

        String moreDDL = "CREATE PROCEDURE another_procedure AS select * from A;\n";
        CatalogBuilder cb2 = new CatalogBuilder(originalDDL + moreDDL);
        File jar2 = cb2.compileToTempJar();
        Catalog catUpdated = catalogForJar(jar2.getPath());
        verifyDiff(catOriginal, catUpdated, null, false);
    }

    public void testChangesCataloNotCompatibleWithElasticAddTable() throws IOException {
        String originalDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "CREATE PROCEDURE the_requisite_procedure AS select * from A;\n" +
                "";
        CatalogBuilder cb = new CatalogBuilder(originalDDL);
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());

        String moreDDL = "CREATE TABLE Another (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n";
        CatalogBuilder cb2 = new CatalogBuilder(originalDDL + moreDDL);
        File jar2 = cb2.compileToTempJar();
        Catalog catUpdated = catalogForJar(jar2.getPath());
        verifyDiff(catOriginal, catUpdated, null, false);
    }

    public void testEnableDROnEmptyTable() throws IOException {
        String originalDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "PARTITION TABLE A ON COLUMN C1;\n" +
                "";
        CatalogBuilder cb = new CatalogBuilder(originalDDL);
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());

        String moreDDL = "DR TABLE A;\n";
        CatalogBuilder cb2 = new CatalogBuilder(originalDDL + moreDDL);
        File jar2 = cb2.compileToTempJar();
        Catalog catUpdated = catalogForJar(jar2.getPath());
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testDisableDROnTable() throws IOException {
        String originalDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "PARTITION TABLE A ON COLUMN C1;\n" +
                "DR TABLE A;\n" +
                "";
        CatalogBuilder cb = new CatalogBuilder(originalDDL);
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());

        // Creating a catalog that disables DR for a table by first enabling
        // and then disabling it requires a "last DR command wins" policy.
        // I'm not sure that this is an important aspect to test -- but this
        // is how the original version of testDisableDROnTable was coded so
        // I preserved it. --paul
        String replacementDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "PARTITION TABLE A ON COLUMN C1;\n" +
                // leaving out this part: "DR TABLE A;\n" +
                "";
        CatalogBuilder cb2 = new CatalogBuilder(replacementDDL);
        File jar2 = cb2.compileToTempJar();
        Catalog catUpdated = catalogForJar(jar2.getPath());
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testDisableDROnEmptyTable_LastDRCommandWins() throws IOException {
        String originalDDL =
                "CREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);\n" +
                "PARTITION TABLE A ON COLUMN C1;\n" +
                "DR TABLE A;\n" +
                "";
        CatalogBuilder cb = new CatalogBuilder(originalDDL);
        File jar = cb.compileToTempJar();
        Catalog catOriginal = catalogForJar(jar.getPath());

        // Creating a catalog that disables DR for a table by first enabling
        // and then disabling it requires a "last DR command wins" policy.
        // I'm not sure that this is an important aspect to test -- but this
        // is how the original version of testDisableDROnTable was coded so
        // I preserved it. --paul
        String moreDDL = "DR TABLE A DISABLE;\n";
        CatalogBuilder cb2 = new CatalogBuilder(originalDDL + moreDDL);
        File jar2 = cb2.compileToTempJar();
        Catalog catUpdated = catalogForJar(jar2.getPath());
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }
}
