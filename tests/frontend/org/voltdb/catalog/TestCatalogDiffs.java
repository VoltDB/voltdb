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
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestCatalogDiffs extends TestCase {

    Class<?>[] BASEPROCS =     { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                 org.voltdb.benchmark.tpcc.procedures.delivery.class };

    Class<?>[] EXPANDEDPROCS = { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class,
                                 org.voltdb.benchmark.tpcc.procedures.delivery.class,
                                 org.voltdb.benchmark.tpcc.procedures.slev.class };

    Class<?>[] FEWERPROCS =    { org.voltdb.benchmark.tpcc.procedures.InsertNewOrder.class };

    Class<?>[] CONFLICTPROCS = { org.voltdb.catalog.InsertNewOrder.class,
                                 org.voltdb.benchmark.tpcc.procedures.delivery.class };

    protected String compile(String name, Class<?>... procList) {
        return  compileWithGroups(false, null, null, null, name, procList);
    }

    protected String compileWithGroups(
            boolean securityEnabled, String securityProvider,
            RoleInfo[] gi, UserInfo[] ui,
            String name, Class<?>... procList) {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(procList);
        builder.setSecurityEnabled(securityEnabled, true);

        if (gi != null && gi.length > 0)
            builder.addRoles(gi);
        if (ui != null && ui.length > 0)
            builder.addUsers(ui);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String retval = testDir + File.separator + "tpcc-catalogcheck-" + name + ".jar";
        builder.compile(retval);
        return retval;
    }

    protected Catalog catalogForJar(String pathToJar) throws IOException {
        byte[] bytes = MiscUtils.fileToBytes(new File(pathToJar));
        String serializedCatalog = CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes).getFirst());
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);
        return c;
    }

    private String verifyDiff(
            Catalog catOriginal,
            Catalog catUpdated)
    {
        return verifyDiff(catOriginal, catUpdated, null, null);
    }

    private String verifyDiff(
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

    private void verifyDiffRejected(
            Catalog catOriginal,
            Catalog catUpdated)
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

    private void verifyDiffIfEmptyTable(
            Catalog catOriginal,
            Catalog catUpdated)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertTrue(diff.supported());
        assertTrue(diff.tablesThatMustBeEmpty().length > 0);
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());
    }


    public void testAddProcedure() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("expanded", EXPANDEDPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure slev added."));
    }

    public void testModifyProcedureCode() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("conflict", CONFLICTPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure InsertNewOrder has been modified."));
    }

    public void testDeleteProcedure() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("fewer", FEWERPROCS);
        Catalog catUpdated = catalogForJar(updated);

        String report = verifyDiff(catOriginal, catUpdated);
        assertTrue(report.contains("Procedure delivery dropped."));
    }

    public void testAddGroup() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        RoleInfo gi[] = new RoleInfo[1];
        gi[0] = new RoleInfo("group1", true, true, true, true, true, true);
        String updated = compileWithGroups(false, null, gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddGroupAndUser() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        RoleInfo gi[] = new RoleInfo[1];
        gi[0] = new RoleInfo("group1", true, true, true, true, true, false);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String updated = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyUser() throws IOException {
        RoleInfo gi[] = new RoleInfo[1];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, false);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        RoleInfo gi2[] = new RoleInfo[1];
        gi2[0] = new RoleInfo("group2", true, true, true, true, true, true);
        // change a user.
        ui[0] = new UserInfo("user1", "drowssap", new String[] {"group2"});
        String updated = compileWithGroups(false, null, gi2, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteUser() throws IOException {
        RoleInfo gi[] = new RoleInfo[1];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, false);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no users this time
        String updated = compileWithGroups(false, null, gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteGroupAndUser() throws IOException {
        RoleInfo gi[] = new RoleInfo[1];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, false);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no groups or users this time
        String updated = compileWithGroups(false, null, null, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeUsersAssignedGroups() throws IOException {
        RoleInfo gi[] = new RoleInfo[2];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, false);
        gi[1] = new RoleInfo("group2", true, true, true, true, false, true);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group2"});

        String original = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // swap the user's group assignments
        ui[0] = new UserInfo("user1", "password", new String[] {"group2"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group1"});
        String updated = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeSecurityEnabled() throws IOException {
        RoleInfo gi[] = new RoleInfo[2];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, true);
        gi[1] = new RoleInfo("group2", true, true, true, true, false, false);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group2"});

        String original = compileWithGroups(false, null, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // just turn on security
        String updated = compileWithGroups(true, "hash", gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff (catOriginal, catUpdated);
    }

    public void testChangeSecurityProvider() throws IOException {
        RoleInfo gi[] = new RoleInfo[2];
        gi[0] = new RoleInfo("group1", true, true, true, true, false, false);
        gi[1] = new RoleInfo("group2", true, true, true, true, false, false);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group2"});

        String original = compileWithGroups(true, "hash", gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // just turn on security
        String updated = compileWithGroups(true, "kerberos", gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff (catOriginal, catUpdated);
    }

    public void testAdminStartupChange() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "adminstartup1.jar",
                1, 1, 0, 1000, true);
        Catalog catOriginal = catalogForJar(testDir + File.separator + "adminstartup1.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "adminstartup2.jar",
                1, 1, 0, 1000, false); // setting adminstartup to false is the test
        Catalog catUpdated = catalogForJar(testDir + File.separator + "adminstartup2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDiffOfIdenticalCatalogs() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);

        builder.compile(testDir + File.separator + "identical3.jar");
        Catalog c3 = catalogForJar(testDir + File.separator + "identical3.jar");
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "identical4.jar");
        Catalog c4 = catalogForJar(testDir + File.separator + "identical4.jar");

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
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE " + tableName + " (C1 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo(tableName, "C1");

        if (tableName.equals("A"))
            builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        else
            builder.addProcedures(org.voltdb.catalog.ProcedureB.class);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        builder.compile(testDir + File.separator + "test-" + catname + ".jar");
        Catalog cat = catalogForJar(testDir + File.separator + "test-" + catname + ".jar");
        return cat;
    }

    Catalog getCatalogForTable(String tableName, String catname, VoltTable t) throws IOException {
        return getCatalogForTable(tableName, catname, t, false);
    }

    Catalog getExportCatalogForTable(String tableName, String catname, VoltTable t) throws IOException {
        return getCatalogForTable(tableName, catname, t, true);
    }

    private Catalog getCatalogForTable(String tableName, String catname, VoltTable t, boolean export) throws IOException {
        CatalogBuilder builder = new CatalogBuilder();
        builder.addLiteralSchema(TableHelper.ddlForTable(t));
        if (export) {
            builder.addLiteralSchema("EXPORT TABLE " + TableHelper.getTableName(t) + ";");
        }

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        builder.compile(testDir + File.separator + "test-" + catname + ".jar");
        Catalog cat = catalogForJar(testDir + File.separator + "test-" + catname + ".jar");
        return cat;
    }


    // N.B. Some of the testcases assume this exact table structure .. if you change it,
    // check the callers...
    Catalog get2ColumnCatalogForTable(String tableName, String catname) throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE " + tableName + " (C1 BIGINT NOT NULL, C2 BIGINT DEFAULT 0 NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo(tableName, "C1");
        if (tableName.equals("A"))
            builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        else
            builder.addProcedures(org.voltdb.catalog.ProcedureB.class);
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        builder.compile(testDir + File.separator + "test-" + catname + ".jar");
        Catalog cat = catalogForJar(testDir + File.separator + "test-" + catname + ".jar");
        return cat;
    }


    public void testAddTable() throws IOException {
        // Start with table A.
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE A (C1 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        builder.compile(testDir + File.separator + "testaddtable1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testaddtable1.jar");

        // Add table B and recompile
        builder.addLiteralSchema("CREATE TABLE B (C1 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("B", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureB.class);
        builder.compile(testDir + File.separator + "testaddtable2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testaddtable2.jar");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testDropTable() throws IOException {
        // Start with table A and B.
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, PRIMARY KEY(C1));" +
                                 "\nCREATE TABLE B (C1 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addPartitionInfo("B", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class,
                              org.voltdb.catalog.ProcedureB.class);
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        builder.compile(testDir + File.separator  + "testdroptable1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testdroptable1.jar");

        // Create a catalog with just table A
        Catalog catUpdated = getCatalogForTable("A", "droptable2");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testViewConversion() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // Start with table A.
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        boolean success = builder.compile(testDir + File.separator + "convertmatview1.jar");
        assertTrue(success);
        Catalog catOriginal = catalogForJar(testDir + File.separator + "convertmatview1.jar");

        // Add table B and recompile
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE MATVIEW(C1 BIGINT NOT NULL, NUM INTEGER);");
        builder.addPartitionInfo("A", "C1");
        success = builder.compile(testDir + File.separator + "convertmatview1.jar");
        assertTrue(success);
        Catalog catUpdated = catalogForJar(testDir + File.separator + "convertmatview1.jar");

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
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        Catalog catOriginal, catUpdated;
        VoltProjectBuilder builder;
        String report;

         // start with a table
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(5 BYTES) ) ;");
        builder.compile(testDir + File.separator + "testVarchar0.jar");
        catOriginal = catalogForJar(testDir + File.separator + "testVarchar0.jar");

        // change from character to bytes
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(20 BYTES), v2 varchar(5 BYTES) );");
        builder.compile(testDir + File.separator + "testVarchar1.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar1.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        // size not satisfied if non-empty table
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(15 BYTES), v2 varchar(5 BYTES) );");
        builder.compile(testDir + File.separator + "testVarchar2.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        // inline character to not in line bytes.
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(100 BYTES), v2 varchar(5 BYTES) );");
        builder.compile(testDir + File.separator + "testVarchar3.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar3.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));


        // bytes to character
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(5 BYTES) ) ;");
        builder.compile(testDir + File.separator + "testVarchar0.jar");
        catOriginal = catalogForJar(testDir + File.separator + "testVarchar0.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(5) );");
        builder.compile(testDir + File.separator + "testVarchar4.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar4.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(15) );");
        builder.compile(testDir + File.separator + "testVarchar5.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar5.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(150) );");
        builder.compile(testDir + File.separator + "testVarchar6.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar6.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(3) );");
        builder.compile(testDir + File.separator + "testVarchar6.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar6.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testAddNonNullityRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT         , PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddNonNullity1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddNonNullity1.jar");

        // add a non-null constraint
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddNonNullity2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddNonNullity2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testDropNonNullity() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testDropNonNullity1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testDropNonNullity1.jar");

        // add a non-null constraint
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT         , PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testDropNonNullity2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testDropNonNullity2.jar");

        String report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));
    }

    public void testAddUniqueCoveringTableIndex() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueCoveringTableIndex1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueCoveringTableIndex1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C1,C2);");
        builder.compile(testDir + File.separator + "testAddUniqueCoveringTableIndex2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddUniqueCoveringTableIndex2.jar");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testAddUniqueNonCoveringTableIndexRejectedIfNotEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE ASSUMEUNIQUE INDEX IDX ON A(C2);");
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testShrinkUniqueNonCoveringTableIndexRejectedIfNonEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1, C2));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // shrink the pkey index
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testExpandUniqueNonCoveringTableIndex() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // shrink the pkey index
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1, C2));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddNonUniqueTableIndex() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddNonUniqueTableIndex1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddNonUniqueTableIndex1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE INDEX IDX ON A(C1,C2);");
        builder.compile(testDir + File.separator + "testAddNonUniqueTableIndex2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddNonUniqueTableIndex2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void renameUniqueIndexes() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C1,C2);");
        builder.addLiteralSchema("\nCREATE INDEX IDX2 ON A(C2);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "renameUniqueIndexes1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "renameUniqueIndexes1.jar");

        // rename an index
        VoltProjectBuilder builder2 = new VoltProjectBuilder();
        builder2.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder2.addLiteralSchema("\nCREATE UNIQUE INDEX RYANLIKETHEYANKEES ON A(C1,C2);");
        builder2.addLiteralSchema("\nCREATE INDEX GAGNAMSTYLE ON A(C2);");
        builder2.addPartitionInfo("A", "C1");
        builder2.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder2.compile(testDir + File.separator + "renameUniqueIndexes2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "renameUniqueIndexes2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveUniqueIndex() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table with an index
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C1,C2);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testRemoveUniqueIndex1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "testRemoveUniqueIndex1.jar");

        // remove the index
        Catalog catUpdated = get2ColumnCatalogForTable("A", "testRemoveUniqueIndex2");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveNonUniqueIndex() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table with an index
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addLiteralSchema("\nCREATE INDEX IDX ON A(C1,C2);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testRemoveNonUniqueIndex1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "testRemoveNonUniqueIndex1.jar");

        // remove the index
        Catalog catUpdated = get2ColumnCatalogForTable("A", "testRemoveNonUniqueIndex2");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddTableConstraintRejectedIfNotEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table without a PKEY
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddTableConstraintRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddTableConstraintRejected1.jar");

        // add a constraint (this function creates a primary key)
        Catalog catUpdated = getCatalogForTable("A", "testAddTableConstraintRejected2");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testRemoveTableConstraint() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with the primary key
        Catalog catOriginal = getCatalogForTable("A", "dropconstraint1");

        // without the primary key
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT DEFAULT 0 NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "dropconstraint2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dropconstraint2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "addmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "addmatview1.jar");

        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.compile(testDir + File.separator + "addmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addmatview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "remmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "remmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "remmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "remmatview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewColumnRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C, NUM) AS " +
                                 "\n    SELECT C3, COUNT(*) FROM A GROUP BY C3;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modmatview1.jar");

        // with a slightly different view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C, NUM) AS " +
                                 "\n    SELECT C2, COUNT(*) FROM A GROUP BY C2;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewStructureRejectedIfEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modmatview1.jar");

        // with a quite different view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C2, C1, NUM) AS " +
                                 "\n    SELECT C2, C1, COUNT(*) FROM A GROUP BY C2, C1;");

        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modmatview2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewAddPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "addpredmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "addpredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "addpredmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addpredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewDropPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "droppredmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "droppredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "droppredmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "droppredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 < 0 GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modpredmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modpredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "modpredmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modpredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewSourceRejectedIfEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "resrcmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "resrcmatview1.jar");

        // without an added column (should work with empty table)
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "resrcmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "resrcmatview2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testRemoveTableAndMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "remtablematview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "remtablematview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("B", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureB.class);
        builder.compile(testDir +  File.separator + "remtablematview2.jar");
        Catalog catUpdated = catalogForJar(testDir +  File.separator + "remtablematview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSetting() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "addpart1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "addpart1.jar");

        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "addpart2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addpart2.jar");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSettingOfExportTable() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nEXPORT TABLE A;");
        builder.addStmtProcedure("the_requisite_procedure", "insert into A values (?, ?);");
        builder.compile(testDir + File.separator + "elastic1a.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1a.jar");

        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "elastic2a.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2a.jar");
        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testChangeElasticSettingsCompatibleWithElastic() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "elastic1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        builder.setElasticDuration(100);
        builder.setElasticThroughput(50);
        builder.compile(testDir + File.separator + "elastic2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, true);
    }

    public void testChangeElasticSettingsNotCompatibleWithElasticAddProcedure() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "elastic1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        builder.setElasticDuration(100);
        builder.setElasticThroughput(50);
        builder.addStmtProcedure("another_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "elastic2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, false);
    }

    public void testChangeElasticSettingsNotCompatibleWithElasticAddTable() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "elastic1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        builder.setElasticDuration(100);
        builder.setElasticThroughput(50);
        builder.addStmtProcedure("another_procedure", "select * from A;");
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.compile(testDir + File.separator + "elastic2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, false);
    }
}
