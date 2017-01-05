/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.io.FileInputStream;
import java.io.IOException;

import org.voltdb.TableHelper;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestCatalogDiffs extends TestCase {
    static final String m_dir = "/tmp" + File.separator + System.getProperty("user.name");

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
        assertTrue("Failed to compile schema", builder.compile(retval));
        return retval;
    }

    public static Catalog catalogForJar(String pathToJar) throws IOException {
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
        assertEquals(0, diff.tablesThatMustBeEmpty()[0].length);
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
        assertTrue(diff.tablesThatMustBeEmpty()[0].length > 0);
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "adminstartup1.jar",
                1, 1, 0, 1000, true, 0));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "adminstartup1.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "adminstartup2.jar",
                1, 1, 0, 1000, false, 0)); // setting adminstartup to false is the test
        Catalog catUpdated = catalogForJar(testDir + File.separator + "adminstartup2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDiffOfIdenticalCatalogs() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "identical3.jar"));
        Catalog c3 = catalogForJar(testDir + File.separator + "identical3.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "identical4.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "test-" + catname + ".jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "test-" + catname + ".jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "test-" + catname + ".jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testaddtable1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testaddtable1.jar");

        // Add table B and recompile
        builder.addLiteralSchema("CREATE TABLE B (C1 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("B", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureB.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testaddtable2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator  + "testdroptable1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testdroptable1.jar");

        // Create a catalog with just table A
        Catalog catUpdated = getCatalogForTable("A", "droptable2");

        verifyDiff(catOriginal, catUpdated, false, null);
    }

    public void testViewConversion() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // Start with table A, B and a single table view.
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "convertmatview1.jar"));
        Catalog catOriginalSingle = catalogForJar(testDir + File.separator + "convertmatview1.jar");

        // table A, B and a join query view.
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "convertmatview2.jar"));
        Catalog catOriginalJoin = catalogForJar(testDir + File.separator + "convertmatview2.jar");

        // table A, B and a normal table instead of a view.
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE MATVIEW(C1 BIGINT NOT NULL, NUM INTEGER);");
        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "convertmatview3.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "convertmatview3.jar");

        verifyDiffRejected(catOriginalSingle, catUpdated);
        verifyDiffRejected(catOriginalJoin, catUpdated);
    }

    public void testAddTableColumn() throws IOException {
        Catalog catOriginal = getCatalogForTable("A", "addtablecolumnrejected1");
        Catalog catUpdated = get2ColumnCatalogForTable("A", "addtablecolumnrejected2");
        verifyDiff(catOriginal, catUpdated, true, null);

        VoltTable t1 = TableHelper.quickTable("(INTEGER, VARCHAR40)");
        VoltTable t2 = TableHelper.quickTable("(INTEGER, VARCHAR40, VARCHAR120)");
        catOriginal = getExportCatalogForTable("A", "addtablecolumn1", t1);
        catUpdated = getExportCatalogForTable("A", "addtablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testRemoveTableColumn() throws IOException {
        Catalog catOriginal = get2ColumnCatalogForTable("A", "removetablecolumn2");
        Catalog catUpdated = getCatalogForTable("A", "removetablecolumn1");
        verifyDiff(catOriginal, catUpdated, true, null);

        VoltTable t1 = TableHelper.quickTable("(INTEGER, VARCHAR40, VARCHAR120)");
        VoltTable t2 = TableHelper.quickTable("(INTEGER, VARCHAR40)");
        catOriginal = getExportCatalogForTable("A", "droptablecolumn1", t1);
        catUpdated = getExportCatalogForTable("A", "droptablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar0.jar"));
        catOriginal = catalogForJar(testDir + File.separator + "testVarchar0.jar");

        // change from character to bytes
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(20 BYTES), v2 varchar(5 BYTES) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar1.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar1.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        // size not satisfied if non-empty table
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(15 BYTES), v2 varchar(5 BYTES) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar2.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        // inline character to not in line bytes.
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(100 BYTES), v2 varchar(5 BYTES) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar3.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar3.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));


        // bytes to character
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(5 BYTES) ) ;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar0.jar"));
        catOriginal = catalogForJar(testDir + File.separator + "testVarchar0.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(5) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar4.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar4.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(15) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar5.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar5.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(150) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar6.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "testVarchar6.jar");
        report = verifyDiff(catOriginal, catUpdated);
        assert(report.contains("Table A has been modified."));

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT, v1 varchar(5), v2 varchar(3) );");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testVarchar6.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddNonNullity1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddNonNullity1.jar");

        // add a non-null constraint
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddNonNullity2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testDropNonNullity1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testDropNonNullity1.jar");

        // add a non-null constraint
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT         , PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testDropNonNullity2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueCoveringTableIndex1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueCoveringTableIndex1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C1,C2);");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueCoveringTableIndex2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE ASSUMEUNIQUE INDEX IDX ON A(C2);");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // shrink the pkey index
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // shrink the pkey index
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1, C2));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddNonUniqueTableIndex1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddNonUniqueTableIndex1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE INDEX IDX ON A(C1,C2);");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddNonUniqueTableIndex2.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "renameUniqueIndexes1.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testRemoveUniqueIndex1.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testRemoveNonUniqueIndex1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "testRemoveNonUniqueIndex1.jar");

        // remove the index
        Catalog catUpdated = get2ColumnCatalogForTable("A", "testRemoveNonUniqueIndex2");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testExportRejectedIfNotEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "testAddTableConstraintRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddTableConstraintRejected1.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addLiteralSchema("\nEXPORT TABLE A;");
        builder.compile(testDir + File.separator + "testAddTableConstraintRejected2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddTableConstraintRejected2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addLiteralSchema("\nEXPORT TABLE A;");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("B", "C1");
        builder.compile(testDir + File.separator + "testAddTableConstraintRejected1.jar");
        catOriginal = catalogForJar(testDir + File.separator + "testAddTableConstraintRejected1.jar");

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addLiteralSchema("\nEXPORT TABLE A;");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("B", "C1");
        builder.addLiteralSchema("\nEXPORT TABLE B;");
        builder.compile(testDir + File.separator + "testAddTableConstraintRejected2.jar");
        catUpdated = catalogForJar(testDir + File.separator + "testAddTableConstraintRejected2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testAddTableConstraintRejectedIfNotEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table without a PKEY
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "testAddTableConstraintRejected1.jar"));
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
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dropconstraint2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dropconstraint2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "addmatview1.jar");

        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addmatview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "remmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "remmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "remmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "remmatview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewColumnRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C, NUM) AS " +
                                 "\n    SELECT C3, COUNT(*) FROM A GROUP BY C3;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modmatview1.jar");

        // with a slightly different view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C, NUM) AS " +
                                 "\n    SELECT C2, COUNT(*) FROM A GROUP BY C2;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C2, COUNT(*) FROM A JOIN B ON A.C1=B.C1 GROUP BY A.C2;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewStructureRejectedIfEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modmatview1.jar");

        // with a quite different view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C2, C1, NUM) AS " +
                                 "\n    SELECT C2, C1, COUNT(*) FROM A GROUP BY C2, C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C2, COUNT(*) FROM A JOIN B ON A.C1=B.C1 GROUP BY A.C2;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modmatview2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewAddPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addpredmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "addpredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 WHERE A.C1 > 0 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addpredmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addpredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewDropPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 WHERE A.C1 > 0 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "droppredmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "droppredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "droppredmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "droppredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewPredicateRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 < 0 GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 WHERE A.C1 > 0 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modpredmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "modpredmatview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A WHERE C1 > 0 GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 WHERE A.C2 <= 0 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "modpredmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "modpredmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testModifyMaterializedViewSourceRejectedIfEmpty() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "resrcmatview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "resrcmatview1.jar");

        // without an added column (should work with empty table)
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, C3 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "resrcmatview2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "resrcmatview2.jar");

        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testRemoveTableAndMaterializedView() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE TABLE B (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS " +
                                 "\n    SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEWJOIN(C1, NUM) AS " +
                                 "\n    SELECT A.C1, COUNT(*) FROM A JOIN B ON A.C1=B.C2 GROUP BY A.C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "remtablematview1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "remtablematview1.jar");

        // without a view
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE C (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("C", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureC.class);
        assertTrue("Failed to compile schema", builder.compile(testDir +  File.separator + "remtablematview2.jar"));
        Catalog catUpdated = catalogForJar(testDir +  File.separator + "remtablematview2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSetting() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addpart1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "addpart1.jar");

        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "addpart2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addpart2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testChangeTableReplicationSettingOfExportTable() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nEXPORT TABLE A;");
        builder.addStmtProcedure("the_requisite_procedure", "insert into A values (?, ?);");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic1a.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1a.jar");

        builder.addPartitionInfo("A", "C1");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic2a.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2a.jar");
        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testChangeCompatibleWithElasticNoChange() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, true);
    }

    public void testChangeNotCompatibleWithElasticAddProcedure() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        builder.addStmtProcedure("another_procedure", "select * from A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, false);
    }

    public void testChangeNotCompatibleWithElasticAddTable() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "elastic1.jar");

        builder.addLiteralSchema("\nCREATE TABLE another_table (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "elastic2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "elastic2.jar");
        verifyDiff(catOriginal, catUpdated, null, false);
    }

    public void testEnableDROnEmptyTable() throws IOException {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                                 "\nPARTITION TABLE A ON COLUMN C1;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "dr1.jar");

        builder.addLiteralSchema("\nDR TABLE A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testSetDRActiveActiveOnEmptyTable() throws IOException {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                                 "\nPARTITION TABLE A ON COLUMN C1;" +
                                 "\nDR TABLE A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "dr1.jar");

        builder.addLiteralSchema("\nSET " + DatabaseConfiguration.DR_MODE_NAME +
                                 "=" + DatabaseConfiguration.ACTIVE_ACTIVE + ";");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testDisableDROnTable() throws IOException {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                                 "\nPARTITION TABLE A ON COLUMN C1;" +
                                 "\nDR TABLE A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "dr1.jar");

        builder.addLiteralSchema("\nDR TABLE A DISABLE;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");
        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddDRTableColumn() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                                 "\nPARTITION TABLE A ON COLUMN C1;" +
                                 "\nDR TABLE A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "dr1.jar");

        builder.addLiteralSchema("\nALTER TABLE A ADD COLUMN C3 INTEGER;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testRemoveDRTableColumn() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);" +
                                 "\nPARTITION TABLE A ON COLUMN C1;" +
                                 "\nDR TABLE A;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir + File.separator + "dr1.jar");

        builder.addLiteralSchema("\nALTER TABLE A DROP COLUMN C2;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyDRTableColumn() throws IOException {
        String originalSchema = "\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 INTEGER NOT NULL);" +
                                "\nPARTITION TABLE A ON COLUMN C1;" +
                                "\nDR TABLE A;";
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(originalSchema);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr1.jar"));
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "dr1.jar");

        builder.addLiteralSchema("\nALTER TABLE A ALTER COLUMN C2 BIGINT;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr2.jar"));
        Catalog catUpdated = catalogForJar(testDir + File.separator + "dr2.jar");

        // Does not require empty table as C2 is made wider
        verifyDiff(catOriginal, catUpdated);

        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(originalSchema);
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr3.jar"));
        catOriginal = catalogForJar(testDir +  File.separator + "dr3.jar");

        builder.addLiteralSchema("\nALTER TABLE A ALTER COLUMN C2 TINYINT;");
        assertTrue("Failed to compile schema", builder.compile(testDir + File.separator + "dr4.jar"));
        catUpdated = catalogForJar(testDir + File.separator + "dr4.jar");

        // Requires empty table as C2 is made narrower
        verifyDiffIfEmptyTable(catOriginal, catUpdated);
    }

    public void testExportConfigStreamTargetAttribute() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final String ddl =
                "CREATE STREAM export_data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);

        String depXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='default' enabled='true' type='file'>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">pre-fix</property>"
                + "            <property name=\"outdir\">"+m_dir+"</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "exporttarget1.jar");
        Catalog cat = catalogForJar(testDir + File.separator + "exporttarget1.jar");
        File file = VoltProjectBuilder.writeStringToTempFile(depXml);
        DeploymentType deployment = CatalogUtil.getDeployment(new FileInputStream(file));

        String msg = CatalogUtil.compileDeployment(cat, deployment, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        depXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration stream='default' target='newtarget' enabled='true' type='file'>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">pre-fix</property>"
                + "            <property name=\"outdir\">"+m_dir+"</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "exporttarget2.jar");
        cat = catalogForJar(testDir + File.separator + "exporttarget2.jar");
        file = VoltProjectBuilder.writeStringToTempFile(depXml);
        deployment = CatalogUtil.getDeployment(new FileInputStream(file));

        msg = CatalogUtil.compileDeployment(cat, deployment, false);
        assertTrue("Must fail when both 'stream' and 'target' attributes are specified",
                msg.contains("Only one of 'target' or 'stream' attribute must be specified"));
    }

    public void testConnectorPropertiesChanges() throws Exception {
        if (!MiscUtils.isPro()) { return; } // not supported in community

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        final String ddl =
                "CREATE TABLE export_data ( id BIGINT default 0 , value BIGINT DEFAULT 0 );\n"
              + "EXPORT TABLE export_data;";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);

        final String origXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration target='default' enabled='true' type='file'>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">pre-fix</property>"
                + "            <property name=\"outdir\">"+m_dir+"</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "propexport1.jar");
        Catalog origCat = catalogForJar(testDir + File.separator + "propexport1.jar");
        final File origFile = VoltProjectBuilder.writeStringToTempFile(origXml);
        DeploymentType origDepl = CatalogUtil.getDeployment(new FileInputStream(origFile));

        String msg = CatalogUtil.compileDeployment(origCat, origDepl, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        final String newPropXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration target='default' enabled='true' type='file'>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "            <property name=\"nonce\">pre-fix</property>"
                + "            <property name=\"outdir\">"+m_dir+"</property>"
                + "            <property name=\"iamnew\">see_me_roar</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "propexport2.jar");
        Catalog newPropCat = catalogForJar(testDir + File.separator + "propexport2.jar");
        final File newPropFile = VoltProjectBuilder.writeStringToTempFile(newPropXml);
        DeploymentType newPropDepl = CatalogUtil.getDeployment(new FileInputStream(newPropFile));

        msg = CatalogUtil.compileDeployment(newPropCat, newPropDepl, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        final String modPropXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration target='default' enabled='true' type='file'>"
                + "            <property name=\"type\">TSV</property>"
                + "            <property name=\"with-schema\">true</property>"
                + "            <property name=\"nonce\">pre-fix-other</property>"
                + "            <property name=\"outdir\">"+m_dir+"</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "propexport3.jar");
        Catalog modPropCat = catalogForJar(testDir + File.separator + "propexport3.jar");
        final File modPropFile = VoltProjectBuilder.writeStringToTempFile(modPropXml);
        DeploymentType modPropDepl = CatalogUtil.getDeployment(new FileInputStream(modPropFile));

        msg = CatalogUtil.compileDeployment(modPropCat, modPropDepl, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        final String modTypeXml =
                "<?xml version='1.0' encoding='UTF-8' standalone='no'?>"
                + "<deployment>"
                + "<cluster hostcount='3' kfactor='1' sitesperhost='2'/>"
                + "    <export>"
                + "        <configuration target='default' enabled='false' type='custom' exportconnectorclass=\"org.voltdb.exportclient.NoOpTestExportClient\" >"
                + "            <property name=\"foo\">false</property>"
                + "            <property name=\"type\">CSV</property>"
                + "            <property name=\"with-schema\">false</property>"
                + "        </configuration>"
                + "    </export>"
                + "</deployment>";

        builder.compile(testDir + File.separator + "propexport4.jar");
        Catalog modTypeCat = catalogForJar(testDir + File.separator + "propexport4.jar");
        final File modTypeFile = VoltProjectBuilder.writeStringToTempFile(modTypeXml);
        DeploymentType modTypeDepl = CatalogUtil.getDeployment(new FileInputStream(modTypeFile));

        msg = CatalogUtil.compileDeployment(modTypeCat, modTypeDepl, false);
        assertTrue("Deployment file failed to parse: " + msg, msg == null);

        verifyDiff(origCat, newPropCat); // test add
        verifyDiff(newPropCat, origCat); // test delete
        verifyDiff(origCat, modPropCat); // test modification
        verifyDiff(modPropCat, modTypeCat); // test modification
    }
}
