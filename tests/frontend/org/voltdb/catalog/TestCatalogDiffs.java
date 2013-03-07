/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;

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
        return  compileWithGroups(false, null, null, name, procList);
    }

    protected String compileWithGroups(boolean securityEnabled, GroupInfo[] gi, UserInfo[] ui, String name, Class<?>... procList) {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(procList);
        builder.setSecurityEnabled(securityEnabled);

        if (gi != null && gi.length > 0)
            builder.addGroups(gi);
        if (ui != null && ui.length > 0)
            builder.addUsers(ui);

        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String retval = testDir + File.separator + "tpcc-catalogcheck-" + name + ".jar";
        builder.compile(retval);
        return retval;
    }

    protected Catalog catalogForJar(String pathToJar) throws IOException {
        byte[] bytes = CatalogUtil.toBytes(new File(pathToJar));
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);
        return c;
    }

    private void verifyDiff(
            Catalog catOriginal,
            Catalog catUpdated)
    {
        verifyDiff(catOriginal, catUpdated, null);
    }

    private void verifyDiff(
            Catalog catOriginal,
            Catalog catUpdated,
            Boolean expectSnapshotIsolation)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        if (expectSnapshotIsolation != null) {
            assertEquals((boolean) expectSnapshotIsolation, diff.requiresSnapshotIsolation());
        }
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());

        System.out.println("========================");
        System.out.println(diff.getDescriptionOfChanges());
        System.out.println("========================");
    }

    private void verifyDiffRejected(
            Catalog catOriginal,
            Catalog catUpdated)
    {
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertFalse(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, catUpdated.serialize());
    }


    public void testAddProcedure() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("expanded", EXPANDEDPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyProcedureCode() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("conflict", CONFLICTPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteProcedure() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("fewer", FEWERPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddGroup() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true, true);
        String updated = compileWithGroups(false, gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testAddGroupAndUser() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String updated = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testModifyUser() throws IOException {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // change a user.
        ui[0] = new UserInfo("user1", "drowssap", new String[] {"group1"});
        String updated = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteUser() throws IOException {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no users this time
        String updated = compileWithGroups(false, gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testDeleteGroupAndUser() throws IOException {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});

        String original = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no groups or users this time
        String updated = compileWithGroups(false, null, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeUsersAssignedGroups() throws IOException {
        GroupInfo gi[] = new GroupInfo[2];
        gi[0] = new GroupInfo("group1", true, true, true);
        gi[1] = new GroupInfo("group2", true, true, true);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group2"});

        String original = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // swap the user's group assignments
        ui[0] = new UserInfo("user1", "password", new String[] {"group2"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group1"});
        String updated = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff(catOriginal, catUpdated);
    }

    public void testChangeSecurityEnabled() throws IOException {
        GroupInfo gi[] = new GroupInfo[2];
        gi[0] = new GroupInfo("group1", true, true, true);
        gi[1] = new GroupInfo("group2", true, true, true);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", "password", new String[] {"group2"});

        String original = compileWithGroups(false, gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // just turn on security
        String updated = compileWithGroups(true, gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);

        verifyDiff (catOriginal, catUpdated);
    }

    public void testUnallowedChange() throws IOException {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // compile an invalid change (add a unique index, in this case)
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addLiteralSchema("CREATE UNIQUE INDEX IDX_CUSTOMER_NAME2 ON CUSTOMER_NAME (C_W_ID,C_D_ID,C_LAST);");
        builder.addProcedures(BASEPROCS);
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String updated = testDir + File.separator + "tpcc-catalogcheck-invalid.jar";
        builder.compile(updated);
        Catalog catUpdated = catalogForJar(updated);

        // and verify the allowed flag
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        assertFalse(diff.supported());
    }

    public void testDiffOfIdenticalCatalogs() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);

        builder.compile(testDir + File.separator + "identical3.jar");
        Catalog c3 = catalogForJar(testDir + File.separator + "identical3.jar");
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
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
        CatalogBuilder builder = new CatalogBuilder();
        builder.addLiteralSchema(TableHelper.ddlForTable(t));

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

        verifyDiff(catOriginal, catUpdated, false);
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

        verifyDiff(catOriginal, catUpdated, false);
    }


    public void testAddTableColumn() throws IOException {
        Catalog catOriginal = getCatalogForTable("A", "addtablecolumnrejected1");
        Catalog catUpdated = get2ColumnCatalogForTable("A", "addtablecolumnrejected2");
        verifyDiff(catOriginal, catUpdated, true);
    }

    public void testRemoveTableColumn() throws IOException {
        Catalog catOriginal = get2ColumnCatalogForTable("A", "removetablecolumn2");
        Catalog catUpdated = getCatalogForTable("A", "removetablecolumn1");
        verifyDiff(catOriginal, catUpdated, true);
    }

    public void testModifyTableColumn() throws IOException {
        // should pass
        VoltTable t1 = TableHelper.quickTable("(SMALLINT, VARCHAR30, VARCHAR80)");
        VoltTable t2 = TableHelper.quickTable("(INTEGER, VARCHAR40, VARCHAR120)");
        Catalog catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        Catalog catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiff(catOriginal, catUpdated, true);

        // fail integer contraction
        t1 = TableHelper.quickTable("(BIGINT)");
        t2 = TableHelper.quickTable("(INTEGER)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);

        // fail string contraction
        t1 = TableHelper.quickTable("(VARCHAR35)");
        t2 = TableHelper.quickTable("(VARCHAR34)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);

        // fail crossing inline - out-of-line boundary
        t1 = TableHelper.quickTable("(VARBINARY30)");
        t2 = TableHelper.quickTable("(VARBINARY70)");
        catOriginal = getCatalogForTable("A", "modtablecolumn1", t1);
        catUpdated = getCatalogForTable("A", "modtablecolumn2", t2);
        verifyDiffRejected(catOriginal, catUpdated);
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

        verifyDiff(catOriginal, catUpdated, false);
    }

    public void testAddUniqueNonCoveringTableIndexRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // start with a table
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL, PRIMARY KEY(C1));");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected1.jar");

        // add an index
        builder.addLiteralSchema("\nCREATE UNIQUE INDEX IDX ON A(C2);");
        builder.compile(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "testAddUniqueNonCoveringTableIndexRejected2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
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

    public void testAddTableConstraintRejected() throws IOException {
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
        verifyDiffRejected(catOriginal, catUpdated);
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

    public void testAddMaterializedViewRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile(testDir + File.separator + "addmatview1.jar");
        Catalog catOriginal = catalogForJar(testDir + File.separator + "addmatview1.jar");

        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.compile(testDir + File.separator + "addmatview2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addmatview2.jar");

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testRemoveMaterializedViewRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
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

        verifyDiffRejected(catOriginal, catUpdated);
    }

    public void testRemoveTableAndMaterializedViewAccepted() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        // with a view
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
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

    public void testChangeTableReplicationSettingRejected() throws IOException {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addStmtProcedure("the_requisite_procedure", "select * from A;");
        builder.compile(testDir + File.separator + "addpart1.jar");
        Catalog catOriginal = catalogForJar(testDir +  File.separator + "addpart1.jar");

        builder.addPartitionInfo("A", "C1");
        builder.compile(testDir + File.separator + "addpart2.jar");
        Catalog catUpdated = catalogForJar(testDir + File.separator + "addpart2.jar");
        verifyDiffRejected(catOriginal, catUpdated);
    }
}
