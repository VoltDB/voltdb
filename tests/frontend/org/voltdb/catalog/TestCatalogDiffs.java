/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.GroupInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
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
        return  compileWithGroups(null, null, name, procList);
    }

    protected String compileWithGroups(GroupInfo[] gi, UserInfo[] ui, String name, Class<?>... procList) {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addProcedures(procList);

        if (gi != null && gi.length > 0)
            builder.addGroups(gi);
        if (ui != null && ui.length > 0)
            builder.addUsers(ui);

        String retval = "tpcc-catalogcheck-" + name + ".jar";
        builder.compile(retval);
        return retval;
    }

    protected Catalog catalogForJar(String pathToJar) {
        String serializedCatalog = CatalogUtil.loadCatalogFromJar(pathToJar, null);
        assertNotNull(serializedCatalog);
        Catalog c = new Catalog();
        c.execute(serializedCatalog);
        return c;
    }

    public void testAddProcedure() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("expanded", EXPANDEDPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        assertTrue(diff.supported());
        catOriginal.execute(diff.commands());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testModifyProcedureCode() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("conflict", CONFLICTPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testDeleteProcedure() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        String updated = compile("fewer", FEWERPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testAddGroup() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true);
        String updated = compileWithGroups(gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testAddGroupAndUser() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group1"});

        String updated = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testModifyUser() {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group1"});

        String original = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // change a user.
        ui[0] = new UserInfo("user1", false, false, "drowssap", new String[] {"group1"});
        String updated = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testDeleteUser() {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group1"});

        String original = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no users this time
        String updated = compileWithGroups(gi, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testDeleteGroupAndUser() {
        GroupInfo gi[] = new GroupInfo[1];
        gi[0] = new GroupInfo("group1", true, true);

        UserInfo ui[] = new UserInfo[1];
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group1"});

        String original = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // no groups or users this time
        String updated = compileWithGroups(null, null, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testChangeUsersAssignedGroups() {
        GroupInfo gi[] = new GroupInfo[2];
        gi[0] = new GroupInfo("group1", true, true);
        gi[1] = new GroupInfo("group2", true, true);

        UserInfo ui[] = new UserInfo[2];
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group1"});
        ui[1] = new UserInfo("user2", true, true, "password", new String[] {"group2"});

        String original = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // swap the user's group assignments
        ui[0] = new UserInfo("user1", true, true, "password", new String[] {"group2"});
        ui[1] = new UserInfo("user2", true, true, "password", new String[] {"group1"});
        String updated = compileWithGroups(gi, ui, "base", BASEPROCS);
        Catalog catUpdated = catalogForJar(updated);
        String updatedSerialized = catUpdated.serialize();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        catOriginal.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = catOriginal.serialize();
        assertEquals(updatedOriginalSerialized, updatedSerialized);
    }

    public void testUnallowedChange() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);

        // compile an invalid change (add ELT, in this case)
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        builder.addDefaultSchema();
        builder.addDefaultPartitioning();
        builder.addDefaultELT();
        builder.addProcedures(BASEPROCS);
        String updated = "tpcc-catalogcheck-invalid.jar";
        builder.compile(updated);
        Catalog catUpdated = catalogForJar(updated);

        // and verify the allowed flag
        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, catUpdated);
        assertFalse(diff.supported());
    }

    public void testIsUpIgnored() {
        String original = compile("base", BASEPROCS);
        Catalog catOriginal = catalogForJar(original);
        catOriginal.getClusters().get("cluster").getSites().add("999");
        catOriginal.getClusters().get("cluster").getSites().get("999").set("isUp", "true");
        Catalog cat_copy = catOriginal.deepCopy();

        CatalogDiffEngine diff = new CatalogDiffEngine(catOriginal, cat_copy);
        String null_diff = diff.commands();
        assertTrue(diff.supported());
        assertEquals("", null_diff);

        cat_copy.getClusters().get("cluster").getSites().get("999").set("isUp", "false");
        diff = new CatalogDiffEngine(catOriginal, cat_copy);
        assertTrue(diff.supported());
        assertEquals("", diff.commands());
    }

    @SuppressWarnings("deprecation")
    public void testDiffOfIdenticalCatalogs() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile("identical3.jar");
        Catalog c3 = catalogForJar("identical3.jar");
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema("\nCREATE TABLE A (C1 BIGINT NOT NULL, C2 BIGINT NOT NULL);");
        builder.addLiteralSchema("\nCREATE VIEW MATVIEW(C1, NUM) AS SELECT C1, COUNT(*) FROM A GROUP BY C1;");
        builder.addPartitionInfo("A", "C1");
        builder.addProcedures(org.voltdb.catalog.ProcedureA.class);
        builder.compile("identical4.jar");
        Catalog c4 = catalogForJar("identical4.jar");

        CatalogDiffEngine diff = new CatalogDiffEngine(c3, c4);
        // don't reach this point.
        c3.execute(diff.commands());
        assertTrue(diff.supported());
        String updatedOriginalSerialized = c3.serialize();
        assertEquals(updatedOriginalSerialized, c4.serialize());
    }
}
