/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import org.voltdb.CatalogContext;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;

public class TestCatalogVersioning extends TestCase {

    public void testSimplest() throws IOException {
        // CREATE A CATALOG AND LOAD FROM SERIALIZATION
        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n",
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);


        // ADD A TABLE

        catalog.execute("add /clusters[cluster]/databases[database] tables FOO");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n",
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);

        Database database = catalog.getClusters().get("cluster").getDatabases().get("database");
        CatalogMap<Table> tables = database.getTables();
        CatalogMap<Procedure> procs = database.getProcedures();

        int tablesVersion = tables.getSubTreeVersion();
        int procsVersion = procs.getSubTreeVersion();
        int catalogVersion = catalog.getSubTreeVersion();

        assertTrue(tablesVersion > procsVersion);
        assertEquals(catalogVersion, tablesVersion);


        // REMOVE A PROC

        catalog.execute("delete /clusters[cluster]/databases[database] procedures InsertCustomer");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n",
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);

        tablesVersion = tables.getSubTreeVersion();
        procsVersion = procs.getSubTreeVersion();
        catalogVersion = catalog.getSubTreeVersion();

        assertTrue(tablesVersion < procsVersion);
        assertEquals(catalogVersion, procsVersion);


        // CHANGE A FIELD (or two)

        catalog.execute("set /clusters[cluster]/databases[database]/procedures[InsertWarehouse] readonly true");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n",
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);
        catalog.execute("set /clusters[cluster]/databases[database]/procedures[InsertWarehouse] partitionparameter 4");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n",
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);

        tablesVersion = tables.getSubTreeVersion();
        procsVersion = procs.getSubTreeVersion();
        int procNVersion = procs.get("InsertWarehouse").getNodeVersion();
        int procSTVersion = procs.get("InsertWarehouse").getSubTreeVersion();
        catalogVersion = catalog.getSubTreeVersion();

        assertTrue(tablesVersion < procsVersion);
        assertEquals(procNVersion, procSTVersion);
        assertEquals(procNVersion, catalogVersion);
        assertEquals(procSTVersion, database.getSubTreeVersion());
        assertTrue(procSTVersion > database.getNodeVersion());
        assertEquals(catalogVersion, procsVersion);
    }


    // verify that a deepCopy preserves version data
    public void testDeepCopyVersioning() throws IOException {
        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
        Catalog copy = catalog.deepCopy();

        assertTrue(catalog != copy);
        assertTrue(catalog.m_currentCatalogVersion == copy.m_currentCatalogVersion);
        assertTrue(catalog.getSubTreeVersion() == copy.getSubTreeVersion());
    }

    // a real catalog update happens on a deep copy.
    // make sure that preserves version numbers (ENG-634)
    public void testUpdateViaContextAPI() throws IOException {
        Catalog catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();

        String addTableFoo = "add /clusters[cluster]/databases[database] tables FOO";
        String addTableBar = "add /clusters[cluster]/databases[database] tables BAR";

        CatalogContext context = new CatalogContext(catalog, CatalogContext.NO_PATH, 0, 0, 0);

        CatalogContext foocontext = context.update(CatalogContext.NO_PATH, addTableFoo, true, -1);
        assertTrue(context != foocontext);
        assertTrue(context.catalogVersion < foocontext.catalogVersion);
        assertTrue(context.catalog.m_currentCatalogVersion < foocontext.catalog.m_currentCatalogVersion);

        // (rtb) I think it's a defect that this requires <=. Should be strictly <.
        assertTrue(context.catalog.getSubTreeVersion() <= foocontext.catalog.getSubTreeVersion());

        // and advance it all one more time to show a copy of copy is a woodchuck
        CatalogContext barcontext = foocontext.update(CatalogContext.NO_PATH, addTableBar, true, -1);
        assertTrue(foocontext != barcontext);
        assertTrue(foocontext.catalogVersion < barcontext.catalogVersion);
        assertTrue(foocontext.catalog.m_currentCatalogVersion < barcontext.catalog.m_currentCatalogVersion);
        assertTrue(foocontext.catalog.getSubTreeVersion() <= barcontext.catalog.getSubTreeVersion());
    }

}
