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

import org.voltdb.catalog.Catalog;

import junit.framework.*;

public class TestCatalogVersioning extends TestCase {

    public void testSimplest() {
        // CREATE A CATALOG AND LOAD FROM SERIALIZATION
        Catalog catalog = new Catalog();
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n", 
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);
        catalog.execute(LoadCatalogToString.THE_CATALOG);
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
        
        catalog.execute("delete /clusters[cluster]/databases[database] procedures MilestoneOneCombined");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n", 
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);
        
        tablesVersion = tables.getSubTreeVersion();
        procsVersion = procs.getSubTreeVersion();
        catalogVersion = catalog.getSubTreeVersion();
        
        assertTrue(tablesVersion < procsVersion);
        assertEquals(catalogVersion, procsVersion);
      
        
        // CHANGE A FIELD (or two)
        
        catalog.execute("set /clusters[cluster]/databases[database]/procedures[MilestoneOneInsert] readonly true");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n", 
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);
        catalog.execute("set /clusters[cluster]/databases[database]/procedures[MilestoneOneInsert] partitionparameter 4");
        System.out.printf("Current catalog version info current/node/subtree: %d/%d/%d\n", 
                catalog.m_currentCatalogVersion, catalog.m_nodeVersion, catalog.m_subTreeVersion);
        
        tablesVersion = tables.getSubTreeVersion();
        procsVersion = procs.getSubTreeVersion();
        int procNVersion = procs.get("MilestoneOneInsert").getNodeVersion();
        int procSTVersion = procs.get("MilestoneOneInsert").getSubTreeVersion();
        catalogVersion = catalog.getSubTreeVersion();
        
        assertTrue(tablesVersion < procsVersion);
        assertEquals(procNVersion, procSTVersion);
        assertEquals(procNVersion, catalogVersion);
        assertEquals(procSTVersion, database.getSubTreeVersion());
        assertTrue(procSTVersion > database.getNodeVersion());
        assertEquals(catalogVersion, procsVersion);
    }
}
