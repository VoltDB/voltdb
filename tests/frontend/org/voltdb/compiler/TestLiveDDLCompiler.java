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

package org.voltdb.compiler;

import java.io.File;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestLiveDDLCompiler extends TestCase {

    // Test that multiple partition statements are accepted and that
    // the final result is the final requested partitioning
    public void testMultiplePartitionStatements() throws Exception
    {
        File jarOut = new File("partitionfun.jar");
        jarOut.deleteOnExit();

        String schema =
            "CREATE TABLE T (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, C3 INTEGER NOT NULL);\n" +
            "PARTITION TABLE T ON COLUMN C1;\n" +
            "PARTITION TABLE T ON COLUMN C2;\n";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();

        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileFromDDL(jarOut.getPath(), schemaPath);
        assertTrue("Compilation failed unexpectedly", success);

        Catalog catalog = new Catalog();
        catalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(MiscUtils.fileToBytes(new File(jarOut.getPath()))).getFirst()));

        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        Table t = db.getTables().get("T");
        assertEquals("C2", t.getPartitioncolumn().getTypeName());
    }

    public void testDropPartitionedTable() throws Exception
    {
        File jarOut = new File("partitionfun.jar");
        jarOut.deleteOnExit();

        String schema =
            "CREATE TABLE T (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, C3 INTEGER NOT NULL);\n" +
            "PARTITION TABLE T ON COLUMN C1;\n" +
            "DROP TABLE T;\n";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();

        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileFromDDL(jarOut.getPath(), schemaPath);
        assertTrue("Compilation failed unexpectedly", success);

        Catalog catalog = new Catalog();
        catalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(MiscUtils.fileToBytes(new File(jarOut.getPath()))).getFirst()));

        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        Table t = db.getTables().get("T");
        assertEquals(null, t);
    }

    public void testDropExportTable() throws Exception
    {
        File jarOut = new File("partitionfun.jar");
        jarOut.deleteOnExit();

        String schema =
            "CREATE TABLE T (C1 INTEGER NOT NULL, C2 INTEGER NOT NULL, C3 INTEGER NOT NULL);\n" +
            "EXPORT TABLE T;\n" +
            "DROP TABLE T;\n";
        File schemaFile = VoltProjectBuilder.writeStringToTempFile(schema);
        String schemaPath = schemaFile.getPath();

        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compileFromDDL(jarOut.getPath(), schemaPath);
        assertTrue("Compilation failed unexpectedly", success);

        Catalog catalog = new Catalog();
        catalog.execute(CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(MiscUtils.fileToBytes(new File(jarOut.getPath()))).getFirst()));

        Database db = catalog.getClusters().get("cluster").getDatabases().get("database");
        Table t = db.getTables().get("T");
        assertEquals(null, t);
    }
}
