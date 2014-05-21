/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

package org.voltdb;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocAlterTable extends AdhocDDLTestBase {

    public void testBasicAddColumn() throws Exception
    {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(
                "create table FOO (" +
                "ID integer not null," +
                "VAL bigint, " +
                "constraint pk_tree primary key (ID)" +
                ");\n" +
                "create procedure TestProc as select VAL from FOO;\n"
                );
        builder.addPartitionInfo("FOO", "ID");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        try {
            startSystem(config);

            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50);");
            }
            catch (ProcCallException pce) {
                fail("Alter table to add column should have succeeded");
            }
            assertTrue(verifyTableColumnType("FOO", "NEWCOL", "VARCHAR"));
            assertTrue(verifyTableColumnSize("FOO", "NEWCOL", 50));

            // second time should fail
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column NEWCOL varchar(50);");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Adding the same column twice should fail", threw);

            // can't add another primary key
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column BADPK integer primary key;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to add a second primary key", threw);

            // Can't add a not-null column with no default
            threw = false;
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column BADNOTNULL integer not null;");
            }
            catch (ProcCallException pce) {
                threw = true;
            }
            assertTrue("Shouldn't be able to add a not null column without default", threw);

            // but we're good with a default
            try {
                m_client.callProcedure("@AdHoc",
                        "alter table FOO add column GOODNOTNULL integer default 0 not null;");
            }
            catch (ProcCallException pce) {
                fail("Should be able to add a column with not null and a default.");
            }
            assertTrue(verifyTableColumnType("FOO", "GOODNOTNULL", "INTEGER"));
        }
        finally {
            teardownSystem();
        }
    }
}
