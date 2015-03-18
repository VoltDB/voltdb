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

package org.voltdb.planner;

import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.TestVoltCompiler;

public class TestVerbotenPlans extends TestCase {

    public void testTwoPartitionedTableJoin() throws IOException {
        final String simpleSchema =
            "CREATE TABLE OBJECT_DETAIL (" +
                "OBJECT_DETAIL_ID INTEGER NOT NULL, " +
                "NAME VARCHAR(256) NOT NULL, " +
                "DESCRIPTION VARCHAR(1024) NOT NULL, " +
                "PRIMARY KEY (OBJECT_DETAIL_ID) );\n" +
            "CREATE TABLE ASSET (" +
                "ASSET_ID INTEGER NOT NULL, " +
                "OBJECT_DETAIL_ID INTEGER NOT NULL, " +
                "PRIMARY KEY (ASSET_ID) );\n" +
            "CREATE PROCEDURE SelectEng490 AS \n" +
                "SELECT A.ASSET_ID, A.OBJECT_DETAIL_ID, OD.OBJECT_DETAIL_ID " +
                "FROM ASSET A, OBJECT_DETAIL OD WHERE A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID;\n" +
            "PARTITION TABLE ASSET ON COLUMN ASSET_ID;\n" +
            "PARTITION TABLE OBJECT_DETAIL ON COLUMN OBJECT_DETAIL_ID;\n" +
            "";

        String pathToCatalog = Configuration.getPathToCatalogForTest("testout.jar");
        TestVoltCompiler.compileLiteralSchemaForErrors(pathToCatalog, simpleSchema);
        //TODO: assert specific error messages from compileLiteralSchemaForErrors's return value.
    }

}
