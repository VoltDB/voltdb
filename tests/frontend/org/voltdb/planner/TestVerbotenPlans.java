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

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.VoltProjectBuilder;

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
                "PRIMARY KEY (ASSET_ID) );";

        final File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        final String schemaPath = schemaFile.getPath();

        final String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<schemas>" +
            "<schema path='" + schemaPath + "' />" +
            "</schemas>" +
            "<procedures>" +
            "<procedure class='SelectEng490'>" +
            "<sql>SELECT A.ASSET_ID, A.OBJECT_DETAIL_ID, OD.OBJECT_DETAIL_ID " +
                "FROM ASSET A, OBJECT_DETAIL OD WHERE A.OBJECT_DETAIL_ID = OD.OBJECT_DETAIL_ID;</sql>" +
            "</procedure>" +
            "</procedures>" +
            "<partitions>" +
            "<partition table='ASSET' column='ASSET_ID' />" +
            "<partition table='OBJECT_DETAIL' column='OBJECT_DETAIL_ID' />" +
            "</partitions>" +
            "</database>" +
            "</project>";

        final File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        final String projectPath = projectFile.getPath();

        final VoltCompiler compiler = new VoltCompiler();

        final boolean success = compiler.compileWithProjectXML(projectPath, "testout.jar");

        assertFalse(success);
    }

}
