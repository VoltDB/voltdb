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

package org.voltdb.compiler;

import java.io.File;

import junit.framework.TestCase;

import org.voltdb.utils.BuildDirectoryUtils;

public class TestMilestoneOneCompile extends TestCase {
    public static final String CATALOG_CLUSTER_NAME = "cluster";
    public static final String CATALOG_DATABASE_NAME = "database";

    public static final String MILESTONE_ONE_DDL = "CREATE TABLE WAREHOUSE (\n" +
                                                   "    W_ID INTEGER DEFAULT '0' NOT NULL,\n" +
                                                   "    W_NAME VARCHAR(16) DEFAULT NULL,\n" +
                                                   "    PRIMARY KEY (W_ID)\n" +
                                                   ");\n" +
                                                   "CREATE TABLE STOCK (\n" +
                                                   "    S_I_ID INTEGER NOT NULL,\n"+
                                                   "    S_W_ID INTEGER NOT NULL,\n"+
                                                   "    S_QUANTITY INTEGER NOT NULL,\n"+
                                                   "    PRIMARY KEY (S_I_ID)\n" +
                                                   ");";

    public void testForMilestoneOne() {
        File ddlFile = VoltProjectBuilder.writeStringToTempFile(MILESTONE_ONE_DDL);
        String ddlPath = ddlFile.getPath();

        String simpleProject =
            "<?xml version=\"1.0\"?>\n" +
            "<project>" +
            "<database name='database'>" +
            "<groups>" +
            "<group adhoc='true' name='default' sysproc='true'/>" +
            "</groups>" +
            "<schemas><schema path='" + ddlPath + "' /></schemas>" +
            "<procedures>" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneInsert' />" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneSelect' />" +
            "<procedure class='org.voltdb.compiler.procedures.MilestoneOneCombined' />" +
            "</procedures>" +
            "<partitions>" +
            "<partition table='WAREHOUSE' column='W_ID' />" +
            "</partitions>" +
            "</database>" +
            "</project>";

        System.out.println(simpleProject);

        File projectFile = VoltProjectBuilder.writeStringToTempFile(simpleProject);
        String projectPath = projectFile.getPath();
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        String catalogJar = testDir + File.separator + "milestoneOneCatalog.jar";
        VoltCompiler compiler = new VoltCompiler();
        boolean success = compiler.compile(projectPath, catalogJar, System.out, null);

        assertTrue(success);
    }
}
