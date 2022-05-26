/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

package org.voltdb.exportclient;

import java.io.File;
import java.net.URLEncoder;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

/**
 * Used for TestExportOpsEvents.
 * Makes a boring cluster that exports and can easily be commanded
 * to do things the old export client would be unhappy with.
 *
 */
public class VoltDBFickleCluster extends LocalCluster {

    static final String jar1Path = Configuration.getPathToCatalogForTest("fickle1.jar");
    static final String jar2Path = Configuration.getPathToCatalogForTest("fickle2.jar");
    static String depPath = null;
    static VoltDBFickleCluster m_cluster = new VoltDBFickleCluster();

    VoltDBFickleCluster() {
        super("fickle1.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        // fickle cluster doesn't yet work with valgrind
        overrideAnyRequestForValgrind();
    }

    public static void killNode() throws Exception {
        m_cluster.killSingleHost(1);
    }

    public static void rejoinNode() throws Exception {
        m_cluster.recoverOne(1, 0);
    }

    public void mutateCatalog() throws Exception {

    }

    public static void compile() throws Exception {
        if (depPath != null) return;

        System.out.printf("### Path: %s\n", jar1Path);

        String simpleSchema =
            "create stream blah (" +
            "ival bigint default 23 not null);\n" +
            "create table blah2 (" +
            "ival bigint default 23 not null, " +
            "PRIMARY KEY(ival));";

        File schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        String schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        //builder.addPartitionInfo("blah", "ival");
        builder.addPartitionInfo("blah2", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?);",
                new ProcedurePartitionData("blah2", "ival"));
        builder.addExport(true /* enabled */);
        boolean success = m_cluster.compile(builder);
        assert(success);

        simpleSchema =
            "create stream blah2 (" +
            "ival bigint default 23 not null);";

        schemaFile = VoltProjectBuilder.writeStringToTempFile(simpleSchema);
        schemaPath = schemaFile.getPath();
        schemaPath = URLEncoder.encode(schemaPath, "UTF-8");

        builder = new VoltProjectBuilder();
        builder.addSchema(schemaPath);
        builder.addPartitionInfo("blah2", "ival");
        builder.addStmtProcedure("Insert", "insert into blah2 values (?,?,?,?,?);");
        builder.addExport(true /* enabled */);
        //boolean success = m_cluster.compile(builder);
        success = builder.compile(jar2Path, 2, 2, 1);
        assert(success);
        depPath = builder.getPathToDeployment();
    }

    public static void start() throws Exception {
        assert (depPath != null);

        m_cluster.setHasLocalServer(true);
        m_cluster.startUp();
    }

    public static void stop() throws Exception {
        m_cluster.shutDown();
    }

    public static int getInternalPort(int hostId) {
        return m_cluster.internalPort(hostId);
    }

    public static int getPort(int hostId) {
        return m_cluster.port(hostId);
    }

    public static int getAdminPort(int hostId) {
        return m_cluster.adminPort(hostId);
    }

}
