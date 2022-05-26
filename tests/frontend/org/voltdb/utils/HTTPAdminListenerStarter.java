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

package org.voltdb.utils;

import org.voltdb.ServerThread;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;

public class HTTPAdminListenerStarter {
    /**
     * Added a main here for manual test purposes. It just starts up
     * a brain-dead VoltDB server so you can look at the admin page.
     */
    public static void main(String[] args) throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "PRIMARY KEY(ival));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?);");
        builder.setHTTPDPort(8080);
        builder.setJSONAPIEnabled(true);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("rejoin.jar"), 1, 1, 0);
        assert(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("rejoin.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("rejoin.xml");
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        Thread.sleep(240000);
    }
}
