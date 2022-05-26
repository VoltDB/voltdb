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

package org.voltdb.eng1016;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;

import junit.framework.TestCase;

public class Runner extends TestCase {

    public static void main(String[] args) throws Exception {
        // compile the catalog
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("create table items (id bigint not null, created bigint not null, primary key (id));");
        project.addLiteralSchema("create index idx_item_tree on items (created, id);");

        project.addStmtProcedure("CreateItem",
                                 "insert into items (id, created) values (?,?);",
                                 new ProcedurePartitionData("items", "id", "0")
                                 );
        project.addStmtProcedure("GetItems",
                                 "select id, created from items " +
                                 "where created <= ? and id < ? " +
                                 "order by created desc, id desc " +
                                 "limit ?;",
                                 new ProcedurePartitionData("items", "id", "1"));

        project.addPartitionInfo("items", "id");
        boolean success = project.compile(Configuration.getPathToCatalogForTest("poc.jar"));
        if (!success) {
            System.err.println("Failure to compile catalog.");
            System.exit(-1);
        }
        String pathToDeployment = project.getPathToDeployment();

        // start up voltdb
        ServerThread server = new ServerThread(Configuration.getPathToCatalogForTest("poc.jar"), pathToDeployment, BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        final org.voltdb.client.Client voltclient = ClientFactory.createClient();
        voltclient.createConnection("localhost");

        // create initial items
        voltclient.callProcedure("CreateItem", 0, 10);
        voltclient.callProcedure("CreateItem", 1, 11);
        voltclient.callProcedure("CreateItem", 2, 12);

        // check that the query does the right thing
        VoltTable result = voltclient.callProcedure("GetItems",11,1,1).getResults()[0];
        System.out.println(result.toJSONString());
        if (result.getRowCount() != 1) {
            System.err.printf("Call failed with %d rows\n", result.getRowCount());
        }

        // clean up / shutdown
        voltclient.close();
        server.shutdown();
        server.join();
    }
}
