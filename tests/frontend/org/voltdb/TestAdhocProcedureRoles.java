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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.VoltProjectBuilder.RoleInfo;
import org.voltdb.compiler.VoltProjectBuilder.UserInfo;
import org.voltdb.utils.MiscUtils;

public class TestAdhocProcedureRoles extends AdhocDDLTestBase {

    final static boolean VERBOSE = false;
    final static String CATALOG_PATH = Configuration.getPathToCatalogForTest("adhocddl.jar");
    final static String DEPLOYMENT_PATH = Configuration.getPathToCatalogForTest("adhocddl.xml");
    final static RoleInfo ADMIN_TEMPLATE = new RoleInfo(null, false, false, true, false, false, false);
    final static RoleInfo USER_TEMPLATE = new RoleInfo(null, false, false, false, false, false, false);

    private class Tester
    {
        final VoltProjectBuilder builder;

        Tester()
        {
            if (VERBOSE) {
                System.out.println("================= Begin Test ==================");
            }
            this.builder = new VoltProjectBuilder();
            this.builder.setSecurityEnabled(true, true);
            this.builder.setUseDDLSchema(true);
        }

        void createTable(String name) throws IOException
        {
            this.builder.addLiteralSchema(String.format(
                    "create table %s (" +
                        "ID integer not null," +
                        "VAL bigint, " +
                        "constraint PK_TREE primary key (ID)" +
                    ");", name));
        }

        void createRoles(final RoleInfo template, String... roles)
        {
            this.builder.addRoles(RoleInfo.fromTemplate(template, roles));
        }

        void createUser(String user, String password, String... roles)
        {
            this.builder.addUsers(new UserInfo[] {new UserInfo(user, password, roles)});
        }

        void createProcedureAdHoc(String procName, String role, String tableName, boolean drop) throws Exception
        {
            List<String> statements = new ArrayList<String>();
            if (drop) {
                statements.add(String.format("drop procedure %s;", procName));
            }
            statements.add(String.format("create procedure %s allow %s as select count(*) from %s;",
                                         procName, role, tableName));
            String ddl = StringUtils.join(statements, '\n');
            try {
                if (VERBOSE) {
                    System.out.println(String.format(":::DDL::: %s", ddl.toString()));
                }
                m_client.callProcedure("@AdHoc", ddl.toString());
            }
            catch (ProcCallException e) {
                fail(String.format("Failed to create procedure\n%s\n%s", ddl, e.toString()));
            }
            assertTrue(findProcedureInSystemCatalog(procName));
        }

        void compile() throws Exception
        {
            if (VERBOSE) {
                System.out.println(":::Deployment:::");
            }
            Catalog catalog = this.builder.compile(CATALOG_PATH, 2, 1, 0, null);
            if (VERBOSE) {
                for (String line : Files.readAllLines(Paths.get(this.builder.getPathToDeployment()), Charset.defaultCharset())) {
                    System.out.println(line);
                }
            }
            assertNotNull("Schema compilation failed", catalog);
            MiscUtils.copyFile(this.builder.getPathToDeployment(), DEPLOYMENT_PATH);
        }

        void start() throws Exception
        {
            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = CATALOG_PATH;
            config.m_pathToDeployment = DEPLOYMENT_PATH;
            startServer(config);
        }

        void connect(String user, String password) throws Exception
        {
            startClient(new ClientConfig(user, password));
        }

        void stop() throws Exception
        {
            stopServer();
        }

        void disconnect() throws Exception
        {
            stopClient();
        }

        void callProcedure(String procName, boolean succeeds) throws Exception
        {
            try {
                ClientResponse resp = m_client.callProcedure(procName);
                if (succeeds) {
                    assertEquals(resp.getStatus(), ClientResponse.SUCCESS);
                }
                else {
                    assertTrue(resp.getStatus() != ClientResponse.SUCCESS);
                }
            }
            catch (ProcCallException e) {
                if (succeeds) {
                    e.printStackTrace();
                    fail(String.format("Failed to call procedure %s", procName));
                }
            }
        }
    }

    @Test
    public void testGoodUserCall() throws Exception
    {
        Tester tester = new Tester();
        tester.createTable("FOO");
        tester.createRoles(ADMIN_TEMPLATE, "ADMIN");
        tester.createRoles(USER_TEMPLATE, "GOOD");
        tester.createUser("ADMIN", "PASSWORD", "ADMIN");
        tester.createUser("USER", "PASSWORD", "GOOD");
        tester.compile();
        try {
            tester.start();
            try {
                tester.connect("ADMIN", "PASSWORD");
                tester.createProcedureAdHoc("PROC", "GOOD", "FOO", false);
            }
            finally {
                tester.disconnect();
            }
            try {
                tester.connect("USER", "PASSWORD");
                tester.callProcedure("PROC", true);
            }
            finally {
                tester.disconnect();
            }
        }
        finally {
            tester.stop();
        }
    }

    @Test
    public void testBadUserCall() throws Exception
    {
        Tester tester = new Tester();
        tester.createTable("FOO");
        tester.createRoles(ADMIN_TEMPLATE, "ADMIN");
        tester.createRoles(USER_TEMPLATE, "GOOD", "BAD");
        tester.createUser("ADMIN", "PASSWORD", "ADMIN");
        tester.createUser("USER", "PASSWORD", "BAD");
        tester.compile();
        try {
            tester.start();
            try {
                tester.connect("ADMIN", "PASSWORD");
                tester.createProcedureAdHoc("PROC", "GOOD", "FOO", false);
            }
            finally {
                tester.disconnect();
            }
            try {
                tester.connect("USER", "PASSWORD");
                tester.callProcedure("PROC", false);
            }
            finally {
                tester.disconnect();
            }
        }
        finally {
            tester.stop();
        }
    }

    // Not that interesting since the procedure is dropped to change its permissions.
    // When alter is supported it can become more interesting.
    @Test
    public void testGoodAndBadUserCall() throws Exception
    {
        Tester tester = new Tester();
        tester.createTable("FOO");
        tester.createRoles(ADMIN_TEMPLATE, "ADMIN");
        tester.createRoles(USER_TEMPLATE, "GOOD", "BAD");
        tester.createUser("ADMIN", "PASSWORD", "ADMIN");
        tester.createUser("USER", "PASSWORD", "GOOD");
        tester.compile();
        try {
            tester.start();
            try {
                tester.connect("ADMIN", "PASSWORD");
                tester.createProcedureAdHoc("PROC", "GOOD", "FOO", false);
            }
            finally {
                tester.disconnect();
            }
            try {
                tester.connect("USER", "PASSWORD");
                tester.callProcedure("PROC", true);     // expect success
            }
            finally {
                tester.disconnect();
            }
            try {
                tester.connect("ADMIN", "PASSWORD");
                tester.createProcedureAdHoc("PROC", "BAD", "FOO", true);
            }
            finally {
                tester.disconnect();
            }
            try {
                tester.connect("USER", "PASSWORD");
                tester.callProcedure("PROC", false);    // expect failure
            }
            finally {
                tester.disconnect();
            }
        }
        finally {
            tester.stop();
        }
    }

}
