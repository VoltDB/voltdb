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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestAdhocMigrateTable extends AdhocDDLTestBase {
    private void setup(String ddl) throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 1, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        startSystem(config);
    }

    @Test
    public void testSimple() throws Exception {
        testMigrate(
                "CREATE TABLE with_ttl migrate to target foo (i TIMESTAMP DEFAULT NOW() NOT NULL, j FLOAT) USING TTL 1 minutes ON COLUMN i;\n" +
                "CREATE TABLE without_ttl migrate to target foo (i int NOT NULL, j FLOAT, k VARCHAR(20));\n" +
                "CREATE TABLE with_ttl_no_target(i TIMESTAMP DEFAULT NOW() NOT NULL, j FLOAT) USING TTL 1 minutes ON COLUMN i;\n" +
                "CREATE TABLE without_ttl_no_target(i int NOT NULL, j FLOAT);\n",
                Stream.of(
                        Pair.of("MIGRATE FROM without_ttl;", false),
                        Pair.of("MIGRATE FROM without_ttl WHERE not migrating;", true),
                        Pair.of("MIGRATE FROM without_ttl WHERE i < 0 and not migrating;", true),
                         // ENG-17049 add support for like ''
                        Pair.of("MIGRATE FROM without_ttl WHERE k Like 'sss%' and not migrating;", true),
                        Pair.of("MIGRATE FROM with_ttl;", false),
                        Pair.of("MIGRATE FROM with_ttl WHERE j > 0;", false),
                        Pair.of("MIGRATE FROM with_ttl WHERE not migrating;", true),
                        Pair.of("MIGRATE FROM with_ttl WHERE not migrating() and j > 0;", true),
                        Pair.of("MIGRATE FROM with_ttl_no_target where not migrating;", false),
                        Pair.of("MIGRATE FROM without_ttl_no_target where not migrating();", false),
                        // we do prevent user from doing this
                        Pair.of("MIGRATE FROM with_ttl WHERE not not migrating;", false),
                        Pair.of("MIGRATE FROM with_ttl WHERE migrating() and j > 0;", false),
                        // we don't prevent user from doing this
                        Pair.of("MIGRATE FROM with_ttl WHERE not (not migrating);", true),
                        // ENG-16463
                        Pair.of("MIGRATE FROM without_ttl where j is not null;", false),
                        Pair.of("MIGRATE FROM without_ttl where j is null;", false),
                        Pair.of("MIGRATE FROM without_ttl where j is not null and not migrating;", true),
                        Pair.of("MIGRATE FROM without_ttl where j is null and not migrating;", true)
                ).collect(Collectors.toList()));
    }

    private void testMigrate(String ddl, List<Pair<String, Boolean>> queries) throws Exception {
        try {
            setup(ddl);
            queries.forEach(stmtAndExpected -> {
                final String stmt = stmtAndExpected.getFirst();
                final boolean pass = stmtAndExpected.getSecond();
                try {
                    m_client.callProcedure("@AdHoc", stmt);
                    assertTrue("Expected query " + stmt + " to fail", pass);
                } catch (IOException | ProcCallException e) {
                    final String msg = e.getMessage();
                    assertTrue("Received unexpected failure for query " + stmt + ": " + msg,
                            !pass && (msg.contains(" invalid WHERE expression") ||
                                    msg.contains("Cannot migrate from table ") ||
                                    msg.contains("unexpected token: NOT")));
                }
            });
        } finally {
            teardownSystem();
        }
    }

    @Test
    // test sql funtion contains ?
    public void testENG16508() throws Exception {
        String ddl = "CREATE TABLE without_ttl migrate to target foo (i int NOT NULL, j FLOAT, ts TIMESTAMP);\n";
        setup(ddl);
        ClientResponse res = m_client.callProcedure("@AdHoc", "MIGRATE FROM without_ttl where not migrating and ts < dateAdd(second, ?, now);", 1);
        assertEquals(res.getStatus(), ClientResponse.SUCCESS);
        teardownSystem();
    }

    @Test
    public void testENG16879() throws Exception {
        String ddl = "CREATE TABLE without_ttl migrate to target foo (i int NOT NULL, j FLOAT, ts TIMESTAMP);\n";
        setup(ddl);
        try {
            m_client.callProcedure("@AdHoc", "MIGRATE FROM without_ttl WHERE NOT MIGRATING() \n" +
                                             "AND without_ttl.i < (SELECT MIN(i) FROM without_ttl \n" +
                                             "WHERE FLOOR(without_ttl.j) <> without_ttl.j ORDER BY NULL, without_ttl.i);");
            fail("Hsql parser should fail when parsing this sql query.");
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Expression is too complicated"));
        } finally {
            teardownSystem();
        }
    }
}
