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
import static org.junit.Assert.fail;

import org.junit.Test;
import org.voltcore.utils.Pair;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestAdhocLimitOffset extends AdhocDDLTestBase {
    @Test
    public void testBasicCreateTable() throws Exception {
        testLimitOffset(false);
        testLimitOffset(true);
    }

    private static void compareAdhoc(Client client, String query, List<Object> expected) {
        try {
            System.err.println(query);
            final List<Object> actual = getColumn(
                    client.callProcedure("@AdHoc", query).getResults()[0],
                    0, VoltType.INTEGER);
            assertEquals(String.format("Query %s expected %s; but got %s", query, expected, actual),
                    expected, actual);
        } catch (IOException | ProcCallException e) {
            fail(String.format("Query \"%s\" should have worked fine", query));
        }
    }

    private void testLimitOffset(boolean isPartitioned) throws Exception {
        VoltDB.Configuration config = new VoltDB.Configuration();
        String ddl = "create table FOO (ID integer not null);" +
                (isPartitioned ? "\nPARTITION TABLE FOO ON COLUMN ID;" : "");
        try {
            createSchema(config, ddl, 2, 1, 0);
            startSystem(config);
            Stream.of("SELECT ID FROM FOO;",
                    "SELECT ID FROM FOO OFFSET 0;",
                    "SELECT ID FROM FOO OFFSET 1;",
                    "SELECT ID FROM FOO OFFSET 2;",
                    "SELECT ID FROM FOO LIMIT 0;",
                    "SELECT ID FROM FOO LIMIT 1;",
                    "SELECT ID FROM FOO LIMIT 2;",
                    "SELECT ID FROM FOO OFFSET 1;",
                    "SELECT ID FROM FOO LIMIT 4;",
                    "SELECT ID FROM FOO OFFSET 0 LIMIT 0;",
                    "SELECT ID FROM FOO OFFSET 1 LIMIT 0;",
                    "SELECT ID FROM FOO OFFSET 2 LIMIT 0;",
                    "SELECT ID FROM FOO OFFSET 0 LIMIT 1;",
                    "SELECT ID FROM FOO OFFSET 1 LIMIT 1;",
                    "SELECT ID FROM FOO OFFSET 2 LIMIT 1;",
                    "SELECT ID FROM FOO OFFSET 0 LIMIT 1000;",
                    "SELECT ID FROM FOO OFFSET 1 LIMIT 1000;",
                    "SELECT ID FROM FOO OFFSET 2 LIMIT 1000;")
                    .forEach(stmt -> compareAdhoc(m_client, stmt, Collections.emptyList()));
            IntStream.range(0, 4).forEach(i -> {    // 0, 1, 2, 3
                final String stmt = String.format("INSERT INTO FOO VALUES %d;\n", i);
                try {
                    m_client.callProcedure("@AdHoc", stmt);
                } catch (IOException | ProcCallException e) {
                    fail("Query \"" + stmt + "\" should have worked fine");
                }
            });
            Stream.of(
                    Pair.of("SELECT ID FROM FOO ORDER BY ID LIMIT 0;", Collections.EMPTY_LIST),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID OFFSET 0;", Arrays.asList(0, 1, 2, 3)),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID LIMIT 4;", Arrays.asList(0, 1, 2, 3)),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID OFFSET 1;", Arrays.asList(1, 2, 3)),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID OFFSET 1 LIMIT 2;", Arrays.asList(1, 2)),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID OFFSET 3 LIMIT 2;", Collections.singletonList(3)),
                    Pair.of("SELECT ID FROM FOO ORDER BY ID OFFSET 4 LIMIT 2;", Collections.EMPTY_LIST))
                    .forEachOrdered(stmtAndExpected ->
                            compareAdhoc(m_client, stmtAndExpected.getFirst(), stmtAndExpected.getSecond()));
        } finally {
            try {
                teardownSystem();
            } catch (Exception e) {
            }
        }
    }
}
