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

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCompactingViewsSuite extends RegressionSuite {

    static final Class<?>[] PROCEDURES = {};

    public TestCompactingViewsSuite(String name) {
        super(name);
    }

    void runCompactingViewsForTable(String insertName, String deleteName, String queryName) throws Exception {
        Client client = getClient();

        String filler = "a";
        for (int i = 0; i < 62; i++) {
            filler = filler + "a";
        }
        assert(filler.length() == 63);

        final int MAX_ROWS = 25000;

        ProcedureCallback modifyOneCheck = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.err.println(clientResponse.getStatusString());
                }

                assertEquals(ClientResponse.SUCCESS, clientResponse.getStatus());
                assertEquals(1, clientResponse.getResults()[0].asScalarLong());
            }
        };

        // insert baseline rows
        System.out.printf("Inserting %d rows into the primary table and the view\n", MAX_ROWS);
        for (int i = 0; i < MAX_ROWS; i++) {
            client.callProcedure(modifyOneCheck, insertName, i, String.valueOf(i),
                    filler, filler, filler, filler, filler, filler, filler, filler);
        }
        client.drain();

        // delete half of them - should trigger compaction
        System.out.printf("Deleting all even rows\n");
        for (int i = 0; i < MAX_ROWS; i += 2) {
            client.callProcedure(modifyOneCheck, deleteName, i);
        }
        client.drain();

        // do a query that hits the index hard
        System.out.printf("Doing a full select and using the index for ordering.\n");
        VoltTable table1 = client.callProcedure(queryName).getResults()[0];
        assertEquals(MAX_ROWS / 2, table1.getRowCount());

        // put the missing half back
        System.out.printf("Inserting even rows back into the table and view\n");
        for (int i = 0; i < MAX_ROWS; i += 2) {
            VoltTable table = client.callProcedure(insertName, i, String.valueOf(i),
                    filler, filler, filler, filler, filler, filler, filler, filler).getResults()[0];
            assertEquals(1, table.asScalarLong());
        }

        // adding view duplicates for half of the rows
        System.out.printf("Inserting duplicates of half of the tuples (with unique primary keys)\n");
        for (int i = MAX_ROWS + 1; i < (MAX_ROWS * 2); i += 2) {
            VoltTable table = client.callProcedure(insertName, i, String.valueOf(i / 2),
                    filler, filler, filler, filler, filler, filler, filler, filler).getResults()[0];
            assertEquals(1, table.asScalarLong());
        }

        // delete all of the rows again, but in three passes to trigger more compaction
        System.out.printf("Deleting all %d rows\n", MAX_ROWS);
        for (int i = 1; i < MAX_ROWS; i += 2) {
            VoltTable table = client.callProcedure(deleteName, i).getResults()[0];
            assertEquals(1, table.asScalarLong());
        }
        for (int i = 0; i < MAX_ROWS; i += 2) {
            VoltTable table = client.callProcedure(deleteName, i).getResults()[0];
            assertEquals(1, table.asScalarLong());
        }
        for (int i = MAX_ROWS + 1; i < (MAX_ROWS * 2); i += 2) {
            VoltTable table = client.callProcedure(deleteName, i).getResults()[0];
            assertEquals(1, table.asScalarLong());
        }
    }

    public void testPartitionedCompactingViews() throws Exception {
        // hard to test compaction in valgrind via java (at the moment)
        if (isValgrind()) {
            return;
        }

        runCompactingViewsForTable("PP.insert", "PP.delete", "selectPP");
        runCompactingViewsForTable("PR.insert", "deletePR", "selectPR");
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        final MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCompactingViewsSuite.class);

        final VoltProjectBuilder project = new VoltProjectBuilder();

        try {
            // partitioned
            project.addLiteralSchema(
                    "CREATE TABLE PP(id INTEGER NOT NULL, value VARCHAR(63), " +
                    "e1 VARCHAR(63), e2 VARCHAR(63), e3 VARCHAR(63), e4 VARCHAR(63)," +
                    "e5 VARCHAR(63), e6 VARCHAR(63), e7 VARCHAR(63), e8 VARCHAR(63)," +
                    "PRIMARY KEY (id)); "
            );
            project.addLiteralSchema(
                    "CREATE INDEX FOO ON PP (value, e1, e2, e3, e4, e5, e6, e7, e8);"
            );
            project.addLiteralSchema(
                    "CREATE VIEW VP(id, value, e1, e2, e3, e4, e5, e6, e7, e8, c) " +
                    "AS SELECT id, value, e1, e2, e3, e4, e5, e6, e7, e8, COUNT(*) " +
                    "FROM PP GROUP BY id, value, e1, e2, e3, e4, e5, e6, e7, e8;"
            );
            project.addPartitionInfo("pp", "id");

            // replicated
            project.addLiteralSchema(
                    "CREATE TABLE PR(id INTEGER NOT NULL, value VARCHAR(1000), " +
                    "e1 VARCHAR(63), e2 VARCHAR(63), e3 VARCHAR(63), e4 VARCHAR(63)," +
                    "e5 VARCHAR(63), e6 VARCHAR(63), e7 VARCHAR(63), e8 VARCHAR(63)," +
                    "PRIMARY KEY (id)); " +
                    "CREATE VIEW VR(value, e1, e2, e3, e4, e5, e6, e7, e8, c) " +
                    "AS SELECT value, e1, e2, e3, e4, e5, e6, e7, e8, COUNT(*) " +
                    "FROM PR GROUP BY value, e1, e2, e3, e4, e5, e6, e7, e8;"
            );

            project.addStmtProcedure("selectPP", "select id from PP order by value, e1, e2, e3, e4, e5, e6, e7, e8");
            project.addStmtProcedure("selectPR", "select id from PR order by value, e1, e2, e3, e4, e5, e6, e7, e8");

            project.addStmtProcedure("deletePR", "delete from PR where id = ?");
        }
        catch (IOException error) {
            fail(error.getMessage());
        }

        // JNI local with 1 site
        config = new LocalCluster("sqltypes-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        boolean t1 = config.compile(project);
        assertTrue(t1);
        builder.addServerConfig(config);

        return builder;
    }
}
