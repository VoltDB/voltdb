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

package org.voltdb.regressionsuites;

import java.util.Collections;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAdHocLargeSuite extends RegressionSuite {

    public void testBasic() throws Exception {
        if (isValgrind()) {
            // don't run this test under valgrind, as it needs IPC support.
            return;
        }

        Client client = getClient();
        ClientResponse cr;

        // This query needs to materialize its derived table, which can get very large
        // because it does a simple cross join.  It also potentially requires aggregation over
        // a table that may be larger than the allowed amount of temp table storage.
        String query =
                  "select count(*), "
                + "       max(dtbl.theval), "
                + "       min(dtbl.theval), "
                + "       max(dtbl.t1_inl_vc01) "
                + "from (select t1.i,  t1.inl_vc00,  t1.inl_vc01 as t1_inl_vc01,  t1.longval, "
                +  "            t2.i,  t2.inl_vc00,  t2.inl_vc01,                 t2.longval as theval"
                + "      from t as t1, t  as t2) as dtbl";

        // RETURN RESULTS TO STORED PROCEDURE
        //  SEQUENTIAL SCAN of "DTBL"
        //   inline Serial AGGREGATION ops: COUNT(*), MAX(column#1), MIN(column#1), MAX(column#0)
        //   NEST LOOP INNER JOIN
        //    SEQUENTIAL SCAN of "T (T1)"
        //    SEQUENTIAL SCAN of "T (T2)"

        cr = client.callProcedure("@AdHocLarge", query);
        assertContentOfTable(new Object[][] {{0, null, null, null}}, cr.getResults()[0]);

        // Now add some data
        int rowCnt = 0;
        for (; rowCnt < 5; ++rowCnt) {
            String val = String.join("", Collections.nCopies(1, "long " + Integer.toString(rowCnt)));
            String inlineVal = String.join("", Collections.nCopies(1, "short " + Integer.toString(rowCnt)));
            cr = client.callProcedure("t.Insert", rowCnt,
                    inlineVal,  inlineVal, val);

            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Query should still execute okay,
        // but this is not enough data to require
        // swapping intermediate results to disk
        cr = client.callProcedure("@AdHocLarge", query);
        assertContentOfTable(new Object[][] {{25, "long 4", "long 0", "short 4"}}, cr.getResults()[0]);

        // @AdHocLarge produces the same answer as @AdHoc
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {{25, "long 4", "long 0", "short 4"}}, cr.getResults()[0]);

        // Add more data to T, so that it has 500 rows.
        // This will cause the LTT block cache to overflow, and
        // write data to disk.
        //
        // The temp table produced by the nested loop join executor
        // will use the most memory:
        //   Each tuple in this table will be
        //     (1 + 4 + 64 + 64 + 8 + 4 + 64 + 64 + 8) = 281 bytes   of inlined data
        //                                     and about  40 bytes   of non-inlined data
        //                                               -------------------------------
        //                                      totaling 321 bytes   per tuple
        //
        // There will be 500^2 or 250,000 rows in the table for a total size of 80,250,000 bytes (80MB or so)
        // This is 9 large temp table blocks.
        //
        // The server is configured to only have 25MB (3 large temp table blocks), so the EE will
        // need to swap blocks to disk as it executes the join.
        final int NUM_ROWS = 500;
        for (; rowCnt < NUM_ROWS; ++rowCnt) {
            String val = String.join("", Collections.nCopies(1, "long " + Integer.toString(rowCnt)));
            String inlineVal = String.join("", Collections.nCopies(1, "short " + Integer.toString(rowCnt)));
            cr = client.callProcedure("t.Insert", rowCnt,
                    inlineVal,  inlineVal, val);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        cr = client.callProcedure("@AdHocLarge", query);
        assertContentOfTable(new Object[][] {{NUM_ROWS * NUM_ROWS, "long 99", "long 0", "short 99"}}, cr.getResults()[0]);

        // The query now gets an expected error message when executed normally.
        verifyProcFails(client, "More than 25 MB of temp table memory used while executing SQL",
                "@AdHoc", query);

        // Variant of test query with an ORDER BY clause, which exercises large temp table sorting.
        String orderByLimitQuery =
                "select theval1 || '--' || theval2 as vals "
              + "from (select t1.i,  t1.inl_vc00,  t1.inl_vc01 as t1_inl_vc01,  t1.longval as theval1, "
              +  "            t2.i,  t2.inl_vc00,  t2.inl_vc01,                 t2.longval as theval2 "
              + "      from t as t1, t  as t2) as dtbl "
              + "order by vals "
              + "limit 5";
        cr = client.callProcedure("@AdHocLarge", orderByLimitQuery);
        assertContentOfTable(new Object[][] {
            {"long 0--long 0"},
            {"long 0--long 1"},
            {"long 0--long 10"},
            {"long 0--long 100"},
            {"long 0--long 101"}
        }, cr.getResults()[0]);

        // Same as above but use OFFSET
        String orderByLimitOffsetQuery =
                "select theval1 || '--' || theval2 as vals "
              + "from (select t1.i,  t1.inl_vc00,  t1.inl_vc01 as t1_inl_vc01,  t1.longval as theval1, "
              +  "            t2.i,  t2.inl_vc00,  t2.inl_vc01,                 t2.longval as theval2 "
              + "      from t as t1, t  as t2) as dtbl "
              + "order by vals "
              + "limit 5 "
              + "offset 5 ";
        cr = client.callProcedure("@AdHocLarge", orderByLimitOffsetQuery);
        assertContentOfTable(new Object[][] {
            {"long 0--long 102"},
            {"long 0--long 103"},
            {"long 0--long 104"},
            {"long 0--long 105"},
            {"long 0--long 106"},
        }, cr.getResults()[0]);

        // Select the last 5 rows, using OFFSET, with no limit
        String orderByOffsetQuery =
                "select theval1 || '--' || theval2 as vals "
                        + "from (select t1.i,  t1.inl_vc00,  t1.inl_vc01 as t1_inl_vc01,  t1.longval as theval1, "
                        +  "            t2.i,  t2.inl_vc00,  t2.inl_vc01,                 t2.longval as theval2 "
                        + "      from t as t1, t  as t2) as dtbl "
                        + "order by vals "
                        + "offset 249995 ";
        cr = client.callProcedure("@AdHocLarge", orderByOffsetQuery);
        assertContentOfTable(new Object[][] {
            {"long 99--long 95"},
            {"long 99--long 96"},
            {"long 99--long 97"},
            {"long 99--long 98"},
            {"long 99--long 99"},
        }, cr.getResults()[0]);

        // Delete some rows
        validateTableOfScalarLongs(client, "delete from t where i >= 5", new long[] {NUM_ROWS - 5});

        // Query can now execute as normal.
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {{25, "long 4", "long 0", "short 4"}}, cr.getResults()[0]);

        // Test large temp table sorting using ORDER BY by itself
        String orderByQuery =
                "select theval1 || '--' || theval2 as vals "
              + "from (select t1.i,  t1.inl_vc00,  t1.inl_vc01 as t1_inl_vc01,  t1.longval as theval1, "
              +  "            t2.i,  t2.inl_vc00,  t2.inl_vc01,                 t2.longval as theval2 "
              + "      from t as t1, t as t2 "
              + "      where mod(t1.i, 3) = 0 "
              + "          and mod(t2.i, 3) = 0) as dtbl "
              + "order by vals desc ";
        cr = client.callProcedure("@AdHocLarge", orderByQuery);
        System.out.println(cr.getResults()[0]);
        assertContentOfTable(new Object[][] {
            {"long 3--long 3"},
            {"long 3--long 0"},
            {"long 0--long 3"},
            {"long 0--long 0"}
        }, cr.getResults()[0]);
    }

    static public junit.framework.Test suite() throws Exception {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHocLargeSuite.class);

        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(          // status byte: 1
                "create table t (i integer not null, " //  8
                + "inl_vc00 varchar(63 bytes), "       // 64
                + "inl_vc01 varchar(63 bytes), "       // 64
                + "longval varchar(500000));");        //  8 (pointer to StringRef)
        //                                        -->    145 bytes per tuple (not counting non-inlined data)

        project.setQueryTimeout(1000 * 60 * 5); // five minutes
        config = new LocalCluster("adhoclarge-voltdbBackend.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        System.setProperty("TEMP_TABLE_MAX_SIZE", "25"); // in MB
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

    public TestAdHocLargeSuite(String name) {
        super(name);
    }
}
