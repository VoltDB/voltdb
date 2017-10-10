/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
        Client client = getClient();

        ClientResponse cr;
        cr = client.callProcedure("@AdHocLarge", "select count(*) from (select * from t as t1, t  as t2) as dtbl");
        assertEquals(0, cr.getResults()[0].asScalarLong());

        // Now add some data
        int rowCnt = 0;
        for (; rowCnt < 5; ++rowCnt) {
            String val = String.join("", Collections.nCopies(5000, "a"));
            String inlineVal = String.join("", Collections.nCopies(63, "b"));
            cr = client.callProcedure("t.Insert", rowCnt,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    val);

            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Query should still execute okay
        cr = client.callProcedure("@AdHocLarge", "select count(*) from (select * from t as t1, t  as t2) as dtbl");
        assertEquals(25, cr.getResults()[0].asScalarLong());

        // Now add more data, such that the LTT block cache will overflow.
        // We'll need to spill to disk in this case
        for (; rowCnt < 500; ++rowCnt) {
            String val = String.join("", Collections.nCopies(5000, "a"));
            String inlineVal = String.join("", Collections.nCopies(63, "b"));
            cr = client.callProcedure("t.Insert", rowCnt,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    inlineVal,  inlineVal,  inlineVal, inlineVal,
                    val);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }

        // Topend routines to store large temp table blocks just return false (failure to store)
        // So this error is expected.
        verifyProcFails(client, "Topend failed to store LTT block",
                "@AdHocLarge",
                "select count(*) from (select * from t as t1, t  as t2) as dtbl");

        // Query gets the conventional error message when executed normally.
        verifyProcFails(client, "More than 25 MB of temp table memory used while executing SQL",
                "@AdHoc", "select count(*) from (select * from t as t1, t  as t2) as dtbl");

        // Delete some rows
        validateTableOfScalarLongs(client, "delete from t where i >= 5", new long[] {495});

        // Query can now execute as normal.
        validateTableOfScalarLongs(client ,"select count(*) from (select * from t as t1, t  as t2) as dtbl",
                new long[] {25});
    }

    static public junit.framework.Test suite() throws Exception {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestAdHocLargeSuite.class);

        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema("create table t (i integer not null, "
                + "inline_vc00 varchar(63 bytes), "
                + "inline_vc01 varchar(63 bytes), "
                + "inline_vc02 varchar(63 bytes), "
                + "inline_vc03 varchar(63 bytes), "
                + "inline_vc04 varchar(63 bytes), "
                + "inline_vc05 varchar(63 bytes), "
                + "inline_vc06 varchar(63 bytes), "
                + "inline_vc07 varchar(63 bytes), "
                + "inline_vc08 varchar(63 bytes), "
                + "inline_vc09 varchar(63 bytes), "
                + "inline_vc10 varchar(63 bytes), "
                + "inline_vc11 varchar(63 bytes), "
                + "inline_vc12 varchar(63 bytes), "
                + "inline_vc13 varchar(63 bytes), "
                + "inline_vc14 varchar(63 bytes), "
                + "inline_vc15 varchar(63 bytes), "
                + "val varchar(500000));");

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
