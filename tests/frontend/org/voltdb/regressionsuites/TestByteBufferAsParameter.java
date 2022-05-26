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

import java.nio.ByteBuffer;
import org.voltdb.client.*;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestByteBufferAsParameter extends RegressionSuite {

    @Test
    public void testLoadByteBufferParam() throws Exception {
        Client client = getClient();

        client.callProcedure("ENG10375.insert", 1, new byte[] {(byte)0xDE, (byte)0xAD, (byte)0xDE, (byte)0xEF});
        client.callProcedure("ENG10375.insert", 2, new byte[] {(byte)0x2F});
        client.callProcedure("ENG10375.insert", 3, new byte[] {(byte)0x2D});
        client.callProcedure("ENG10375.insert", 4, new byte[] {(byte)0x2F, (byte)0X2F});
        client.callProcedure("ENG10375.insert", 5, new byte[] {(byte)0x2F});
        client.callProcedure("ENG10375.insert", 6, new byte[] {(byte)0x00, (byte)0x13, (byte)0x00, (byte)0xBA});

        byte[] by = {(byte)0x2F};
        ByteBuffer bb = ByteBuffer.wrap(by);

        VoltTable vt = client.callProcedure("ByteBufferAsParam", bb).getResults()[0];
        assertTrue(vt.getRowCount() == 2);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestByteBufferAsParameter(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestByteBufferAsParameter.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE ENG10375 (\n"
                + "  ID INTEGER NOT NULL PRIMARY KEY,\n"
                + "  A VARBINARY"
                + ");\n"
                ;

        literalSchema += "CREATE PROCEDURE FROM CLASS org.voltdb_testprocs.regressionsuites.basecase.ByteBufferAsParam;";
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (Exception e) {
            fail();
        }

        config = new LocalCluster("fixedsql-threesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
