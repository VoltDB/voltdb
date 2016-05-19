/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.Client;
import org.voltdb.client.ClientImpl;

/**
 * End to end CSV formatter tests using the injected socket importer.
 *
 */

public class TestCSVStrictQuoteAndBlankErrorSuite extends TestCSVFormatterSuiteBase {

    public TestCSVStrictQuoteAndBlankErrorSuite(final String name) {
        super(name);
    }

    public void testStrictQuoteAndBlankError() throws Exception {
        System.out.println("testStrictQuote");

        Client client = getClient();
        while (!((ClientImpl) client).isHashinatorInitialized()) {
            Thread.sleep(1000);
            System.out.println("Waiting for hashinator to be initialized...");
        }

        String[] myData = {
                "\"1\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "2,2,2,2,a word,1.10,1.11,7777-12-25 14:35:26,POINT(2 2),\"POLYGON((0 0, 2 0, 0 2, 0 0))\"\n",
                "3,3,3,3,a word,1.10,1.11,7777-12-25 14:35:26,POINT(3 3),\"POLYGON((0 0, 3 0, 0 3, 0 0))\"\n",
                "\"4\",\"1\",\"1\",\"1\",\"a word\",\"1.10\",\"1.11\",\"7777-12-25 14:35:26\",\"POINT(1 1)\",\"POLYGON((0 0, 1 0, 0 1, 0 0))\"\n",
                "5,\"5\",\"5\",\"5\",,,,,,\n", "\"5\",5,\"5\",\"5\",,,,,,\n", "\"5\",\"5\",,,,,,,,\n", };

        CountDownLatch latch = new CountDownLatch(1);
        (new SocketDataPusher("localhost", 7001, latch, myData)).start();

        VoltTable ts_table = client.callProcedure("@AdHoc", "SELECT * FROM importCSVTable ORDER BY clm_integer;")
                .getResults()[0];
        assertEquals(2, ts_table.getRowCount());
    }

    static public junit.framework.Test suite() throws Exception {
        Properties formatConfig = new Properties();
        formatConfig.setProperty("nullstring", "test");
        formatConfig.setProperty("separator", ",");
        formatConfig.setProperty("blank", "error");
        formatConfig.setProperty("escape", "\\");
        formatConfig.setProperty("quotechar", "\"");
        formatConfig.setProperty("strictquotes", "true");

        return buildEnv(formatConfig, TestCSVStrictQuoteAndBlankErrorSuite.class);
    }


}
