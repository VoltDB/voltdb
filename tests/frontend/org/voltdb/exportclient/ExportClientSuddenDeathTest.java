/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import java.io.File;
import java.net.InetSocketAddress;

import junit.framework.TestCase;

import org.voltdb.VoltDB;

public class ExportClientSuddenDeathTest extends TestCase {

    public void testDeath() throws ExportClientException {
        Thread t = new Thread() {
            @Override
            public void run() {
                MockExportSource.run(1, 1, 0);
            }
        };
        t.start();
        ExportToFileClient etfc = new ExportToFileClient(',',
                                                         "N",
                                                         new File("."),
                                                         1,
                                                         "yyyyMMddHHmmss",
                                                         null,
                                                         0,
                                                         false,
                                                         false,
                                                         false,
                                                         0);

        InetSocketAddress addr = new InetSocketAddress("127.0.0.1", VoltDB.DEFAULT_PORT);
        etfc.addServerInfo(addr);
        etfc.run();
    }

}
