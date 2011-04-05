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

import junit.framework.TestCase;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.exportclient.ExportToFileClient.ExportToFileDecoder;
import org.voltdb.utils.DelimitedDataWriterUtil.CSVWriter;

public class TestExportToFileClient extends TestCase {

    public void testEng1088() throws Exception {
        ExportToFileClient exportClient =
            new ExportToFileClient(
                new CSVWriter(),
                "testnonce",
                new File("/tmp/" + System.getProperty("user.name")),
                60,
                "yyyyMMddHHmmss",
                0,
                false);
        AdvertisedDataSource source0 = TestExportDecoderBase.constructTestSource(0);
        AdvertisedDataSource source1 = TestExportDecoderBase.constructTestSource(1);
        ExportToFileDecoder decoder0 = exportClient.constructExportDecoder(source0);
        ExportToFileDecoder decoder1 = exportClient.constructExportDecoder(source1);
        assertEquals(decoder0, decoder1);
        decoder0.sourceNoLongerAdvertised(source1);
        decoder0.sourceNoLongerAdvertised(source0);
    }
}
