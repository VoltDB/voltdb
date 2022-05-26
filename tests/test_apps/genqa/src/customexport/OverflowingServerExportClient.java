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
package customexport;

import java.util.Properties;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.ExportClientBase;
import org.voltdb.exportclient.ExportDecoderBase;
import org.voltdb.exportclient.ExportRow;

public class OverflowingServerExportClient extends ExportClientBase {

    public class ExportCounterDecoder extends ExportDecoderBase {

        public ExportCounterDecoder(AdvertisedDataSource source) {
            super(source);
        }

        @Override
        public boolean processRow(ExportRow row) throws RestartBlockException {
            String drain = System.getProperty("drain", "false");
            if (drain.equalsIgnoreCase("true")) {
                return true;
            }
            throw new RestartBlockException(true);
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
        }
    }

    @Override
    public void configure(Properties config) throws Exception {
        String drain = config.getProperty("drain");
        if (drain == null) {
            drain = System.getProperty("drain", "false");
        }
        System.out.println("Custom Export client is configured: " + this.getClass().getName() + " Drain is: " + drain);
    }


    @Override
    public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
        return new ExportCounterDecoder(source);
    }
}
