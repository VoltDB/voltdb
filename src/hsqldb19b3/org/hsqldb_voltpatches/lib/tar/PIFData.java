/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.lib.tar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pax Interchange Format object constituted from an Input Stream.
 * <P/>
 * Right now, the only Pax property that we support directly is "size".
 */
public class PIFData extends HashMap {

    private static Pattern pifRecordPattern =
        Pattern.compile("\\d+ +([^=]+)=(.*)");

    /**
     * N.b. this is nothing to do with HashMap.size() or Map.size().
     * This returns the value of the Pax "size" property.
     */
    public Long getSize() {
        return sizeObject;
    }

    private Long sizeObject = null;

    public PIFData(InputStream stream)
    throws TarMalformatException, IOException {

        try {
            BufferedReader br =
                new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String  s, k, v;
            Matcher m;
            int     lineNum = 0;

            /*
             * Pax spec does not allow for blank lines, ignored white space,
             * nor comments of any type, in the file.
             */
            while ((s = br.readLine()) != null) {
                lineNum++;

                m = pifRecordPattern.matcher(s);

                if (!m.matches()) {
                    throw new TarMalformatException(
                        RB.singleton.getString(RB.PIF_MALFORMAT, lineNum, s));
                }

                k = m.group(1);
                v = m.group(2);

                if (v == null || v.length() < 1) {
                    remove(k);
                } else {
                    put(k, v);
                }
            }
        } finally {
            stream.close();
        }

        String sizeString = (String) get("size");

        if (sizeString != null) {
            try {
                sizeObject = Long.valueOf(sizeString);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                    RB.singleton.getString(RB.PIF_MALFORMAT_SIZE, sizeString));
            }
        }
    }
}
