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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

/**
 * Encapsulates Pax Interchange Format key/value pairs.
 */
public class PIFGenerator extends ByteArrayOutputStream {

    OutputStreamWriter writer;
    String             name;
    int                fakePid;    // Only used by contructors
    char               typeFlag;

    public String getName() {
        return name;
    }

    protected PIFGenerator() {

        try {
            writer = new OutputStreamWriter(this, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(
                "Serious problem.  JVM can't encode UTF-8", uee);
        }

        fakePid = (int) (new java.util.Date().getTime() % 100000L);

        // Java doesn't have access to PIDs, as PIF wants in the "name" field,
        // so we emulate one in a way that is easy for us.
    }

    /**
     * Construct a PIFGenerator object for a 'g' record.
     *
     * @param sequenceNum  Index starts at 1 in each Tar file
     */
    public PIFGenerator(int sequenceNum) {

        this();

        if (sequenceNum < 1) {

            // No need to localize.  Would be caught at dev-time.
            throw new IllegalArgumentException("Sequence numbers start at 1");
        }

        typeFlag = 'g';
        name = System.getProperty("java.io.tmpdir") + "/GlobalHead." + fakePid
               + '.' + sequenceNum;
    }

    /**
     * Construct a PIFGenerator object for a 'x' record.
     *
     * @param file Target file of the x record.
     */
    public PIFGenerator(File file) {

        this();

        typeFlag = 'x';

        String parentPath = (file.getParentFile() == null) ? "."
                                                           : file.getParentFile()
                                                               .getPath();

        name = parentPath + "/PaxHeaders." + fakePid + '/' + file.getName();
    }

    /**
     * Convenience wrapper for addRecord(String, String).
     * N.b. this writes values exactly as either "true" or "false".
     *
     * @see #addRecord(String, String)
     * @see Boolean#toString(boolean)
     */
    public void addRecord(String key,
                          boolean b)
                          throws TarMalformatException, IOException {
        addRecord(key, Boolean.toString(b));
    }

    /**
     * Convenience wrapper for addRecord(String, String).
     *
     * @see #addRecord(String, String)
     */
    public void addRecord(String key,
                          int i) throws TarMalformatException, IOException {
        addRecord(key, Integer.toString(i));
    }

    /**
     * Convenience wrapper for addRecord(String, String).
     *
     * @see #addRecord(String, String)
     */
    public void addRecord(String key,
                          long l) throws TarMalformatException, IOException {
        addRecord(key, Long.toString(l));
    }

    /**
     * I guess the "initial length" field is supposed to be in units of
     * characters, not bytes?
     */
    public void addRecord(String key,
                          String value)
                          throws TarMalformatException, IOException {

        if (key == null || value == null || key.length() < 1
                || value.length() < 1) {
            throw new TarMalformatException(
                RB.singleton.getString(RB.ZERO_WRITE));
        }

        int lenWithoutIlen = key.length() + value.length() + 3;

        // "Ilen" means Initial Length field.  +3 = SPACE + = + \n
        int lenW = 0;    // lenW = Length With initial-length-field

        if (lenWithoutIlen < 8) {
            lenW = lenWithoutIlen + 1;    // Takes just 1 char to report total
        } else if (lenWithoutIlen < 97) {
            lenW = lenWithoutIlen + 2;    // Takes 2 chars to report this total
        } else if (lenWithoutIlen < 996) {
            lenW = lenWithoutIlen + 3;    // Takes 3...
        } else if (lenWithoutIlen < 9995) {
            lenW = lenWithoutIlen + 4;    // ditto
        } else if (lenWithoutIlen < 99994) {
            lenW = lenWithoutIlen + 5;
        } else {
            throw new TarMalformatException(
                RB.singleton.getString(RB.PIF_TOOBIG, 99991));
        }

        writer.write(Integer.toString(lenW));
        writer.write(' ');
        writer.write(key);
        writer.write('=');
        writer.write(value);
        writer.write('\n');
        writer.flush();    // Does this do anything with a BAOS?
    }
}
