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


package org.hsqldb_voltpatches.lib;

import java.io.LineNumberReader;

import org.hsqldb_voltpatches.store.ValuePool;

/**
 * Uses a LineNumberReader and returns multiple consecutive lines which conform
 * to the specified group demarcation characteristics. Any IOException
 * thrown while reading from the reader is handled internally.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class LineGroupReader {

    private final static String[] defaultContinuations = new String[] {
        " ", "*"
    };
    private final static String[] defaultIgnoredStarts = new String[]{ "--" };
    static final String LS = System.getProperty("line.separator", "\n");

    //
    LineNumberReader reader;
    String           nextStartLine       = null;
    int              startLineNumber     = 0;
    int              nextStartLineNumber = 0;

    //
    final String[] sectionContinuations;
    final String[] sectionStarts;
    final String[] ignoredStarts;

    /**
     * Default constructor for TestUtil usage.
     * Sections start at lines beginning with any non-space character.
     * SQL comment lines are ignored.
     */
    public LineGroupReader(LineNumberReader reader) {

        this.sectionContinuations = defaultContinuations;
        this.sectionStarts        = ValuePool.emptyStringArray;
        this.ignoredStarts        = defaultIgnoredStarts;
        this.reader               = reader;

        try {
            getSection();
        } catch (Exception e) {}
    }

    /**
     * Constructor for sections starting with specified strings.
     */
    public LineGroupReader(LineNumberReader reader, String[] sectionStarts) {

        this.sectionStarts        = sectionStarts;
        this.sectionContinuations = ValuePool.emptyStringArray;
        this.ignoredStarts        = ValuePool.emptyStringArray;
        this.reader               = reader;

        try {
            getSection();
        } catch (Exception e) {}
    }

    public HsqlArrayList getSection() {

        String        line;
        HsqlArrayList list = new HsqlArrayList();

        if (nextStartLine != null) {
            list.add(nextStartLine);

            startLineNumber = nextStartLineNumber;
        }

        while (true) {
            boolean newSection = false;

            line = null;

            try {
                line = reader.readLine();
            } catch (Exception e) {}

            if (line == null) {
                nextStartLine = null;

                return list;
            }

            line = line.substring(
                0, org.hsqldb_voltpatches.lib.StringUtil.rightTrimSize(line));

            //if the line is blank or a comment, then ignore it
            if (line.length() == 0 || isIgnoredLine(line)) {
                continue;
            }

            if (isNewSectionLine(line)) {
                newSection = true;
            }

            if (newSection) {
                nextStartLine       = line;
                nextStartLineNumber = reader.getLineNumber();

                return list;
            }

            list.add(line);
        }
    }

    /**
     * Returns a map/list which contains the first line of each line group
     * as key and the rest of the lines as a String value.
     */
    public HashMappedList getAsMap() {

        HashMappedList map = new HashMappedList();

        while (true) {
            HsqlArrayList list = getSection();

            if (list.size() < 1) {
                break;
            }

            String key   = (String) list.get(0);
            String value = LineGroupReader.convertToString(list, 1);

            map.put(key, value);
        }

        return map;
    }

    private boolean isNewSectionLine(String line) {

        if (sectionStarts.length == 0) {
            for (int i = 0; i < sectionContinuations.length; i++) {
                if (line.startsWith(sectionContinuations[i])) {
                    return false;
                }
            }

            return true;
        } else {
            for (int i = 0; i < sectionStarts.length; i++) {
                if (line.startsWith(sectionStarts[i])) {
                    return true;
                }
            }

            return false;
        }
    }

    private boolean isIgnoredLine(String line) {

        for (int i = 0; i < ignoredStarts.length; i++) {
            if (line.startsWith(ignoredStarts[i])) {
                return true;
            }
        }

        return false;
    }

    public int getStartLineNumber() {
        return startLineNumber;
    }

    public void close() {
        try {
            reader.close();
        } catch (Exception e) {}
    }

    public static String convertToString(HsqlArrayList list, int offset) {

        StringBuffer sb = new StringBuffer();

        for (int i = offset; i < list.size(); i++) {
            sb.append(list.get(i)).append(LS);
        }

        return sb.toString();
    }
}
