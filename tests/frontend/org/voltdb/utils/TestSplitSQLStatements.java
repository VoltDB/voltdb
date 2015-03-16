/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.parser.SQLLexer;

public class TestSplitSQLStatements {

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    private void checkSplitter(final String strIn, final String... strsCmp) {
        final List<String> strsOut = SQLLexer.splitStatements(strIn);
        assertEquals(strsCmp.length, strsOut.size());
        for (int i = 0; i < strsCmp.length; ++i) {
            assertEquals(strsCmp[i], strsOut.get(i));
        }
    }

    @Test
    public void testSQLSplitter() {
        checkSplitter("");
        checkSplitter(" ");
        checkSplitter(" ; ");
        checkSplitter("abc", "abc");
        checkSplitter(" ab c ", "ab c");
        checkSplitter(" ab ; c ", "ab", "c");
        checkSplitter(" ab ; c; ", "ab", "c");
        checkSplitter(" a\"b ; c \" ; ", "a\"b ; c \"");
        checkSplitter(" a\"b ; c 'd;ef' \" ; ", "a\"b ; c 'd;ef' \"");
        checkSplitter(" a\"b ; c \\\" 'd;ef' \" ; ", "a\"b ; c \\\" 'd;ef' \"");
        checkSplitter(" a'b ; c \\' \"d;ef\" ' ; ", "a'b ; c \\' \"d;ef\" '");
        checkSplitter("a;;b;;c;;", "a", "b", "c");
        checkSplitter("abc --;def\n;ghi", "abc --;def", "ghi");
        checkSplitter("abc /*\";def\n;*/ghi", "abc /*\";def\n;*/ghi");
        checkSplitter("a\r\nb;c\r\nd;", "a\r\nb", "c\r\nd");
        checkSplitter("--one\n--two\nreal", "--one", "--two", "real");
        checkSplitter("  --one\n  --two\nreal", "--one", "--two", "real");
        checkSplitter("  abc;  --def\n\n  /*ghi\njkl;*/", "abc", "--def", "/*ghi\njkl;*/");
    }

}
