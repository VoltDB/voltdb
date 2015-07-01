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

package org.voltdb;

import org.junit.Test;
import org.voltdb.utils.MiscUtils;


import junit.framework.TestCase;

public class TestBuildString extends TestCase {

        @Test
        public void testParseGitString() {
                String buildstr = MiscUtils.parseRevisionString("voltdb-2.0-70-gb39f43e");
                assertEquals("Parsed git revision string does not match","70-gb39f43e",buildstr);
        }

        @Test
        public void testParseDirtyGitString() {
                String buildstr = MiscUtils.parseRevisionString("this is some tag-70-gb39f43e-more-qualifiers-were-added");
                assertEquals("Parsed git revision string does not match","70-gb39f43e-more-qualifiers-were-added",buildstr);
        }

        @Test
        public void testParseBadGitString() {
                String buildstr = MiscUtils.parseRevisionString("this is some tag-gb39f43e-more-qualifiers-were-added");
                assertNull(buildstr);
        }
        @Test
        public void testParseSvnString() {
                String buildstr = MiscUtils.parseRevisionString("https://svn.voltdb.com/eng/trunk?revision=2352");
                assertEquals("Parsed revision string does not match","2352",buildstr);
        }
        @Test
        public void testParseDirtySvnString() {
                String buildstr = MiscUtils.parseRevisionString("all-sorts-of-branch-info?revision=12-qualifiers");
                assertEquals("Parsed revision string does not match","12-qualifiers",buildstr);
        }

        @Test
            public void testParseBadSvnString() {
            String buildstr = MiscUtils.parseRevisionString("all-sorts-of-branch-info?revision=");
                assertNull(buildstr);
        }
        @Test
        public void testParseNullString() {
            String buildstr = MiscUtils.parseRevisionString("");
            assertNull(buildstr);
        }

}
