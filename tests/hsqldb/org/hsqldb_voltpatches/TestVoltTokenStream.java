/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.hsqldb_voltpatches;

import org.voltcore.utils.Pair;

import junit.framework.TestCase;
import static org.hsqldb_voltpatches.VoltToken.Kind.*;

public class TestVoltTokenStream extends TestCase {

    @SafeVarargs
    private static void assertStreamProducesTokens(String sqlText, Pair<VoltToken.Kind, String>... pairs) {
        VoltTokenStream stream = new VoltTokenStream(sqlText);

        int i = 0;
        for (VoltToken vt : stream) {
            assertTrue("token stream longer than expected. Next token was " + vt,
                    i < pairs.length);
            assertEquals("token kind at index " + i, pairs[i].getFirst(), vt.kind());
            assertEquals("token text at index " + i, pairs[i].getSecond(), vt.toSqlText());
            ++i;
        }

        assertEquals("token stream shorter than expected", pairs.length, i);
    }

    private static void assertThrowsParseException(String input) {
        try {
            VoltTokenStream vts = new VoltTokenStream(input);
            VoltTokenIterator it = vts.iterator();
            while (it.hasNext()) {
                it.next();
            }
        }
        catch (VoltTokenIterator.ParseException pe) {
            return;
        }

        fail("Failed to catch expected ParseException for input \"" + input + "\"");
    }

    public void testComments() {

        // Skips single line comments
        assertStreamProducesTokens("SELECT -- foo\n FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // C-style multi-line comments too
        assertStreamProducesTokens("SELECT /* foo \n bar */ FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // single quote in comment
        assertStreamProducesTokens("SELECT /* foo \n bar' */ FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // double quote in comment
        assertStreamProducesTokens("SELECT /* foo \n bar\" */ FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // semicolon in comment
        assertStreamProducesTokens("SELECT /* foo \n bar; */ FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // single quote in comment
        assertStreamProducesTokens("SELECT -- ' \n FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // double quote in comment
        assertStreamProducesTokens("SELECT -- \" \n FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // semicolon in comment
        assertStreamProducesTokens("SELECT -- ; \n FROM",
                Pair.of(SELECT, "SELECT"),
                Pair.of(FROM, "FROM"));

        // C++ style // comments
        // This doesn't work: Scanner.scanNext doesn't seem to advance
        // token position, so hits an infinite loop.  Seems to be an HSQL bug.
        // What happens in @AdHoc?
        //                assertStreamProducesTokens("SELECT // foo \n FROM",
        //                        Pair.of(SELECT, "SELECT"),
        //                        Pair.of(X_REMARK, " foo "),
        //                        Pair.of(FROM, "FROM"));
    }

    public void testStrings() {

        // empty string
        assertStreamProducesTokens("''",
                Pair.of(X_VALUE, "''"));

        // normal string
        assertStreamProducesTokens("'foo'",
                Pair.of(X_VALUE, "'foo'"));

        // embedded escaped single quote
        assertStreamProducesTokens("'Let''s Go!'",
                Pair.of(X_VALUE, "'Let''s Go!'"));

        // embedded double quote
        assertStreamProducesTokens("'Let\"s Go!'",
                Pair.of(X_VALUE, "'Let\"s Go!'"));

        // Apparently, newlines within string literals work okay.
        assertStreamProducesTokens("'Let''s \nGo!'",
                Pair.of(X_VALUE, "'Let''s \nGo!'"));

        // unterminated string.
        assertThrowsParseException("'Foo");

        // Missing first single quote.
        assertThrowsParseException("Foo'");

        // Just a single quote.
        assertThrowsParseException("'");
    }

    public void testIdentifiers() {

        assertStreamProducesTokens("C",
                Pair.of(X_IDENTIFIER, "C"));

        assertStreamProducesTokens("SELECT myfield FROM mytable",
                Pair.of(SELECT, "SELECT"),
                Pair.of(X_IDENTIFIER, "MYFIELD"),
                Pair.of(FROM, "FROM"),
                Pair.of(X_IDENTIFIER, "MYTABLE"));
    }
}
