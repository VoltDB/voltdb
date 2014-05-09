/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import static org.junit.Assert.*;

import org.junit.Test;

public class TestSQLLexer {

    private void checkDDL(final String strIn, final String expectString)
    {
        String result = SQLLexer.extractDDLToken(strIn);
        assertEquals(expectString, result);
    }

    @Test
    public void testDDLLexer()
    {
        checkDDL("", null);
        checkDDL("ryan loves the yankees", null);

        checkDDL("drop table pants", "drop");
        checkDDL("DROP table pants", "drop");
        checkDDL("dRoP table pants", "drop");
        checkDDL("   drop table pants    ", "drop");
        checkDDL("droptable pants", null);
        checkDDL("-- drop table pants", null);
        checkDDL("droop table pants", null);

        checkDDL("create table pants", "create");
        checkDDL("CREATE table pants", "create");
        checkDDL("CrEaTe table pants", "create");
        checkDDL("   create table pants    ", "create");
        checkDDL("createtable pants", null);
        checkDDL("-- create table pants", null);
        checkDDL("crate table pants", null);

        checkDDL("alter table pants", "alter");
        checkDDL("ALTER table pants", "alter");
        checkDDL("ALteR table pants", "alter");
        checkDDL("   alter table pants    ", "alter");
        checkDDL("altertable pants", null);
        checkDDL("-- alter table pants", null);
        checkDDL("altar table pants", null);
    }
}
