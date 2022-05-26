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

package org.voltdb;

import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;
import org.voltdb.client.ProcCallException;

public class TestAdhocCompilerErrorMessages extends AdhocDDLTestBase
{
    // Expect: [SOURCE:LINE]: DDL Error: MESSAGE...
    static Pattern pat = Pattern.compile("^\\[[^:]+:\\d+\\]: DDL Error: .+$");

    public TestAdhocCompilerErrorMessages()
    {
    }

    @Test
    public void testEng7609CleanDDLErrorMessages() throws Exception
    {
        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            startSystem(config);
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table x y if exists;");
            }
            catch (ProcCallException pce) {
                String message = pce.getLocalizedMessage();
                Matcher m = pat.matcher(message);
                assertTrue(String.format("'%s' mismatch: %s", pat.pattern(), message), m.matches());
                threw = true;
            }
            assertTrue("Expected exception", threw);
        }
        finally {
            teardownSystem();
        }
    }

    @Test
    public void testEmptyMultiStmtProcErrors() throws Exception
    {
        try {
            VoltDB.Configuration config = new VoltDB.Configuration();
            startSystem(config);
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "create procedure dummy as begin   end;");
            }
            catch (ProcCallException pce) {
                String message = pce.getLocalizedMessage();
                assertTrue(message.contains("Cannot create a stored procedure with no statements for procedure: dummy"));
                threw = true;
            }
            assertTrue("Expected exception", threw);
        }
        finally {
            teardownSystem();
        }
    }
}
