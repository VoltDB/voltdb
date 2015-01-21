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

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ProcCallException;

public class TestAdhocDropCreateTable extends AdhocDDLTestBase
{
    public void testDropCreateTableError() throws Exception {
        Configuration config = configurationForTest("create table FOO (ID integer);", 2);
        try {
            startSystem(config);

            // separate commands (succeed)
            assertTrue(findTableInSystemCatalogResults("FOO"));
            try {
                m_client.callProcedure("@AdHoc", "drop table FOO;");
                m_client.callProcedure("@AdHoc", "create table FOO (ID integer);");
            }
            catch (ProcCallException pce) {
                pce.printStackTrace();
                fail("Separate DROP/CREATE commands should have worked.");
            }

            // batch with conflicting DROP/CREATE for same table (fail)
            assertTrue(findTableInSystemCatalogResults("FOO"));
            boolean threw = false;
            try {
                m_client.callProcedure("@AdHoc", "drop table FOO; create table FOO (ID integer);");
            }
            catch (ProcCallException pce) {
                threw = true;
                boolean correct = pce.getMessage().contains("contains both DROP and CREATE");
                assertTrue("Wrong error received for DROP/CREATE batch.", correct);
            }
            assertTrue("DROP/CREATE batch didn't fail.", threw);

            // batch with non-conflicting DROP/CREATE for different tables (succeed)
            assertTrue(findTableInSystemCatalogResults("FOO"));
            assertFalse(findTableInSystemCatalogResults("FO"));
            try {
                m_client.callProcedure("@AdHoc", "drop table FOO; create table FO (NAME varchar(20));");
            }
            catch (ProcCallException pce) {
                fail("Non-conflicting DROP/CREATE commands should have worked.");
            }
            assertFalse(findTableInSystemCatalogResults("FOO"));
            assertTrue(findTableInSystemCatalogResults("FO"));
        }
        finally {
            teardownSystem();
        }
    }
}
