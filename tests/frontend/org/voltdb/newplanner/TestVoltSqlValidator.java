/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner;

import org.voltdb.catalog.org.voltdb.calciteadaptor.CatalogAdapter;

public class TestVoltSqlValidator extends VoltSqlValidatorTestCase{

    @Override
    protected void setUp() throws Exception {
        setupSchema(TestVoltSqlValidator.class.getResource(
                "testcalcite-ddl.sql"), "testcalcite", false);
        setupValidator(CatalogAdapter.schemaPlusFromDatabase(getDatabase()));
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testTableColumn() {
        assertValid("select i from R2");
        assertValid("select pk from R3");
        assertValid("select pk from R3 where bad < 0");
        // TODO: the validator exception should able to detect the table name
//        assertExceptionIsThrown("select ^bad^ from R2", "Column 'BAD' not found in table 'R2'");
        assertExceptionIsThrown("select ^bad^ from R2", "Column 'BAD' not found in any table");
        assertExceptionIsThrown("select pk from R3 where ^bad^ < 0", "Column 'BAD' not found in any table");
    }

    public void testTableName() {
        assertValid("select * from R3");
        assertExceptionIsThrown("select * from ^bad^", "Object 'BAD' not found");
    }

}
