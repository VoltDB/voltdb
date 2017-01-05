/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.catalog;

import java.io.IOException;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;

import junit.framework.*;

public class TestCatalogSerialization extends TestCase {

    public void testSimplest() throws IOException
    {
        Catalog catalog1 = TPCCProjectBuilder.getTPCCSchemaCatalog();

        String commands = catalog1.serialize();
        // System.out.println(commands);

        Catalog catalog2 = new Catalog();
        try {
            catalog2.execute(commands);
        } catch (Exception e) {
            e.printStackTrace();
        }
        String commands2 = catalog2.serialize();

        Catalog catalog3 = catalog2.deepCopy();
        String commands3 = catalog3.serialize();

        assertTrue(commands.equals(commands2));
        assertTrue(commands.equals(commands3));

        assertTrue(catalog1.equals(catalog2));
        assertTrue(catalog1.equals(catalog3));
    }
}
