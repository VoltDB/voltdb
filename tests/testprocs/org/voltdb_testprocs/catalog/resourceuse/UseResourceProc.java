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

package org.voltdb_testprocs.catalog.resourceuse;

import java.io.IOException;
import java.net.URL;

import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

public class UseResourceProc extends VoltProcedure {

    public VoltTable run() {

        URL resourceURL = this.getClass().getResource("resource.txt");
        URL missingURL = this.getClass().getResource("missing.txt");
        String resourceContents = null;
        boolean exceptionCaught = false;
        try {
            resourceContents = Resources.toString(missingURL, Charsets.UTF_8);
        } catch (IOException e) {
            exceptionCaught = true;
        }
        if (!exceptionCaught) {
            throw new VoltAbortException("Missing resources should throw a IOException");
        }
        try {
            resourceContents = Resources.toString(resourceURL, Charsets.UTF_8);
        } catch (IOException e) {
            throw new VoltAbortException(e);
        }

        VoltTable t = new VoltTable(new VoltTable.ColumnInfo[] { new VoltTable.ColumnInfo("resource", VoltType.STRING) });
        t.addRow(new Object[] { resourceContents} );
        return t;
    }

}
