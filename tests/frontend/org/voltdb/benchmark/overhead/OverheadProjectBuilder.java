/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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


package org.voltdb.benchmark.overhead;

import org.voltdb.benchmark.overhead.procedures.*;
import org.voltdb.compiler.VoltProjectBuilder;

import java.net.URL;

public class OverheadProjectBuilder extends VoltProjectBuilder {

    private static final URL ddlURL =
        OverheadProjectBuilder.class.getResource("measureoverhead-ddl.sql");

    private static final String m_jarFileName = "measureoverhead.jar";

    @Override
    public String[] compileAllCatalogs(int sitesPerHost,
                                       int length,
                                       int kFactor,
                                       String voltRoot) {
        addAllDefaults();
        boolean compile = compile(m_jarFileName, sitesPerHost,
                                  length, kFactor, voltRoot);
        if (!compile) {
            throw new RuntimeException("Bingo project builder failed app compilation.");
        }
        return new String[] {m_jarFileName};
    }


    @Override
    public void addAllDefaults() {
        addProcedures(measureOverhead.class, measureOverhead42Longs.class,
                measureOverhead42Strings.class, measureOverheadMultipartition.class,
                measureOverheadMultipartition42Strings.class, measureOverheadMultipartitionBatched.class,
                measureOverheadMultipartitionNoFinal.class, measureOverheadMultipartitionTwoStatements.class);
        addSchema(ddlURL);
        addPartitionInfo("NEWORDER", "NO_O_ID");
    }

}
