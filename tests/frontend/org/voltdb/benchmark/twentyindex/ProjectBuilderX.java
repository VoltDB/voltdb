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

package org.voltdb.benchmark.twentyindex;

import java.net.URL;

import org.voltdb.benchmark.twentyindex.procedures.Insert;
import org.voltdb.compiler.VoltProjectBuilder;

public class ProjectBuilderX extends VoltProjectBuilder {

    public static final Class<?> m_procedures[] = new Class<?>[] {
        Insert.class,
        };

    public static final Class<?> m_supplementalClasses[] = new Class<?>[] {
        ClientBenchmark.class,
        ProjectBuilderX.class
    };

    public static final URL m_ddlURL = ProjectBuilderX.class.getResource("ddl.sql");
    private static final String m_jarFileName = "20index.jar";

    public static String m_partitioning[][] = new String[][] {
        {"TABLE1", "MAINID"},
        {"TABLE2", "MAINID"},
        {"TABLE3", "MAINID"},
    };

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
        addProcedures(m_procedures);
        for (String partitionInfo[] : m_partitioning) {
            addPartitionInfo(partitionInfo[0], partitionInfo[1]);
        }
        addSchema(m_ddlURL);
        addSupplementalClasses(m_supplementalClasses);
    }
}
