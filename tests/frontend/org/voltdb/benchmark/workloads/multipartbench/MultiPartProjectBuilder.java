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

package org.voltdb.benchmark.workloads.multipartbench;

import java.net.URL;

import org.voltdb.benchmark.workloads.WorkloadProjectBuilder;
import org.voltdb.benchmark.workloads.multipartbench.procedures.*;

public class MultiPartProjectBuilder extends WorkloadProjectBuilder
{
    public static final Class<?> m_procedures[] = new Class<?>[]
    {
        CreditDebitAccounts.class,
        EveryoneWins.class,
        InsertAccount.class,
        NullMultiProc.class,
        SelectEmptyTable.class,
        TotalBalances.class,
        UpdateBalance.class
    };

    public static final URL m_ddlURL = MultiPartProjectBuilder.class.getResource("MultiPartBench-ddl.sql");
    private static final String m_jarFileName = "catalog.jar";

    public static String m_partitioning[][] = new String[][]
    {
        {"FAKE_ACCOUNTS", "ID"},
        {"EMPTY_TABLE", "ID"}
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
            throw new RuntimeException("Multipart benchmark project builder failed app compilation.");
        }
        return new String[] {m_jarFileName};
    }

    @Override
    public void addAllDefaults()
    {
        addProcedures(m_procedures);
        for (String partitionInfo[] : m_partitioning) {
            addPartitionInfo(partitionInfo[0], partitionInfo[1]);
        }
        addSchema(m_ddlURL);
    }

    @Override
    public Class<?>[] getProcedures()
    {
        return m_procedures;
    }
}
