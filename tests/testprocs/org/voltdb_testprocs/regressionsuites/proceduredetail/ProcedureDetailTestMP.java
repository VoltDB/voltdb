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

package org.voltdb_testprocs.regressionsuites.proceduredetail;

import org.voltdb.ProcStatsOption;
import org.voltdb.VoltTable;

/* This Java stored procedure is used to test the PROCEDUREDETAIL selector in @Statistics.
 * It will queue batches based on the parameters you gave to test the behavior of PROCEDUREDETAIL
 * under different scenarios. */

@ProcStatsOption (
    procSamplingInterval = 1,
    stmtSamplingInterval = 1
    )

public class ProcedureDetailTestMP extends ProcedureDetailTestSP {
    // Nothing to change here, I just want to have a separate class that does the same thing as
    // ProcedureDetailTestSP but I can load it as a multi-partition stored procedure.
    @Override
    public VoltTable[] run(int id, String arg) throws VoltAbortException {
        return super.run(id, arg);
    }
}
