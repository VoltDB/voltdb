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

package org.voltdb_testprocs.regressionsuites.failureprocs;

import org.voltdb.*;

@ProcInfo (
    singlePartition = false
)
public class ReturnAppStatus extends VoltProcedure {

    public VoltTable run(
            int behavior,//0 don't set anything 1, set both, 2, set app status code, 3 set status code and string, 4 set them both and abort
            String statusString, byte statusCode) {
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo( "a", VoltType.BIGINT));
        switch (behavior) {
        case 0:
            break;
        case 1:
            result.setStatusCode(statusCode);
            setAppStatusCode(statusCode);
            setAppStatusString(statusString);
            break;
        case 2:
            result.setStatusCode(statusCode);
            setAppStatusCode(statusCode);
            break;
        case 3:
            result.setStatusCode(statusCode);
            setAppStatusString(statusString);
            break;
        case 4:
            result.setStatusCode(statusCode);
            setAppStatusCode(statusCode);
            setAppStatusString(statusString);
            throw new VoltAbortException();
        default:
                throw new VoltAbortException("Invalid behavior value " + behavior);
        }

        return result;
    }
}
