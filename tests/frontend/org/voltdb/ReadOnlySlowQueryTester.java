/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.IOException;

import junit.framework.TestCase;

import org.voltcore.utils.PortGenerator;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

// Copied from AdHocQueryTester
public abstract class ReadOnlySlowQueryTester extends AdHocQueryTester {


    public abstract void runQueryTest(String query, int expected, boolean validateResult)
            throws IOException, NoConnectionsException, ProcCallException;

    /**
     * @throws ProcCallException
     * @throws IOException
     * @throws NoConnectionsException
     */
    protected void runAllReadOnlySlowSPtests(int hashableA, int hashableB, int hashableC, int hashableD) throws NoConnectionsException, IOException, ProcCallException {
        runQueryTest(String.format("SELECT * FROM PARTED1 WHERE PARTVAL = %d;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM PARTED1 WHERE PARTVAL = %d;", hashableB), 1, true);
        runQueryTest("SELECT * FROM REPPED1;", 2, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 WHERE REPPEDVAL = %d;", hashableA), 1, true);
        runQueryTest("SELECT * FROM V_REPPED1;", 2, true);
        runQueryTest(String.format("SELECT * FROM V_REPPED1 WHERE REPPEDVAL = %d;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL = %d;", hashableA, hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL = %d;", hashableA, hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.PARTVAL;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and A.REPPEDVAL = B.REPPEDVAL;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = %d and B.PARTVAL = A.REPPEDVAL;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = %d and B.REPPEDVAL = A.REPPEDVAL;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = %d and A.REPPEDVAL = B.PARTVAL;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = %d and A.REPPEDVAL = B.REPPEDVAL;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = %d and B.PARTVAL = A.REPPEDVAL;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = %d and B.REPPEDVAL = A.REPPEDVAL;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and A.REPPEDVAL = %d;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and A.REPPEDVAL = %d;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE A.REPPEDVAL = B.PARTVAL and B.PARTVAL = %d;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE A.REPPEDVAL = B.REPPEDVAL and B.REPPEDVAL = %d;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and A.REPPEDVAL = %d;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and A.REPPEDVAL = %d;", hashableA), 1, true);

        runQueryTest(String.format("SELECT * FROM REPPED1 A, PARTED2 B WHERE B.PARTVAL = A.REPPEDVAL and B.PARTVAL = %d;", hashableA), 1, true);
        runQueryTest(String.format("SELECT * FROM REPPED1 A, REPPED2 B WHERE B.REPPEDVAL = A.REPPEDVAL and B.REPPEDVAL = %d;", hashableA), 1, true);


    }


}
