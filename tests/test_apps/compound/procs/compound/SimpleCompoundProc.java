/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package compound;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltCompoundProcedure;
import org.voltdb.VoltCompoundProcedure.CompoundProcAbortException;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;

// Run two table lookups in parallel, and process results
// in a third procedure. Implementation is asynchronous.
//
// The id generator is a sorry attempt to get unique ids
// in multinode clusters. Don't try this at home.
//
public class SimpleCompoundProc extends VoltCompoundProcedure {

    private static final long startId = (long)(new Random()).nextInt(31) << 32L;
    private static final AtomicLong nextId = new AtomicLong(startId);

    public long run() {
        newStageList(this::getData)
            .then(this::doInsert)
            .then(this::finishUp)
            .build();
        return 0;
    }

    // Execution stages

    private void getData(ClientResponse[] nil) {
        queueProcedureCall("MySpProc", 1);
        queueProcedureCall("MyOtherSpProc", 2);
    }

    private void doInsert(ClientResponse[] resp) {
        String val1 = extractString(resp[0]), val2 = extractString(resp[1]);
        long id = nextId.incrementAndGet();
        queueProcedureCall("MyLastProc", id, val1, val2);
    }

    private void finishUp(ClientResponse[] resp) {
        long count = extractLong(resp[0]);
        if (count != 1)
            throw new CompoundProcAbortException("insert didn't insert");
        completeProcedure(123L);
    }

    // Utility routines

    private static Object extractResult(ClientResponse resp, VoltType type) {
        if (resp.getStatus() != ClientResponse.SUCCESS)
            throw new CompoundProcAbortException(resp.getStatusString());
        VoltTable vt = resp.getResults()[0];
        vt.advanceRow();
        return vt.get(0, type);
    }

    private static String extractString(ClientResponse resp) {
        return (String)extractResult(resp, VoltType.STRING);
    }

    private static long extractLong(ClientResponse resp) {
        return ((Long)extractResult(resp, VoltType.BIGINT)).longValue();
    }
}
