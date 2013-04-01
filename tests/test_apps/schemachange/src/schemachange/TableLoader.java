/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package schemachange;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

class TableLoader {

    static VoltLogger log = new VoltLogger("HOST");

    final SchemaChangeClient scc;
    final VoltTable table;
    final Client client;
    final Random rand;
    final int pkeyColIndex;
    final String deleteCRUD;
    final String insertCRUD;

    final AtomicBoolean hadError = new AtomicBoolean(false);
    final SortedSet<Long> outstandingPkeys = Collections.synchronizedSortedSet(new TreeSet<Long>());

    TableLoader(SchemaChangeClient scc, VoltTable t, Random rand) {
        this.scc = scc;
        this.table = t;
        this.client = scc.client;
        this.rand = rand;

        // find the primary key
        pkeyColIndex = TableHelper.getBigintPrimaryKeyIndexIfExists(table);
        assert (pkeyColIndex >= 0);

        // get the CRUD procedure names
        insertCRUD = TableHelper.getTableName(table).toUpperCase() + ".insert";
        deleteCRUD = TableHelper.getTableName(table).toUpperCase() + ".delete";
    }

    class Callback implements ProcedureCallback {

        final long pkey;

        Callback(long pkey) {
            this.pkey = pkey;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            switch (clientResponse.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                boolean success = outstandingPkeys.remove(pkey);
                assert(success);
                break;
            case ClientResponse.CONNECTION_LOST:
            case ClientResponse.CONNECTION_TIMEOUT:
            case ClientResponse.RESPONSE_UNKNOWN:
            case ClientResponse.SERVER_UNAVAILABLE:
                // no need to be verbose, as there might be many messages
                hadError.set(true);
                break;
            case ClientResponse.UNEXPECTED_FAILURE:
            case ClientResponse.GRACEFUL_FAILURE:
            case ClientResponse.USER_ABORT:
                // should never happen
                log.error("Error in loader callback:");
                log.error(((ClientResponseImpl)clientResponse).toJSONString());
                assert(false);
                System.exit(-1);
            }
        }
    }

    void load(long startPkey, long stopPkey, long jump) {
        assert(outstandingPkeys.isEmpty());
        assert(!hadError.get());

        if (startPkey >= stopPkey) return;

        long lastSuccessfullyLoadedKey = -1;

        while (lastSuccessfullyLoadedKey < stopPkey) {
            long nextKey = lastSuccessfullyLoadedKey >= 0 ? lastSuccessfullyLoadedKey : startPkey;
            nextKey = loadChunk(nextKey, stopPkey, jump);
            if (nextKey >= 0) {
                lastSuccessfullyLoadedKey = nextKey;
            }
        }

        hadError.set(false);
        outstandingPkeys.clear();
    }

    void delete(long startPkey, long stopPkey, long jump) {
        assert(outstandingPkeys.isEmpty());
        assert(!hadError.get());

        if (startPkey > stopPkey) return;
        do {
            startPkey = deleteChunk(startPkey, stopPkey, jump) + jump;
        } while (startPkey <= stopPkey);

        hadError.set(false);
        outstandingPkeys.clear();
    }

    private long deleteChunk(long startPkey, long stopPkey, long jump) {
        assert(startPkey < stopPkey);
        assert(startPkey >= 0);

        long nextPkey = startPkey;

        long maxSentPkey = -1;
        while ((nextPkey <= stopPkey) && (!hadError.get())) {
            try {
                outstandingPkeys.add(nextPkey);
                maxSentPkey = nextPkey;
                client.callProcedure(new Callback(nextPkey), deleteCRUD, nextPkey);
            }
            catch (Exception e) {
                break;
            }
            nextPkey += jump;
        }

        try { client.drain(); } catch (Exception e) {}

        long minOutstandingPkey = -1;
        try {
            minOutstandingPkey = outstandingPkeys.first();
        }
        catch (NoSuchElementException e) {
            // we inserted all rows
            assert((maxSentPkey + jump) > stopPkey);
            return stopPkey;
        }
        assert(minOutstandingPkey >= 0);

        // delete any messiness beyond where the errors started
        for (long pkey = minOutstandingPkey; pkey <= maxSentPkey; pkey += jump) {
            long modCount = scc.callROProcedureWithRetry(deleteCRUD, pkey).getResults()[0].asScalarLong();
            assert((modCount >= 0) && (modCount <= 1));
        }

        return maxSentPkey;
    }

    private long loadChunk(long startPkey, long stopPkey, long jump) {
        assert(startPkey < stopPkey);
        assert(startPkey >= 0);

        long nextPkey = startPkey;

        long maxSentPkey = -1;
        while ((nextPkey <= stopPkey) && (!hadError.get())) {
            Object[] row = TableHelper.randomRow(table, Integer.MAX_VALUE, rand);
            row[pkeyColIndex] = nextPkey;
            try {
                outstandingPkeys.add(nextPkey);
                maxSentPkey = nextPkey;
                client.callProcedure(new Callback(nextPkey), insertCRUD, row);
            }
            catch (Exception e) {
                break;
            }
            nextPkey += jump;
        }

        try { client.drain(); } catch (Exception e) {}

        long minOutstandingPkey = -1;
        try {
            minOutstandingPkey = outstandingPkeys.first();
        }
        catch (NoSuchElementException e) {
            // we inserted all rows
            assert((maxSentPkey + jump) > stopPkey);
            return stopPkey;
        }
        assert(minOutstandingPkey >= 0);

        // delete any messiness beyond where the errors started
        for (long pkey = minOutstandingPkey; pkey <= maxSentPkey; pkey += jump) {
            long modCount = scc.callROProcedureWithRetry(deleteCRUD, pkey).getResults()[0].asScalarLong();
            assert((modCount >= 0) && (modCount <= 1));
        }

        return Math.max(minOutstandingPkey - jump, -1);
    }
}
