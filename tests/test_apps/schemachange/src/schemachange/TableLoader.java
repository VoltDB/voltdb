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

package schemachange;

import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TableHelper;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

class TableLoader {

    static VoltLogger log = new VoltLogger("HOST");

    final VoltTable table;
    final Client client;
    final TableHelper helper;
    final int pkeyColIndex;
    final String deleteCRUD;
    final String insertCRUD;
    final int timeout;

    final AtomicBoolean hadError = new AtomicBoolean(false);
    final SortedSet<Long> outstandingPkeys = Collections.synchronizedSortedSet(new TreeSet<Long>());
    long lastSentPkey = 0;
    final AtomicInteger success_count = new AtomicInteger(0);

    private static String _F(String str, Object... parameters) {
        return String.format(str, parameters);
    }

    TableLoader(Client client, VoltTable t, int timeout, TableHelper helper) {
        this.table = t;
        this.client = client;
        assert(helper != null);
        this.helper = helper;
        this.timeout = timeout;

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
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                log.debug("TableLoader::clientCallback operation failed: " + ((ClientResponseImpl)clientResponse).toJSONString());
            }
            switch (clientResponse.getStatus()) {
            case ClientResponse.SUCCESS:
                // hooray!
                boolean success = outstandingPkeys.remove(pkey);
                assert(success);
                success_count.incrementAndGet();
                break;
            case ClientResponse.CONNECTION_LOST:
            case ClientResponse.CONNECTION_TIMEOUT:
            case ClientResponse.RESPONSE_UNKNOWN:
            case ClientResponse.SERVER_UNAVAILABLE:
                // no need to be verbose, as there might be many messages
                hadError.set(true);
                break;
            case ClientResponse.GRACEFUL_FAILURE:
                // all graceful failures but this one fall through to death
                if (clientResponse.getStatusString().contains("CONSTRAINT VIOLATION")) {
                    log.info("CONSTRAINT VIOLATION: for pkey: " + pkey
                            + " Details: " + ((ClientResponseImpl) clientResponse).toJSONString());
                    break;
                }
            case ClientResponse.UNEXPECTED_FAILURE:
            case ClientResponse.USER_ABORT:
                // should never happen
                log.error("Error in loader callback:");
                log.error(((ClientResponseImpl)clientResponse).toJSONString());
                assert(false);
                System.exit(-1);
            }
        }
    }

    long countKeys(long max) {
        return SchemaChangeUtility.callROProcedureWithRetry(this.client, "@AdHoc", this.timeout,
                String.format("select count(*) from %s where pkey <= %d;",
                        TableHelper.getTableName(table), max)).getResults()[0].asScalarLong();
    }

    long safeStartPkey(long min, long max) {
        long low = min;
        long high = max;

        while (low < high) {
            long mid = (high + low) / 2;
            long count = countKeys(mid);
            if (count == mid) {
                low = mid + 1;
            }
            else {
                high = mid;
            }
        }

        return Math.min(low, max);
    }

    void load(long startPkey, long stopPkey) {
        while (!loadChunk(startPkey, stopPkey)) {
            startPkey = safeStartPkey(startPkey, stopPkey);
        }
    }

    private boolean loadChunk(long startPkey, long stopPkey) {
        assert(startPkey >= 0);
        assert(stopPkey >= 0);

        if (startPkey >= stopPkey) {
            return true;
        }

        outstandingPkeys.clear();

        log.info(_F("loadChunk | startPkey:%d stopPkey:%d", startPkey, stopPkey));

        TableHelper.RandomRowMaker filler = helper.createRandomRowMaker(table, Integer.MAX_VALUE, false, true);
        long maxSentPkey = -1;
        hadError.set(false);
        for (long key = startPkey; key <= stopPkey; key++) {
            if (hadError.get()) {
                log.info("loadChunk exiting (failed) due to callback error");
                return false;
            }

            Object[] row = filler.randomRow();
            row[pkeyColIndex] = key;
            try {
                outstandingPkeys.add(key);
                maxSentPkey = key;
                client.callProcedure(new Callback(key), insertCRUD, row);
            }
            catch (Exception e) {
                log.info("loadChunk exiting (failed) due to thrown exception: " + e.getMessage());
                return false;
            }
            // periodically print a progress confirmation
            int sc = success_count.get();
            if (sc > 0 && sc % 100 == 0)
                log.info(_F("loadChunk progress report: ops count: %d last key: %d", sc, key));
        }

        try {
            client.drain();
        }
        catch (Exception e) {
            log.info("loadChunk exiting (failed) due to thrown exception during drain: " + e.getMessage());
            return false;
        }

        if ((outstandingPkeys.size() == 0) && (maxSentPkey == stopPkey)) {
            return true;
        }
        else {
            log.info(_F("loadChunk exiting (failed) due to thrown condition %d, %d, %d",
                    outstandingPkeys.size(), maxSentPkey, stopPkey));
            return false;
        }
    }
}
