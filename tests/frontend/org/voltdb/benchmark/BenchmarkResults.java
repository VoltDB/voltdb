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

package org.voltdb.benchmark;

import java.util.*;
import java.util.Map.Entry;

class BenchmarkResults {

    public static class Error {
        public Error(String clientName, String message, int pollIndex) {
            this.clientName = clientName;
            this.message = message;
            this.pollIndex = pollIndex;
        }
        public final String clientName;
        public final String message;
        public final int pollIndex;
    }

    public static class Result {
        public Result(long benchmarkTimeDelta, long transactionCount) {
            this.benchmarkTimeDelta = benchmarkTimeDelta;
            this.transactionCount = transactionCount;
        }
        public final long benchmarkTimeDelta;
        public final long transactionCount;
    }

    private final HashMap<String, HashMap<String, ArrayList<Result>>> m_data =
        new HashMap<String, HashMap<String, ArrayList<Result>>>();
    private final Set<Error> m_errors = new HashSet<Error>();

    private final long m_durationInMillis;
    private final long m_pollIntervalInMillis;
    private final int m_clientCount;

    // cached data for performance and consistency
    private final Set<String> m_transactionNames = new HashSet<String>();

    BenchmarkResults(long pollIntervalInMillis, long durationInMillis, int clientCount) {
        assert((durationInMillis % pollIntervalInMillis) == 0) : "duration does not comprise an integral number of polling intervals.";

        m_durationInMillis = durationInMillis;
        m_pollIntervalInMillis = pollIntervalInMillis;
        m_clientCount = clientCount;
    }

    public Set<Error> getAnyErrors() {
        if (m_errors.size() == 0)
            return null;
        Set<Error> retval = new TreeSet<Error>();
        for (Error e : m_errors)
            retval.add(e);
        return retval;
    }

    public int getCompletedIntervalCount() {
        // make sure all
        if (m_data.size() < m_clientCount)
            return 0;
        assert(m_data.size() == m_clientCount);

        int min = Integer.MAX_VALUE;
        String txnName = m_transactionNames.iterator().next();
        for (HashMap<String, ArrayList<Result>> txnResults : m_data.values()) {
            ArrayList<Result> results = txnResults.get(txnName);
            if (results.size() < min)
                min = results.size();
        }

        return min;
    }

    public long getIntervalDuration() {
        return m_pollIntervalInMillis;
    }

    public long getTotalDuration() {
        return m_durationInMillis;
    }


    public Set<String> getTransactionNames() {
        Set<String> retval = new TreeSet<String>();
        retval.addAll(m_transactionNames);
        return retval;
    }

    public Set<String> getClientNames() {
        Set<String> retval = new HashSet<String>();
        retval.addAll(m_data.keySet());
        return retval;
    }

    public Result[] getResultsForClientAndTransaction(String clientName, String transactionName) {
        HashMap<String, ArrayList<Result>> txnResults = m_data.get(clientName);
        ArrayList<Result> results = txnResults.get(transactionName);
        int intervals = getCompletedIntervalCount();
        Result[] retval = new Result[intervals];

        long txnsTillNow = 0;
        for (int i = 0; i < intervals; i++) {
            Result r = results.get(i);
            retval[i] = new Result(r.benchmarkTimeDelta, r.transactionCount - txnsTillNow);
            txnsTillNow = r.transactionCount;
        }
        return retval;
    }

    void setPollResponseInfo(String clientName, int pollIndex, long time, Map<String, Long> transactionCounts, String errMsg) {
        long benchmarkTime = pollIndex * m_pollIntervalInMillis;
        long offsetTime = time - benchmarkTime;

        if (errMsg != null) {
            Error err = new Error(clientName, errMsg, pollIndex);
            m_errors.add(err);
        }
        else {
            // put the transactions names:
            for (String txnName : transactionCounts.keySet())
                m_transactionNames.add(txnName);

            // ensure there is an entry for the client
            HashMap<String, ArrayList<Result>> txnResults = m_data.get(clientName);
            if (txnResults == null) {
                txnResults = new HashMap<String, ArrayList<Result>>();
                for (String txnName : transactionCounts.keySet())
                    txnResults.put(txnName, new ArrayList<Result>());
                m_data.put(clientName, txnResults);
            }

            for (Entry<String, Long> entry : transactionCounts.entrySet()) {
                Result r = new Result(offsetTime, entry.getValue());
                ArrayList<Result> results = m_data.get(clientName).get(entry.getKey());
                assert(results != null);
                assert (results.size() == pollIndex);
                results.add(r);
            }
        }
    }

    BenchmarkResults copy() {
        BenchmarkResults retval = new BenchmarkResults(m_pollIntervalInMillis, m_durationInMillis, m_clientCount);

        retval.m_errors.addAll(m_errors);
        retval.m_transactionNames.addAll(m_transactionNames);

        for (Entry<String, HashMap<String, ArrayList<Result>>> entry : m_data.entrySet()) {
            HashMap<String, ArrayList<Result>> txnsForClient =
                new HashMap<String, ArrayList<Result>>();

            for (Entry <String, ArrayList<Result>> entry2 : entry.getValue().entrySet()) {
                ArrayList<Result> newResults = new ArrayList<Result>();
                for (Result r : entry2.getValue())
                    newResults.add(r);
                txnsForClient.put(entry2.getKey(), newResults);
            }

            retval.m_data.put(entry.getKey(), txnsForClient);
        }

        return retval;
    }

    public String toString() {
        // TODO Auto-generated method stub
        return super.toString();
    }
}
