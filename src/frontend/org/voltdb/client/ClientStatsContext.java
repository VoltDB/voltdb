/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * <p>An object to store and manipulate statistics information from
 * the VoltDB Java client. Each instance has a set of timestamped
 * baseline statistics and a set of timestamped current statistics.
 * Given these two sets of data, this object can return statistics
 * covering the period between the baseline data and current data.</p>
 *
 * <p>An instance is created using {@link Client#createStatsContext()}.
 * Mutliple instances can coexist, each covering a different time
 * period. See the Voter example in /examples for an example of using
 * one context for long term stats and another for short term updates.</p>
 */
public class ClientStatsContext {

    final Distributer m_distributor;
    Map<Long, Map<String, ClientStats>> m_baseline;
    Map<Long, Map<String, ClientStats>> m_current;
    Map<Long, ClientIOStats> m_baselineIO;
    Map<Long, ClientIOStats> m_currentIO;
    long m_baselineTS;
    long m_currentTS;

    ClientStatsContext(Distributer distributor,
                       Map<Long, Map<String, ClientStats>> current,
                       Map<Long, ClientIOStats> currentIO) {
        m_distributor = distributor;
        m_baseline = new TreeMap<Long, Map<String, ClientStats>>();
        m_baselineIO = new TreeMap<Long, ClientIOStats>();
        m_current = current;
        m_currentIO = currentIO;
        m_baselineTS = m_currentTS = System.currentTimeMillis();
    }

    /**
     * Fetch current statistics from the client internals. Don't
     * update the baseline. This will increase the range covered
     * by any {@link ClientStats} instances returned from this
     * context.
     *
     * @return A <code>this</code> pointer for chaining calls.
     */
    public ClientStatsContext fetch() {
        m_current = m_distributor.getStatsSnapshot();
        m_currentIO = m_distributor.getIOStatsSnapshot();
        m_currentTS = System.currentTimeMillis();
        return this;
    }

    /**
     * Fetch current statistics from the client internals. Update
     * the baseline to be the previously current statistics. This
     * will update the range covered by any {@link ClientStats}
     * instances returned from this context.
     *
     * @return A <code>this</code> pointer for chaining calls.
     */
    public ClientStatsContext fetchAndResetBaseline() {
        m_baseline = m_current;
        m_baselineIO = m_currentIO;
        m_baselineTS = m_currentTS;
        fetch();
        return this;
    }

    /**
     * Return a {@link ClientStats} that covers all procedures and
     * all connection ids. The {@link ClientStats} instance will
     * apply to the time period currently covered by the context.
     *
     * @return A {@link ClientStats} instance.
     */
    public ClientStats getStats() {
        return ClientStats.merge(getStatsByConnection().values());
    }

    /**
     * Return a map of {@link ClientStats} by procedure name. This will
     * roll up {@link ClientStats} instances by connection id. Each
     * {@link ClientStats} instance will apply to the time period
     * currently covered by the context.
     *
     * @return A map from procedure name to {@link ClientStats} instances.
     */
    public Map<String, ClientStats> getStatsByProc() {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStats();
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            for (Entry<String, ClientStats> e2 : e.getValue().entrySet()) {
                ClientStats current = e2.getValue();
                ClientStats aggregate = retval.get(current.getProcedureName());
                if (aggregate == null) {
                    retval.put(current.getProcedureName(), (ClientStats) current.clone());
                }
                else {
                    aggregate.add(current);
                }
            }
        }
        return retval;
    }

    /**
     * Return a map of {@link ClientStats} by connection id. This will
     * roll up {@link ClientStats} instances by procedure name for each
     * connection. Note that connection id is unique, while hostname and
     * port may not be. Hostname and port will be included in the
     * {@link ClientStats} instance data. Each {@link ClientStats}
     * instance will apply to the time period currently covered by the
     * context.
     *
     * @return A map from connection id to {@link ClientStats} instances.
     */
    public Map<Long, ClientStats> getStatsByConnection() {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStats();
        Map<Long, ClientIOStats> completeIO = diffIO(m_currentIO, m_baselineIO);
        Map<Long, ClientStats> retval = new TreeMap<Long, ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            ClientStats cs = ClientStats.merge(e.getValue().values());
            ClientIOStats cios = completeIO.get(e.getKey());
            if (cios != null) {
                cs.m_bytesReceived = cios.m_bytesReceived;
                cs.m_bytesSent = cios.m_bytesSent;
            }
            retval.put(e.getKey(), cs);
        }
        return retval;
    }

    /**
     * Return a map of maps by connection id. Each sub-map maps procedure
     * names to {@link ClientStats} instances. Note that connection id is
     * unique, while hostname and port may not be. Hostname and port will
     * be included in the {@link ClientStats} instance data. Each
     * {@link ClientStats} instance will apply to the time period currently
     * covered by the context. This is full set of data available from this
     * context instance.
     *
     * @return A map from connection id to {@link ClientStats} instances.
     */
    public Map<Long, Map<String, ClientStats>> getCompleteStats() {
        Map<Long, Map<String, ClientStats>> retval =
                new TreeMap<Long, Map<String, ClientStats>>();

        for (Entry<Long, Map<String, ClientStats>> e : m_current.entrySet()) {
            if (m_baseline.containsKey(e.getKey())) {
                retval.put(e.getKey(), diff(e.getValue(), m_baseline.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), dup(e.getValue()));
            }
        }

        // reset the timestamp fields to reflect the difference
        for (Entry<Long, Map<String, ClientStats>> e : retval.entrySet()) {
            for (Entry<String, ClientStats> e2 : e.getValue().entrySet()) {
                ClientStats cs = e2.getValue();
                cs.m_startTS = Math.max(cs.m_startTS, m_baselineTS);
                cs.m_endTS = Math.min(cs.m_endTS, m_currentTS);
            }
        }

        return retval;
    }

    /**
     * Return a {@link ClientStats} instance for a specific procedure
     * name. This will be rolled up across all connections. The
     * {@link ClientStats} instance will apply to the time period
     * currently covered by the context.
     *
     * @return A {@link ClientStats} instance.
     */
    public ClientStats getStatsForProcedure(String procedureName) {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStats();
        List<ClientStats> statsForProc = new ArrayList<ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            ClientStats procStats = e.getValue().get(procedureName);
            if (procStats != null) {
                statsForProc.add(procStats);
            }
        }
        if (statsForProc.size() == 0) {
            return null;
        }
        return ClientStats.merge(statsForProc);
    }

    Map<Long, ClientIOStats> diffIO(Map<Long, ClientIOStats> newer, Map<Long, ClientIOStats> older) {
        Map<Long, ClientIOStats> retval = new TreeMap<Long, ClientIOStats>();
        for (Entry<Long, ClientIOStats> e : newer.entrySet()) {
            if (older.containsKey(e.getKey())) {
                retval.put(e.getKey(), ClientIOStats.diff(e.getValue(), older.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), (ClientIOStats) e.getValue().clone());
            }
        }
        return retval;
    }

    Map<String, ClientStats> diff(Map<String, ClientStats> newer, Map<String, ClientStats> older) {
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<String, ClientStats> e : newer.entrySet()) {
            if (older.containsKey(e.getKey())) {
                retval.put(e.getKey(), ClientStats.diff(e.getValue(), older.get(e.getKey())));
            }
            else {
                retval.put(e.getKey(), (ClientStats) e.getValue().clone());
            }
        }
        return retval;
    }

    Map<String, ClientStats> dup(Map<String, ClientStats> x) {
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<String, ClientStats> e : x.entrySet()) {
            retval.put(e.getKey(), (ClientStats) e.getValue().clone());
        }
        return retval;
    }
}
