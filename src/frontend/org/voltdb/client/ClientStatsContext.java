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

public class ClientStatsContext {

    protected final Distributer m_distributor;
    protected Map<Long, Map<String, ClientStats>> m_baseline;
    protected Map<Long, Map<String, ClientStats>> m_current;
    protected long m_baselineTS;
    protected long m_currentTS;

    ClientStatsContext(Distributer distributor, Map<Long, Map<String, ClientStats>> current) {
        m_distributor = distributor;
        m_baseline = new TreeMap<Long, Map<String, ClientStats>>();
        m_current = current;
        m_baselineTS = m_currentTS = System.currentTimeMillis();

    }

    public ClientStatsContext fetch() {
        m_current = m_distributor.getStatsSnapshot();
        m_currentTS = System.currentTimeMillis();
        return this;
    }

    public ClientStatsContext fetchAndResetBaseline() {
        m_baseline = m_current;
        m_baselineTS = m_currentTS;
        m_current = m_distributor.getStatsSnapshot();
        m_currentTS = System.currentTimeMillis();
        return this;
    }

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

    public Map<Long, ClientStats> getStatsByConnection() {
        Map<Long, Map<String, ClientStats>> complete = getCompleteStats();
        Map<Long, ClientStats> retval = new TreeMap<Long, ClientStats>();
        for (Entry<Long, Map<String, ClientStats>> e : complete.entrySet()) {
            retval.put(e.getKey(), ClientStats.merge(e.getValue().values()));
        }
        return retval;
    }

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

        // reset the since field to reflect the difference
        for (Entry<Long, Map<String, ClientStats>> e : retval.entrySet()) {
            for (Entry<String, ClientStats> e2 : e.getValue().entrySet()) {
                ClientStats cs = e2.getValue();
                cs.m_since = Math.max(cs.m_since, m_baselineTS);
            }
        }

        return retval;
    }

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

    public ClientStats getStats() {
        return ClientStats.merge(getStatsByConnection().values());
    }

    public Map<String, ClientStats> diff(Map<String, ClientStats> newer, Map<String, ClientStats> older) {
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

    public Map<String, ClientStats> dup(Map<String, ClientStats> x) {
        Map<String, ClientStats> retval = new TreeMap<String, ClientStats>();
        for (Entry<String, ClientStats> e : x.entrySet()) {
            retval.put(e.getKey(), (ClientStats) e.getValue().clone());
        }
        return retval;
    }
}
