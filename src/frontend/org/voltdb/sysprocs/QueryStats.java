/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.json_voltpatches.JSONObject;
import org.voltcore.utils.Pair;
import org.voltdb.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.volttableutil.VoltTableUtil;

/**
 * QueryStats "pseudo-" system procedure.
 * Usage: exec @QueryStats 'select * from (table, 0);' -- Note that the query string needs to be quoted
 * Or, in sqlcmd: > querystats select * from (table, 0);
 * Complex queries are supported, e.g.
 * querystats select * from (table, 0) inner join (index, 0) using (host_id, hostname, site_id, partition_id) where site_id = 0;
 *
 * This is called "pseudo-", since under the hood fake, temporary table(s) is created
 * inside Calcite that stores query statistics and Calcite is used to run the query.
 * It is split into separate steps:
 * 1. Generate table(s) for all @Statistics metrics, each as a separate call
 * 2. Add those as temp tables into Calcite schema (VoltDB schema is not affected)
 * 3. Plan and execute SELECT query from Calcite
 * 4. Return an artificial result table.
 * In case of error in these steps, the error message is printed to stderr, and empty table is returned.
 * We cannot return GRACEFUL_FAILURE, since that would exit SQLCMD.
 *
 * @author Chao Zhou, Wenxuan Qiu, Lukai Liu
 */
public class QueryStats extends AdHocNTBase {
    private static final Map<String, StatsSelector> STATISTICS =
            Arrays.stream(StatsSelector.getAllStatsCollector())
            .collect(Collectors.toMap(Enum::name, a -> a, (a, b) -> b));
    private static final Pattern PROC_PATTERN = Pattern.compile("\\(\\s*([a-zA-Z]+)\\s*,\\s*(\\d+)\\s*\\)");
    private static final String tempTableAlias = "TT";

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runInternal(params);
    }
    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        return runUsingCalcite(params);     // fall back to Calcite
    }
    @Override
    protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) {
        assert params.size() == 1 : "Sysproc QueryStats accepts a single query string";
        return CompletableFuture.supplyAsync(() -> {
            try {
                return dispatchQueryStats((String) params.getParam(0));
            } catch (Exception e) {
                return error(e);
            }
        });
    }

    /**
     * Report error back to sqlcmd.
     * Note that since we choose to always succeed transaction, an error is only printed to stderr,
     * and an empty table is returned.
     * @param e exception to be printed to stderr, and an empty table will be returned.
     */
    private static ClientResponse error(Exception e) {
        System.err.println(e.getMessage());
        return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], e.getMessage());
    }

    private static ClientResponse dispatchQueryStats(String sql) throws Exception {
        final List<Pair<String, VoltTable>> tables = new LinkedList<>();
        final StringBuffer buf = new StringBuffer();
        final Matcher m = PROC_PATTERN.matcher(sql);
        while (m.find()) {
            final String procName = m.group(1).toUpperCase();
            if (STATISTICS.containsKey(procName)) {
                final int procArg = Integer.parseInt(m.group(2));
                m.appendReplacement(buf, Matcher.quoteReplacement(tempTableAlias + tables.size()));
                tables.add(new Pair<>(tempTableAlias + tables.size(),
                        VoltDB.instance().getStatsAgent().collectDistributedStats(
                                new JSONObject(new HashMap<String, Object>(){{
                                    put("selector", "STATISTICS");
                                    put("subselector", procName);
                                    put("interval", procArg == 1);
                                }}))[0],
                        false));
            } else {
                throw new RuntimeException(String.format("Unrecognized @Statistics procedure name \"%s\"", procName));
            }
        }
        m.appendTail(buf);
        return new ClientResponseImpl(ClientResponse.SUCCESS,
                new VoltTable[] {
                        VoltTableUtil.executeSql(buf.toString().replaceAll(";", " "), tables)
                }, "SUCCESS");
    }
}
