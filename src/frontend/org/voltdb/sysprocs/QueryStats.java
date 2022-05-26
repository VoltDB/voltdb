/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.voltdb.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.volttableutil.VoltTableUtil;

/**
 * QueryStats "pseudo-" system procedure.
 * Usage: exec @QueryStats 'select * from (table, 0);' -- Note that the query string needs to be quoted
 * Or, in sqlcmd: > querystats select * from statistics(table, 0);
 * Complex queries are supported, e.g.:
 * querystats select * from statistics(table, 0) inner join statistics(index, 0)
 * using (host_id, hostname, site_id, partition_id) where site_id = 0;
 * For stats that return more than 1 table, you can specify the optional third argument as table index (0 indexed)
 * i.e. sqlcmd: > querystats select * from statistics(DRPRODUCER, 0, 1); could retrieve the second table (node-level table)
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
    private static final Pattern PROC_PATTERN =
            Pattern.compile("statistics\\s*\\(\\s*(\\w+)\\s*,\\s*(\\d+)\\s*(,\\s*(\\d+)\\s*)?\\)", Pattern.CASE_INSENSITIVE);
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
                // NOTE: we cannot give back GRACEFUL_FAILURE, since that would quit sqlcmd.
                // Instead, we give back empty table, and print error message.
                // This is not perfect, but it works much of the time.
                final String msg = truncated(e.getMessage()); // truncate error message if too long
                System.err.println(msg);
                return new ClientResponseImpl(ClientResponse.SUCCESS, new VoltTable[0], msg);
            }
        });
    }

    /**
     * Truncate a message that is too long, i.e. over 5 lines, to 5 lines.
     * @param src source message
     * @return truncated message if source is too long; or source message as provided.
     */
    private static String truncated(String src) {
        final String[] lines = src.split("\n");
        return lines.length <= 5 ? src :
                String.join("\n", Arrays.copyOfRange(lines, 0, 5));
    }

    private ClientResponse dispatchQueryStats(String sql) throws Exception {
        final List<String> tableNames = new LinkedList<>();
        final List<VoltTable> tables = new LinkedList<>();
        final StringBuffer buf = new StringBuffer();
        final Matcher m = PROC_PATTERN.matcher(sql);
        while (m.find()) {
            final String procName = m.group(1).toUpperCase();
            if (STATISTICS.containsKey(procName)) {
                final int interval = Integer.parseInt(m.group(2));
                final int tableId = m.group(4) == null ? 0 :Integer.parseInt(m.group(4));
                m.appendReplacement(buf, Matcher.quoteReplacement(tempTableAlias + tables.size()));
                tableNames.add(tempTableAlias + tables.size());

                    CompletableFuture<ClientResponse> cf = callProcedure("@Statistics", procName, interval);
                    // for making result table in order, has to sync the result to table
                    ClientResponseImpl cri = (ClientResponseImpl) (cf.get(1, TimeUnit.MINUTES));
                if (cri.getStatus() == ClientResponse.SUCCESS) {
                    VoltTable[] results = cri.getResults();
                    if (results.length <= tableId) {
                        throw new RuntimeException(String.format("@Statistics procedure  \"%s\" does not have %dth table", procName, tableId));
                    }
                    tables.add(results[tableId]);
                } else {
                    throw new RuntimeException(String.format("@Statistics procedure  \"%s\" failed with %s", procName, cri.getStatusString()));
                }
            } else {
                throw new RuntimeException(String.format("Unrecognized @Statistics procedure name \"%s\"", procName));
            }
        }
        m.appendTail(buf);
        final String query = buf.toString().replaceAll(";", " ");
        return new ClientResponseImpl(ClientResponse.SUCCESS,
                new VoltTable[] {VoltTableUtil.executeSql(query, tableNames, tables)},
                "SUCCESS");
    }
}
