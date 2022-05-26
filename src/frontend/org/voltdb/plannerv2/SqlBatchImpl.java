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

package org.voltdb.plannerv2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.UnmodifiableArrayList;
import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;
import org.voltdb.plannerv2.guards.AcceptAllSelect;
import org.voltdb.plannerv2.guards.AcceptDDLsAsWeCan;
import org.voltdb.plannerv2.guards.BanLargeQuery;
import org.voltdb.plannerv2.guards.CalciteCompatibilityCheck;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.sysprocs.AdHocNTBase;

import com.google.common.collect.ImmutableList;

/**
 * A basic query batch with one or more {@link SqlTask}.
 * The basic information is sufficient for DDL batches, but planning DQL/DML needs
 * more add-ons. We will have decorators {@link NonDdlBatch}
 * to bulk up the information contained in the batch.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class SqlBatchImpl extends SqlBatch {

    private final ImmutableList<SqlTask> m_tasks;
    // use UnmodifiableArrayList in calcite instead of immutableList in Guava because we want to allow null elements
    private final UnmodifiableArrayList<Object> m_userParams;
    private final Boolean m_isDDLBatch;
    private final Context m_context;
    VoltLogger log = new VoltLogger("Calcite");

    static final String ADHOC_ERROR_RESPONSE =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    /**
     * A chain of checks to determine whether a SQL statement should be routed to Calcite.
     * Eventually we will let Calcite support all the VoltDB SQLs and remove this check from the code.
     * Disapproving checks should be chained first (e.g.: {@link BanLargeQuery}).
     */
    static final CalciteCompatibilityCheck CALCITE_CHECKS =
            CalciteCompatibilityCheck.chain(
                    // Disapproving checks go first
                    new BanLargeQuery(),
                    // Approving checks go next.
                    new AcceptDDLsAsWeCan(),
                    new AcceptAllSelect());

    /**
     * Build a batch from a string of one or more SQL statements. </br>
     * The SQL statements can have parameter place-holders, but we will throw
     * an error if the batch has more than one query when user parameters are supplied
     * because we currently do not support it.
     *
     * @param sqlBlock    a string of one or more SQL statements.
     * @param userParams  user parameter values for the query.
     * @param context     the AdHoc context. used for calling internal AdHoc APIs before the refactor is done.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws PlannerFallbackException when any of the queries in the batch cannot be handled by Calcite.
     * @throws UnsupportedOperationException when the batch is a mixture of DDL and non-DDL
     *         statements or has parameters and more than one query at the same time.
     */
    SqlBatchImpl(String sqlBlock, Object[] userParams, Context context)
            throws SqlParseException, PlannerFallbackException {
        // TODO: Calcite's error message (in SqlParseException) will contain line and column numbers.
        // This information is lost during the split. It will be helpful to develop a way to preserve it.

        // Split the SQL statements, and process them one by one.
        // Currently (1.17.0), SqlParser only supports parsing single SQL statement.
        // https://issues.apache.org/jira/browse/CALCITE-2310
        List<String> sqlList = SQLLexer.splitStatements(sqlBlock).getCompletelyParsedStmts();
        if (sqlList.size() == 0) {
            throw new RuntimeException("Failed to plan, no SQL statement provided.");
        }
        if (userParams != null && userParams.length > 0 && sqlList.size() != 1) {
            throw new UnsupportedOperationException(ADHOC_ERROR_RESPONSE);
        }

        ImmutableList.Builder<SqlTask> taskBuilder = new ImmutableList.Builder<>();
        // Are all the queries in this input batch DDL? (null means not determined yet)
        Boolean isDDLBatch = null;
        int lineNo = 0;
        // Iterate over the SQL string list and build SqlTasks out of the SQL strings.
        for (final String sql : sqlList) {
            try {
                if (! CALCITE_CHECKS.check(sql)) {
                    // The query cannot pass the compatibility check, throw a fall-back exception
                    // so that VoltDB will use the legacy parser and planner.
                    throw new PlannerFallbackException("Not in white list, or is black listed: " + sql);
                }
            } catch (SqlParseException e) {
                final String errMsg = String.format("Error: invalid SQL statement in line: %d, column %d. Expecting one of: %s",
                        (lineNo + e.getPos().getLineNum()), e.getPos().getColumnNum(),
                        e.getExpectedTokenNames());
                log.debug(errMsg);
                throw new PlannerFallbackException(errMsg);
                // TODO throw a parse exception instead of fallback exception to reflect error position
                // TODO respond error message back to sqlcmd
            }
            SqlTask sqlTask = SqlTask.from(sql);
            if (isDDLBatch == null) {
                isDDLBatch = sqlTask.isDDL();
            } else if (isDDLBatch != sqlTask.isDDL()) {
                // No mixing DDL and DML/DQL. Turn this into an error and return it to the client.
                throw new UnsupportedOperationException("Mixing DDL with DML/DQL queries is unsupported.");
            }
            taskBuilder.add(sqlTask);
            lineNo += StringUtils.countMatches(sql, "\n") + 1;
        }
        m_tasks = taskBuilder.build();
        if (userParams != null) {
            m_userParams = UnmodifiableArrayList.of(userParams);
        } else {
            m_userParams = null;
        }
        m_isDDLBatch = isDDLBatch;
        m_context = context;
    }

    @Override public CompletableFuture<ClientResponse> execute() {
        // TRAIL [Calcite-AdHoc-DDL:1] SqlBatchImpl.execute()

        // DDL batches will be executed here because they don't require additional context information.
        // DQL/DML batches need to be decorated by NonDDLBatch so they can have enough information for planning.
        // It is NonDDLBatch.execute() that will get called for those batches.

        // Note - ethan - 12/28/2018:
        // The code here is still not in its final form. It is more like a compromised, temporary solution
        // and relies heavily on the legacy code path.
        final List<String> sqls = new ArrayList<>(getTaskCount()), validated = new ArrayList<>();
        final List<SqlNode> nodes = new ArrayList<>(getTaskCount());
        for (SqlTask task : this) {
            final String sql = task.getSQL();
            sqls.add(sql);
            nodes.add(task.getParsedQuery());
            // NOTE: need to exercise the processAdHocSQLStmtTypes() method, because some tests
            // (i.e. ENG-7653, TestAdhocCompilerException.java) rely on the code path.
            // But, we might not need to call it? Certainly it involves some refactory for
            // the check and simulation.
            AdHocNTBase.processAdHocSQLStmtTypes(sql, validated);
            validated.clear();
        }
        return getContext().runDDLBatch(sqls, nodes);
    }

    @Override public boolean isDDLBatch() {
        return m_isDDLBatch;
    }

    @Override public Iterator<SqlTask> iterator() {
        return m_tasks.iterator();
    }

    @Override public Object[] getUserParameters() {
        if (m_userParams == null) {
            return null;
        } else {
            return m_userParams.toArray();
        }
    }

    @Override public int getTaskCount() {
        return m_tasks.size();
    }

    @Override Context getContext() {
        return m_context;
    }
}
