/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;
import org.voltdb.plannerv2.guards.CalciteCheck;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.sysprocs.AdHocNTBase;

import com.google.common.collect.ImmutableList;

/**
 * A basic SQL query batch containing one or more {@link SqlTask}s.
 * It does not carry information specific to DDL or non-DDL planning.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class SqlBatchImpl extends SqlBatch {

    private final ImmutableList<SqlTask> m_tasks;
    private final ImmutableList<Object> m_userParams;
    private final Boolean m_isDDLBatch;
    private final Context m_context;

    static final String ADHOC_ERROR_RESPONSE =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    /**
     * A chain of checks to determine whether a SQL statement should be routed to Calcite.
     */
    static final CalciteCheck CALCITE_CHECKS = CalciteCheck.create();

    /**
     * Build a batch from a string of one or more SQL statements. </br>
     * The SQL statements can have parameter place-holders, but we will throw
     * an error if the batch has more than one query when user parameters are supplied
     * because we currently do not support it.
     * @param sqlBlock a string of one or more SQL statements.
     * @param userParams user parameter values for the query.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws PlannerFallbackException when any of the queries in the batch cannot be handled by Calcite.
     * @throws UnsupportedOperationException when the batch is a mixture of
     * DDL and non-DDL statements or has parameters and more than one query at the same time.
     */
    SqlBatchImpl(String sqlBlock, Object[] userParams, Context context)
            throws SqlParseException, PlannerFallbackException {
        // We can process batches of either all DDL or all DML/DQL, no mixed batch can be accepted.
        // Split the SQL statements, and process them one by one.
        // Currently (1.17.0), SqlParser only supports parsing single SQL statement.
        // https://issues.apache.org/jira/browse/CALCITE-2310

        // TODO: Calcite's error message (in SqlParseException) will contain line and column numbers.
        // This information is lost during the split. It will be helpful to develop a way to preserve it.
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
        // Iterate over the SQL string list and build SqlTasks out of the SQL strings.
        for (final String sql : sqlList) {
            if (! CALCITE_CHECKS.check(sql)) {
                // The query cannot pass the compatibility check, throw a fall-back exception
                // so that VoltDB will use the legacy parser and planner.
                throw new PlannerFallbackException();
            }
            SqlTask sqlTask = SqlTask.from(sql);
            if (isDDLBatch == null) {
                isDDLBatch = sqlTask.isDDL();
            } else if (isDDLBatch ^ sqlTask.isDDL()) { // True if isDDLBatch is different from isDDL
                // No mixing DDL and DML/DQL. Turn this into an error and return it to the client.
                throw new UnsupportedOperationException("Mixing DDL with DML/DQL queries is unsupported.");
            }
            taskBuilder.add(sqlTask);
        }
        m_tasks = taskBuilder.build();
        m_userParams = ImmutableList.copyOf(userParams);
        m_isDDLBatch = isDDLBatch;
        m_context = context;
    }

    @Override
    public CompletableFuture<ClientResponse> execute() {
        // This function is called for DDL batches. Non-DDL batches will be handled in NonDDLBatch.execute().
        final List<String> sqls = new ArrayList<>(getTaskCount()), validated = new ArrayList<>();
        final List<SqlNode> nodes = new ArrayList<>(getTaskCount());
        for (SqlTask task : this) {
            final String sql = task.getSQL();
            sqls.add(sql);
            nodes.add(task.getParsedQuery());
            AdHocNTBase.processAdHocSQLStmtTypes(sql, validated);
            // NOTE: need to exercise the processAdHocSQLStmtTypes() method, because some tests
            // (i.e. ENG-7653, TestAdhocCompilerException.java) rely on the code path.
            // But, we might not need to call it? Certainly it involves some refactory for the check and simulation.
            validated.clear();
        }
        return getContext().runDDLBatch(sqls, nodes);
    }

    @Override
    public boolean isDDLBatch() {
        return m_isDDLBatch;
    }

    @Override
    public Iterator<SqlTask> iterator() {
        return m_tasks.iterator();
    }

    @Override
    public Object[] getUserParameters() {
        return m_userParams.toArray();
    }

    @Override
    public int getTaskCount() {
        return m_tasks.size();
    }

    @Override
    Context getContext() {
        return m_context;
    }
}
