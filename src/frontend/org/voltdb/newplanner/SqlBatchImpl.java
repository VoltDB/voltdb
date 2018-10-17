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

package org.voltdb.newplanner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.newplanner.guards.AlwaysFail;
import org.voltdb.newplanner.guards.AlwaysPassThrough;
import org.voltdb.newplanner.guards.CalcitePass;
import org.voltdb.newplanner.guards.NoLargeQuery;
import org.voltdb.newplanner.guards.PlannerFallbackException;
import org.voltdb.parser.SQLLexer;

/**
 * A basic SQL query batch containing one or more {@link SqlTask}s.
 * It does not carry information specific to DDL or non-DDL planning.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class SqlBatchImpl extends SqlBatch {

    private final List<SqlTask> m_tasks;
    private final Boolean m_isDDLBatch;
    private final Object[] m_userParams;

    static final String s_adHocErrorResponseMessage =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    static final CalcitePass s_calcitePass = new AlwaysPassThrough();

    static {
        s_calcitePass.addNext(new NoLargeQuery())
                     .addNext(new AlwaysFail());
    }

    /**
     * Build a batch from a string of one or more SQL statements. </br>
     * The SQL statements can have parameter place-holders, but we will throw
     * an error if the batch has more than one query when user parameters are supplied
     * because we currently do not support this.
     * @param sqlBlock a string of one or more SQL statements.
     * @param userParams user parameter values for the query.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws PlannerFallbackException when any of the queries in the batch cannot be handled by Calcite.
     * @throws UnsupportedOperationException when the batch is a mixture of
     * DDL and non-DDL statements or has parameters and more than one query at the same time.
     */
    public SqlBatchImpl(String sqlBlock, Object... userParams) throws SqlParseException, PlannerFallbackException {
        // We can process batches of either all DDL or all DML/DQL, no mixed batch can be accepted.
        // Split the SQL statements, and process them one by one.
        // Currently (1.17.0), SqlParser only supports parsing single SQL statement.
        // https://issues.apache.org/jira/browse/CALCITE-2310

        // TODO: Calcite's error message (in SqlParseException) will contain line and column numbers.
        // This information is lost during the split. It will be helpful to develop a way to preserve it.
        List<String> sqlList = SQLLexer.splitStatements(sqlBlock).getCompletelyParsedStmts();
        if (userParams != null && userParams.length > 0 && sqlList.size() != 1) {
            throw new UnsupportedOperationException(s_adHocErrorResponseMessage);
        }
        m_tasks = new ArrayList<>(sqlList.size());
        // Are all the queries in this input batch DDL? (null means not determined yet)
        Boolean isDDLBatch = null;
        // Iterate over the SQL string list and build SqlTasks out of the SQL strings.
        for (final String sql : sqlList) {
            if (! s_calcitePass.check(sql)) {
                // The query cannot pass the compatibility check, throw a fallback exception
                // so that VoltDB will use the old parser and planner.
                throw new PlannerFallbackException();
            }
            SqlTask sqlTask = SqlTaskFactory.createSqlTask(sql);
            if (isDDLBatch == null) {
                isDDLBatch = sqlTask.isDDL();
            } else if (isDDLBatch ^ sqlTask.isDDL()) { // True if isDDLBatch is different from isDDL
                // No mixing DDL and DML/DQL. Turn this into an error and return it to the client.
                throw new UnsupportedOperationException("Mixing DDL with DML/DQL queries is unsupported.");
            }
            m_tasks.add(sqlTask);
        }
        m_isDDLBatch = isDDLBatch;
        m_userParams = userParams;
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
        return m_userParams;
    }

    @Override
    public int getTaskCount() {
        return m_tasks.size();
    }
}
