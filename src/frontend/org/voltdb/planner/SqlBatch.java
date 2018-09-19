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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ParameterSet;
import org.voltdb.parser.SQLLexer;

/**
 * A SQL query batch containing one or more {@link SqlTask}s. </br>
 * This class implemented the {@code Iterable<SqlTask>} interface.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class SqlBatch implements Iterable<SqlTask> {

    final List<SqlTask> m_tasks;
    final Boolean m_isDDLBatch;
    final Object[] m_userParams;

    static final VoltLogger s_adHocLog = new VoltLogger("ADHOC");

    /**
     * Build a batch from a string with one or more SQL statements. </br>
     * The SQL statements can have parameter place-holders, but we will throw
     * an error if the batch has more than one query and user parameters are supplied
     * because we currently do not support this.
     * @param sqlBlock a string of one or more SQL statements.
     * @param userParams user parameter values for the query.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws UnsupportedOperationException when the batch is a mixture of
     * DDL and non-DDL statements.
     */
    public SqlBatch(String sqlBlock, Object... userParams) throws SqlParseException {
        // We can process batches with either all DDL or all DML/DQL, no mixed batch can be accepted.
        // Split the SQL statements, and run them through SqlParser.
        // Currently (1.17.0), SqlParser only supports parsing single SQL statement.
        // https://issues.apache.org/jira/browse/CALCITE-2310

        // TODO: Calcite's error message (in SqlParseException) will contain line and column numbers,
        // this information is lost during the split. It will be helpful to develop a way to preserve
        // this information.
        List<String> sqlList = SQLLexer.splitStatements(sqlBlock).getCompletelyParsedStmts();
        m_tasks = new ArrayList<>(sqlList.size());
        // Are all the queries in this input batch DDL? (null means not determined yet)
        Boolean isDDLBatch = null;
        // Iterate over the SQL string list and build SqlTasks out of the SQL strings.
        for (final String sql : sqlList) {
            SqlTask sqlTask = new SqlTask(sql);
            if (isDDLBatch == null) {
                isDDLBatch = sqlTask.isDDL();
            } else if (isDDLBatch ^ sqlTask.isDDL()) { // True if isDDLBatch is different from isDDL
                // No mixing DDL and DML/DQL. Turn this into an error and return it to the client.
                throw new UnsupportedOperationException("Mixing DDL with DML/DQL queries is unsupported.");
            }
        }
        m_isDDLBatch = isDDLBatch;
        m_userParams = userParams;
    }

    /**
     * A package-visible copy constructor for internal usages.
     * @param other the other {@link SqlBatch} to copy from.
     */
    SqlBatch(SqlBatch other) {
        m_tasks = other.m_tasks;
        m_isDDLBatch = other.m_isDDLBatch;
        m_userParams = other.m_userParams;
    }

    /**
     * Check if the batch is purely comprised of DDL statements.
     * @return true if the batch is comprised of DDL statements only.
     */
    public boolean isDDLBatch() {
        return m_isDDLBatch;
    }

    /**
     * Get an iterator over the {@link SqlTask}s in this batch.
     * @return an iterator over the {@code SqlTasks} in this batch.
     */
    @Override
    public Iterator<SqlTask> iterator() {
        return m_tasks.iterator();
    }

    /**
     * Build a {@link SqlBatch} from a {@link ParameterSet} passed through the {@code @AdHoc}
     * system stored procedure.
     * @param params the user parameters. The first parameter is always the query text.
     * The rest parameters are the ones used in the query.
     * @return a {@code SqlBatch} built from the given {@code ParameterSet}.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws UnsupportedOperationException when the batch is a mixture of
     * DDL and non-DDL statements.
     */
    public static SqlBatch fromParameterSet(ParameterSet params) throws SqlParseException {
        Object[] paramArray = params.toArray();
        String sqlBlock = (String) paramArray[0];
        Object[] userParams = null;
        // AdHoc query can have parameters, see TestAdHocQueries.testAdHocWithParams.
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }
        return new SqlBatch(sqlBlock, userParams);
    }

    /**
     * Get the user parameters.
     * @return the user parameter array.
     */
    public Object[] getUserParameters() {
        return m_userParams;
    }
}
