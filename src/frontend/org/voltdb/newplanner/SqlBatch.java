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

import java.util.Arrays;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.ParameterSet;

/**
 * The abstract class for a SQL query batch containing one or more {@link SqlTask}s. </br>
 * This class implemented the {@code Iterable<SqlTask>} interface, so
 * one can use a Java for-each loop to enumerate the contained {@code SqlTask}s.
 * @since 8.4
 * @author Yiqun Zhang
 */
public abstract class SqlBatch implements Iterable<SqlTask>  {

    /**
     * Check if the batch is purely comprised of DDL statements.
     * @return true if the batch is comprised of DDL statements only.
     */
    public abstract boolean isDDLBatch();


    /**
     * Get the user parameters.
     * @return the user parameter array.
     */
    public abstract Object[] getUserParameters();

    /**
     * Get the number of tasks in this batch.
     * @return the count of tasks in this batch.
     */
    public abstract int getTaskCount();

    /**
     * Build a {@link SqlBatch} from a {@link ParameterSet} passed through the {@code @AdHoc}
     * system stored procedure.
     * @param params the user parameters. The first parameter is always the query text.
     * The rest parameters are the ones used in the query.
     * @return a {@code SqlBatch} built from the given {@code ParameterSet}.
     * @throws SqlParseException when the query parsing went wrong.
     * @throws UnsupportedOperationException when the batch is a mixture of
     * DDL and non-DDL statements or has parameters and more than one query at the same time.
     */
    public static SqlBatch fromParameterSet(ParameterSet params) throws SqlParseException {
        Object[] paramArray = params.toArray();
        String sqlBlock = (String) paramArray[0];
        Object[] userParams = null;
        // AdHoc query can have parameters, see TestAdHocQueries.testAdHocWithParams.
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }
        return new BasicSqlBatch(sqlBlock, userParams);
    }
}
