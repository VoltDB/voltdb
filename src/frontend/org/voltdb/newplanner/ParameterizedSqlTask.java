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

import java.util.List;

import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.util.Util.FoundOne;

/**
 * Parameterize a query stored in a {@link SqlTask} and store its parameter
 * values as a list of {@link SqlLiteral}.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class ParameterizedSqlTask extends AbstractSqlTaskDecorator {

    private final List<SqlLiteral> m_sqlLiteralList;

    /**
     * Build a {@code ParameterizedSqlTask} from a {@code SqlTask}.
     * @param taskToDecorate the {@code SqlTask} this is built on.
     */
    public ParameterizedSqlTask(SqlTask taskToDecorate) {
        super(taskToDecorate);
        // Re-decorating another ParameterizedSqlTask is not what we want to see.
        // But, be robust.
        if (taskToDecorate instanceof ParameterizedSqlTask) {
            m_sqlLiteralList = ((ParameterizedSqlTask) taskToDecorate).m_sqlLiteralList;
            return;
        }
        // DDL statements cannot be parameterized.
        boolean doNotParameterize = taskToDecorate.isDDL();
        if (! doNotParameterize) {
            try {
                getParsedQuery().accept(DynamicParamFinder.INSTANCE);
            } catch (FoundOne found) {
                // If the SqlTask already has parameters, do not parameterize it.
                doNotParameterize = true;
            }
        }
        if (doNotParameterize) {
            m_sqlLiteralList = null;
        } else {
            ParameterizationVisitor visitor = new ParameterizationVisitor();
            getParsedQuery().accept(visitor);
            m_sqlLiteralList = visitor.getSqlLiteralList();
        }
    }

    /**
     * Get the List of parameter values.
     * @return the List of parameter values.
     */
    public List<SqlLiteral> getSqlLiteralList() {
        return m_sqlLiteralList;
    }

    /**
     * A visitor to find {@link SqlDynamicParam} in a node tree.
     * @author Yiqun Zhang
     * @since 8.4
     */
    private static final class DynamicParamFinder extends SqlBasicVisitor<Void> {
        static final DynamicParamFinder INSTANCE = new DynamicParamFinder();
        @Override
        public Void visit(SqlDynamicParam param) {
            throw FoundOne.NULL;
        }
    }
}
