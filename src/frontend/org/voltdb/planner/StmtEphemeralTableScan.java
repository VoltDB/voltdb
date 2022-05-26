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
package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map;

import org.voltcore.utils.Pair;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;

/**
 * An ephemeral table is one which is not persistent.  These are
 * derived tables, which we call subqueries, and common tables, which
 * are defined by WITH clauses.  This just aggregates some common
 * behavior.
 */
public abstract class StmtEphemeralTableScan extends StmtTableScan {
    // If we have a TVE which is a reference to a column in
    // the this table scan, it will have a differentiator.  It will
    // also have a name, but there may be two columns in the
    // this table scan which have the same name, so that
    // won't uniquely determine the column.  Note that the
    // value from m_outputColumnIndexMap is the index in the
    // result of getScanColumns().  So, getScanColumns() has to
    // return the list of columns we will scan.  The index of
    // TVEs which reference columns in this scan comes from these
    // values.
    protected final Map<Pair<String, Integer>, Integer> m_outputColumnIndexMap = new HashMap<>();
    /**
     * This is the equivalent of the catalog's Table.  We don't
     * have a lot of what the catalog has, but we have the contents
     * of a schema, and that's enough for us.
     */
    private final NodeSchema m_outputColumnSchema = new NodeSchema();

    private final String m_tableName;

    public StmtEphemeralTableScan(String tableName, String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
        m_tableName = tableName;
    }

    private StatementPartitioning m_scanPartitioning = null;

    @Override
    public final String getTableName() {
        return m_tableName;
    }

    public void setScanPartitioning(StatementPartitioning scanPartitioning) {
        m_scanPartitioning = scanPartitioning;
    }

    public final StatementPartitioning getScanPartitioning() {
        return m_scanPartitioning;
    }

    public void addOutputColumn(SchemaColumn scol) {
        String colAlias = scol.getColumnAlias();
        int differentiator = scol.getDifferentiator();
        // Order matters here.  We want to assign the index
        // in m_outputColumnIndexMap before we add the column to the
        // schema.
        m_outputColumnIndexMap.put(Pair.of(colAlias, differentiator), m_outputColumnSchema.size());
        m_outputColumnSchema.addColumn(scol);
    }

    public SchemaColumn getSchemaColumn(int columnIndex) {
        return m_outputColumnSchema.getColumn(columnIndex);
    }

    public NodeSchema getOutputSchema() {
        return m_outputColumnSchema;
    }

    public Integer getColumnIndex(String columnAlias, int differentiator) {
        return m_outputColumnIndexMap.get(Pair.of(columnAlias, differentiator));
    }

    @Override
    public AbstractExpression processTVE(TupleValueExpression expr, String columnName) {
        Integer idx = m_outputColumnIndexMap.get(Pair.of(columnName, expr.getDifferentiator()));
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns <"
                                                + columnName
                                                + ", "
                                                + expr.getDifferentiator()
                                                + "> in common table expression. Please update your query.",
                                            1);
        }
        SchemaColumn schemaCol = getOutputSchema().getColumn(idx);

        expr.setColumnIndex(idx);
        expr.setTypeSizeAndInBytes(schemaCol);
        return expr;
    }

    public abstract boolean canRunInOneFragment();

    public abstract boolean isOrderDeterministic(boolean orderIsDeterministic);

    public abstract String contentNonDeterminismMessage(String isContentDeterministic);

    public abstract boolean hasSignificantOffsetOrLimit(boolean hasSignificantOffsetOrLimit);

}
