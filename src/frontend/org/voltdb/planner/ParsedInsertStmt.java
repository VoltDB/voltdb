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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;

/**
 *
 *
 */
public class ParsedInsertStmt extends AbstractParsedStmt {

    /**
     * A hash of the columns that were provided in the insert stmt,
     * and the corresponding values that were provided.  It is a
     * linked hash map so we retain the order in which the user
     * specified the columns.
     */
    public LinkedHashMap<Column, AbstractExpression> m_columns = new LinkedHashMap<>();

    /**
     * The SELECT statement for INSERT INTO ... SELECT.
     */
    private StmtSubqueryScan m_subquery = null;
    /**
     * An empty list for INSERT ... VALUES statements
     * until they support subquery-based expressions,
     * OR a list with one entry for an INSERT ... SELECT statement,
     * to represent the top-level SELECT.
     */
    private final List<StmtSubqueryScan> m_scans = new ArrayList<>();



    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedInsertStmt(AbstractParsedStmt parent, String[] paramValues, Database db) {
        super(parent, paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        // An INSERT statement may have table scans if its an INSERT INTO ... SELECT,
        // but those table scans will belong to the corresponding ParsedSelectStmt
        assert(m_tableList.isEmpty());

        String tableName = stmtNode.attributes.get("table");
        // Need to add the table to the cache. It may be required to resolve the
        // correlated TVE in case of WHERE clause contains IN subquery
        Table table = getTableFromDB(tableName);
        addTableToStmtCache(table, tableName);

        m_tableList.add(table);

        for (VoltXMLElement node : stmtNode.children) {
            if (node.name.equals("columns")) {
                parseTargetColumns(node, table, m_columns);
            }
            else if (node.name.equals(SELECT_NODE_NAME)) {
                m_subquery = new StmtSubqueryScan (parseSubquery(node), "__VOLT_INSERT_SUBQUERY__");
                // Until scalar subqueries are allowed in INSERT ... VALUES statements,
                // The top-level SELECT subquery in an INSERT ... SELECT statement
                // is the only possible subselect in an INSERT statement.
                m_scans.add(m_subquery);
            }
            else if (node.name.equals(UNION_NODE_NAME)) {
                throw new PlanningErrorException(
                        "INSERT INTO ... SELECT is not supported for UNION or other set operations.");
            }
        }
        calculateContentDeterminismMessage();
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "COLUMNS:\n";
        for (Entry<Column, AbstractExpression> col : m_columns.entrySet()) {
            retval += "\tColumn: " + col.getKey().getTypeName();
            if (col.getValue() != null) {
                retval += ": " + col.getValue().toString();
            }
            retval += "\n";
        }

        if (getSubselectStmt() != null) {
            retval += "SUBSELECT:\n";
            retval += getSubselectStmt().toString();
        }
        return retval;
    }

    private AbstractExpression defaultValueToExpr(Column column) {
        AbstractExpression expr = null;

        boolean isConstantValue = true;
        if (column.getDefaulttype() == VoltType.TIMESTAMP.getValue()) {

            boolean isFunctionFormat = true;
            String timeValue = column.getDefaultvalue();
            try {
                Long.parseLong(timeValue);
                isFunctionFormat = false;
            } catch (NumberFormatException  e) {}
            if (isFunctionFormat) {
                try {
                    java.sql.Timestamp.valueOf(timeValue);
                    isFunctionFormat = false;
                } catch (IllegalArgumentException e) {}
            }

            if (isFunctionFormat) {
                String name = timeValue.split(":")[0];
                int id = Integer.parseInt(timeValue.split(":")[1]);

                FunctionExpression funcExpr = new FunctionExpression();
                funcExpr.setAttributes(name, null, null, id);

                funcExpr.setValueType(VoltType.TIMESTAMP);
                funcExpr.setValueSize(VoltType.TIMESTAMP.getMaxLengthInBytes());

                expr = funcExpr;
                isConstantValue = false;
            }
        }
        if (isConstantValue) {
            // Not Default sql function.
            ConstantValueExpression const_expr = new ConstantValueExpression();
            expr = const_expr;
            if (column.getDefaulttype() != 0) {
                const_expr.setValue(column.getDefaultvalue());
                const_expr.refineValueType(VoltType.get((byte) column.getDefaulttype()), column.getSize());
            }
            else {
                const_expr.setValue(null);
                const_expr.refineValueType(VoltType.get((byte) column.getType()), column.getSize());
            }
        }

        assert(expr != null);
        return expr;
    }

    public AbstractExpression getExpressionForPartitioning(Column column) {
        AbstractExpression expr = null;
        if (getSubselectStmt() != null) {
            // This method is used by statement partitioning to help infer single partition statements.
            // Caller expects a constant or parameter to be returned.
            return null;
        }
        else {
            expr = m_columns.get(column);
            if (expr == null) {
                expr = defaultValueToExpr(column);
            }
        }

        assert(expr != null);
        return expr;
    }

    public StmtSubqueryScan getSubqueryScan() { return m_subquery; }

    /**
     * Return the subqueries for this statement.  For INSERT statements,
     * there can be only one.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public List<StmtEphemeralTableScan> getEphemeralTableScans() {
        // m_scans is a list of StmtSubqueryScan, which is
        // a subclass of StmtEphemeralTableScan.  So this should
        // be ok.
        return (List)m_scans;
    }

    /**
     * @return the subquery for the insert stmt if there is one, null otherwise
     */
    private ParsedSelectStmt getSubselectStmt() {
        if (m_subquery != null) {
            return (ParsedSelectStmt)(m_subquery.getSubqueryStmt());
        }
        return null;
    }

    @Override
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        assert(getSubselectStmt() != null);
        return getSubselectStmt().isOrderDeterministicInSpiteOfUnorderedSubqueries();
    }

    @Override
    public Set<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        Set<AbstractExpression> exprs = super.findAllSubexpressionsOfClass(aeClass);

        for (AbstractExpression expr : m_columns.values()) {
            if (expr == null) {
                continue;
            }
            exprs.addAll(expr.findAllSubexpressionsOfClass(aeClass));
        }

        if (m_subquery != null) {
            exprs.addAll(m_subquery.getSubqueryStmt().findAllSubexpressionsOfClass(aeClass));
        }

        return exprs;
    }

    /**
     * Return the content determinism string of the subquery if there is one.
     */
    @Override
    public String calculateContentDeterminismMessage() {
        String ans = getContentDeterminismMessage();
        if (ans != null) {
            return ans;
        }
        if (m_subquery != null) {
            updateContentDeterminismMessage(m_subquery.calculateContentDeterminismMessage());
            return getContentDeterminismMessage();
        }
        if (m_columns != null) {
            for (AbstractExpression expr : m_columns.values()) {
                String emsg = expr.getContentDeterminismMessage();
                if (emsg != null) {
                    updateContentDeterminismMessage(emsg);
                    return emsg;
                }
            }
        }
        return null;
    }

    @Override
    public boolean isDML() { return true; }

    @Override
    protected void parseCommonTableExpressions(VoltXMLElement root) {
        // No with statements here.
    }

}
