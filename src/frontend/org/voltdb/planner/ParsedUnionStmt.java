/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.SubqueryExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.types.ExpressionType;

public class ParsedUnionStmt extends AbstractParsedStmt {

    public enum UnionType {
        NOUNION,
        UNION,
        UNION_ALL,
        INTERSECT,
        INTERSECT_ALL,
        EXCEPT_ALL,
        EXCEPT
    };

    public ArrayList<AbstractParsedStmt> m_children = new ArrayList<AbstractParsedStmt>();
    public UnionType m_unionType = UnionType.NOUNION;

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUnionStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String type = stmtNode.attributes.get("uniontype");
        // Set operation type
        m_unionType = UnionType.valueOf(type);

        assert(stmtNode.children.size() == m_children.size());
        int i = 0;
        for (VoltXMLElement selectSQL : stmtNode.children) {
            AbstractParsedStmt nextSelectStmt = m_children.get(i++);
            nextSelectStmt.parse(selectSQL);
        }
    }

    /**Parse tables and parameters
     *
     * @param root
     * @param db
     */
    @Override
    void parseTablesAndParams(VoltXMLElement stmtNode) {

        assert(stmtNode.children.size() > 1);
        m_tableList.clear();
        for (VoltXMLElement childSQL : stmtNode.children) {
            if (childSQL.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                AbstractParsedStmt childStmt = new ParsedSelectStmt(this.m_paramValues, this.m_db);
                // Assign every child a unique ID
                childStmt.m_stmtId = AbstractParsedStmt.NEXT_STMT_ID++;
                childStmt.m_parentStmt = m_parentStmt;
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);

                // Add statement's tables to the consolidated list
                m_tableList.addAll(childStmt.m_tableList);
            } else if (childSQL.name.equalsIgnoreCase(UNION_NODE_NAME)) {
                ParsedUnionStmt childStmt = new ParsedUnionStmt(this.m_paramValues, this.m_db);
                // Set the parent before recursing to children.
                childStmt.m_parentStmt = m_parentStmt;
                childStmt.parseTablesAndParams(childSQL);
                m_children.add(childStmt);
                // Add statement's tables to the consolidated list
                m_tableList.addAll(childStmt.m_tableList);
            } else {
                throw new PlanningErrorException("Unexpected Element in UNION statement: " + childSQL.name);
            }
        }
    }

    /**Miscellaneous post parse activity
     * .
     * @param sql
     * @param joinOrder
     */
    @Override
    void postParse(String sql, String joinOrder) {
        for (AbstractParsedStmt selectStmt : m_children) {
            selectStmt.postParse(sql, joinOrder);
        }

        m_sql = sql;
        m_joinOrder = joinOrder;
    }

    @Override
    public boolean isOrderDeterministic() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( ! childStmt.isOrderDeterministic()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean hasLimitOrOffset() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( childStmt.hasLimitOrOffset()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        for (AbstractParsedStmt childStmt : m_children) {
            if ( ! childStmt.isOrderDeterministicInSpiteOfUnorderedSubqueries()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<StmtSubqueryScan> findAllFromSubqueries() {
        List<StmtSubqueryScan> subqueries = new ArrayList<StmtSubqueryScan>();
        for (AbstractParsedStmt childStmt : m_children) {
            subqueries.addAll(childStmt.findAllFromSubqueries());
        }
        return subqueries;
    }

    @Override
    public List<AbstractExpression> findAllSubexpressionsOfType(ExpressionType exprType) {
        List<AbstractExpression> exprs = new ArrayList<AbstractExpression>();
        for (AbstractParsedStmt childStmt : m_children) {
            exprs.addAll(childStmt.findAllSubexpressionsOfType(exprType));
        }
        return exprs;
    }

    /**
     * Break up UNION/INTERSECT (ALL) set ops into individual selects that are part
     * of the IN/EXISTS subquery into multiple expressions for each set op child
     * combined by the conjunction AND/OR expression.
     * col IN ( queryA UNION queryB ) - > col IN (queryA) OR col IN (queryB)
     * col IN ( queryA INTERSECTS queryB ) - > col IN (queryA) AND col IN (queryB)
     * The EXCEPT set op is LEFT as is
     * Also the ALL qualifier is dropped because IN/EXISTS expressions only
     * need just one tuple in the results set
     *
     * @param inExistsExpr - IN/EXISTS expression with a possible SET OP subquery
     * @return simplified expression
     */
    protected static AbstractExpression breakUpSetOpSubquery(AbstractExpression inExistsExpr) {
        ExpressionType subqueryExprType = inExistsExpr.getExpressionType();
        AbstractExpression subqueryExpr = (ExpressionType.COMPARE_IN == subqueryExprType) ?
                inExistsExpr.getRight() : inExistsExpr.getLeft();
        assert (subqueryExpr instanceof SubqueryExpression);
        AbstractParsedStmt subquery = ((SubqueryExpression) subqueryExpr).getSubquery();
        if (!(subquery instanceof ParsedUnionStmt)) {
            return inExistsExpr;
        }
        ParsedUnionStmt setOpStmt = (ParsedUnionStmt) subquery;
        if (UnionType.EXCEPT == setOpStmt.m_unionType || UnionType.EXCEPT_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.EXCEPT;
            return inExistsExpr;
        }
        if (UnionType.UNION_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.UNION;
        } else if (UnionType.INTERSECT_ALL == setOpStmt.m_unionType) {
            setOpStmt.m_unionType = UnionType.INTERSECT;
        }
        ExpressionType conjuctionType = (setOpStmt.m_unionType == UnionType.UNION) ?
                ExpressionType.CONJUNCTION_OR : ExpressionType.CONJUNCTION_AND;
        AbstractExpression retval = null;
        AbstractParsedStmt parentStmt = subquery.m_parentStmt;
        // It's a subquery which meant it must have a parent
        assert (parentStmt != null);
        for (AbstractParsedStmt child : setOpStmt.m_children) {
            String tableName = "VOLT_TEMP_TABLE_" + child.m_stmtId;
            // add table to the query cache
            StmtTableScan tableCache = parentStmt.addTableToStmtCache(tableName, tableName, child);
            assert(tableCache instanceof StmtSubqueryScan);
            SubqueryExpression childSubquery = new SubqueryExpression((StmtSubqueryScan)tableCache);
            AbstractExpression childSubqueryExpr = (ExpressionType.COMPARE_IN == subqueryExprType) ?
                    new ComparisonExpression(subqueryExprType, inExistsExpr.getLeft(), childSubquery) :
                        new OperatorExpression(subqueryExprType, childSubquery, null);
            // Recurse
            childSubqueryExpr = ParsedUnionStmt.breakUpSetOpSubquery(childSubqueryExpr);
            if (retval == null) {
                retval = childSubqueryExpr;
            } else {
                retval = new ConjunctionExpression(conjuctionType, retval, childSubqueryExpr);
            }
        }
        return retval;
    }
}
