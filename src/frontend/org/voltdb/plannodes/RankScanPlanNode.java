/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.plannodes;

import java.util.List;
import java.util.Map.Entry;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.RankExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class RankScanPlanNode extends AbstractScanPlanNode {

    public enum Members {
        RANK_START_TYPE,
        RANK_END_TYPE,
        RANK_EXPRESSION,
        RANK_START_VALUE_EXPRESSION,
        RANK_END_VALUE_EXPRESSION,
    }
    protected IndexLookupType m_rankStartType;
    protected IndexLookupType m_rankEndType;
    protected RankExpression m_rankExpression;
    protected AbstractExpression m_rankStartExpression;
    protected AbstractExpression m_rankEndExpression;


    protected boolean m_isRankScan = false;

    public RankScanPlanNode() {
    }

    public RankScanPlanNode(AbstractScanPlanNode scan) {
        m_isRankScan = false;
        m_rankStartExpression = null;
        m_rankStartType = IndexLookupType.INVALID;
        m_rankEndExpression = null;
        m_rankEndType = IndexLookupType.INVALID;

        // copy inlined nodes
        for (Entry<PlanNodeType, AbstractPlanNode> entry : scan.getInlinePlanNodes().entrySet()) {
            addInlinePlanNode(entry.getValue());
        }

        if (scan instanceof SeqScanPlanNode) {
            SeqScanPlanNode seqScan = (SeqScanPlanNode) scan;
            m_isRankScan = setupRankScanNode(seqScan);
        }
    }

    public boolean isRankScan() {
        return m_isRankScan;
    }

    @Override
    public boolean isSubQuery() {
        return false;
    }

    @Override
    public PlanNodeType getPlanNodeType() {
        return PlanNodeType.RANKSCAN;
    }

    @Override
    public void generateOutputSchema(Database db)
    {
        super.generateOutputSchema(db);
    }

    @Override
    public void resolveColumnIndexes() {
        super.resolveColumnIndexes();
    }

    private void swapStartWithEnd() {
        AbstractExpression tmpExpr = m_rankStartExpression;
        IndexLookupType tmpType = m_rankStartType;

        m_rankStartExpression = m_rankEndExpression;
        m_rankStartType = m_rankEndType;

        m_rankEndExpression = tmpExpr;
        m_rankEndType = tmpType;
    }

    private boolean setupRankScanNode(SeqScanPlanNode seqScan) {
        if (seqScan.isSubQuery()) {
            return false;
        }
        AbstractExpression ex = seqScan.getPredicate();
        List<AbstractExpression> expressionList = ExpressionUtil.uncombine(ex);
        int rankIdx = findRankExpressionComparison(expressionList, true);
        if (rankIdx < 0) {
            return false;
        }
        if (! m_targetTableName.equals(seqScan.getTargetTableName())) {
            // running on difference tables
            return false;
        }
        // find the rank comparison expression
        expressionList.remove(rankIdx);

        rankIdx = findRankExpressionComparison(expressionList, false);
        if (rankIdx < 0) {
            m_rankEndType = IndexLookupType.INVALID;
        } else {
            expressionList.remove(rankIdx);
        }

        if (m_rankStartType == IndexLookupType.LT || m_rankStartType == IndexLookupType.LTE) {
            if (m_rankEndType == IndexLookupType.GT || m_rankEndType == IndexLookupType.GTE) {
                swapStartWithEnd();
            } else if (m_rankEndType == IndexLookupType.INVALID) {
                m_rankEndType = m_rankStartType;
                m_rankEndExpression = m_rankStartExpression;

                m_rankStartType = IndexLookupType.GTE;
                m_rankStartExpression = ConstantValueExpression.makeExpression(VoltType.BIGINT, "0");
            }
        }

        AbstractExpression newPredicate = ExpressionUtil.combine(expressionList);
        this.setPredicate(newPredicate);

        return true;
    }

    private int findRankExpressionComparison(List<AbstractExpression> expressList, boolean forStart) {
        int NOT_RANK_CANDIDATE = -1;

        for (int i = 00; i < expressList.size(); i++) {
            AbstractExpression ex = expressList.get(i);
            if (ex instanceof ComparisonExpression == false) continue;

            ComparisonExpression ce = (ComparisonExpression) ex;
            if (forStart) {
                m_rankStartType = setupLookupType(ce.getExpressionType());
                if (m_rankStartType == IndexLookupType.INVALID) {
                    continue;
                }

                if (ce.getLeft() instanceof RankExpression) {
                    m_rankExpression = (RankExpression) ce.getLeft();
                    m_rankStartExpression = ce.getRight();
                } else if (ce.getRight() instanceof RankExpression) {
                    m_rankExpression = (RankExpression) ce.getRight();
                    m_rankStartExpression = ce.getLeft();
                } else {
                    continue;
                }

                // temporary feature guards
                // The partition by is not supported right now
                assert(m_rankExpression != null);
                if (m_rankExpression.getPartitionbySize() > 0) {
                    continue;
                }

                // The descending is not supported right now
                if (m_rankExpression.isDecending()) {
                    continue;
                }

                // The rankStart and rankEnd has to be constant, no tuple involved
                if (! ExpressionUtil.findAllExpressionsOfClass(
                        m_rankStartExpression, TupleValueExpression.class).isEmpty()) {
                    continue;
                }

                // The partition by order by has to cover all the index definition
                if (! m_rankExpression.areAllIndexColumnsCovered()) {
                    continue;
                }

                m_targetTableName = m_rankExpression.getTableName();
            } else {
                m_rankEndType = setupLookupType(ce.getExpressionType());
                if (m_rankEndType == IndexLookupType.INVALID) {
                    continue;
                }

                RankExpression rankExpr = null;
                if (ce.getLeft() instanceof RankExpression) {
                    rankExpr = (RankExpression) ce.getLeft();
                    m_rankEndExpression = ce.getRight();
                } else if (ce.getRight() instanceof RankExpression) {
                    rankExpr = (RankExpression) ce.getRight();
                    m_rankEndExpression = ce.getLeft();
                } else {
                    // not a rank end expression candidate
                    continue;
                }

                if (! m_rankStartExpression.equals(rankExpr)) {
                    // not the same rank expression, skip for null
                    // in future, iterate them and find the best rank expression to use
                    continue;
                }

                if (! ExpressionUtil.findAllExpressionsOfClass(
                        m_rankEndExpression, TupleValueExpression.class).isEmpty()) {
                    continue;
                }
            }

            // return the ith index for the matched comparison expression
            return i;
        }

        return NOT_RANK_CANDIDATE;
    }

    public static IndexLookupType setupLookupType(ExpressionType type) {
        switch (type) {
        case COMPARE_GREATERTHAN:
            return IndexLookupType.GT;
        case COMPARE_GREATERTHANOREQUALTO:
            return IndexLookupType.GTE;
        case COMPARE_EQUAL:
            return IndexLookupType.EQ;
        case COMPARE_LESSTHANOREQUALTO:
            return IndexLookupType.LTE;
        case COMPARE_LESSTHAN:
            return IndexLookupType.LT;
        default:
            return IndexLookupType.INVALID;
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.RANK_START_TYPE.name()).value(m_rankStartType.toString());

        assert(m_rankExpression != null);
        stringer.key(Members.RANK_EXPRESSION.name());
        stringer.value(m_rankExpression);

        assert(m_rankStartExpression != null);
        stringer.key(Members.RANK_START_VALUE_EXPRESSION.name());
        stringer.value(m_rankStartExpression);

        if (m_rankEndType != IndexLookupType.INVALID) {
            stringer.key(Members.RANK_END_TYPE.name()).value(m_rankEndType.toString());

            stringer.key(Members.RANK_END_VALUE_EXPRESSION.name());
            stringer.value(m_rankEndExpression);
        }
    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {
        super.loadFromJSONObject(jobj, db);
        m_rankStartType = IndexLookupType.get( jobj.getString( Members.RANK_START_TYPE.name() ) );
        if (jobj.has("Members.RANK_END_TYPE.name()")) {
            m_rankEndType = IndexLookupType.get( jobj.getString( Members.RANK_END_TYPE.name() ) );
            m_rankEndExpression = AbstractExpression.fromJSONChild(
                    jobj, Members.RANK_END_VALUE_EXPRESSION.name(), m_tableScan);
        }
        m_rankExpression = (RankExpression) AbstractExpression.fromJSONChild(
                jobj, Members.RANK_EXPRESSION.name(), m_tableScan);
        m_rankStartExpression = AbstractExpression.fromJSONChild(
                jobj, Members.RANK_START_VALUE_EXPRESSION.name(), m_tableScan);
    }

    @Override
    protected String explainPlanForNode(String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append("Rank SCAN of \"").append(m_targetTableName).append("\" using ");
        sb.append(m_rankExpression.explain(getTableNameForExplain()))
        .append("\n" + indent + "scan matches for RANK value from ")
        .append(m_rankStartExpression.explain(m_targetTableName));

        if (m_rankEndExpression != null) {
            sb.append(" to ").append(m_rankEndExpression.explain(m_targetTableName));
        }

        sb.append(explainPredicate("\n" + indent + " filter by "));
        return sb.toString();
    }
}