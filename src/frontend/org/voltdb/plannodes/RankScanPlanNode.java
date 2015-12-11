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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.RankExpression;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.PlanNodeType;

public class RankScanPlanNode extends AbstractScanPlanNode {

    public enum Members {
        LOOKUP_TYPE,
        END_EXPRESSION,
    }
    protected RankExpression m_rankExpression;

    protected AbstractExpression m_searchkeyExpression;
    protected AbstractExpression m_endExpression;
    protected IndexLookupType m_lookupType = IndexLookupType.EQ;

    protected boolean m_isRankScan = false;

    public RankScanPlanNode(AbstractScanPlanNode scan) {
        if (scan instanceof SeqScanPlanNode) {
            SeqScanPlanNode seqScan = (SeqScanPlanNode) scan;
            m_isRankScan = isRankCandidate(seqScan);
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
    public void toJSONString(JSONStringer stringer) throws JSONException {

    }

    @Override
    public void loadFromJSONObject( JSONObject jobj, Database db ) throws JSONException {

    }

    @Override
    protected String explainPlanForNode(String indent) {
        return null;
    }

    private boolean isRankCandidate(SeqScanPlanNode seqScan) {
        if (seqScan.isSubQuery()) {
            return false;
        }
        // TODO(xin): add more guards like inlined aggregation check?

        AbstractExpression ex = seqScan.getPredicate();
        List<AbstractExpression> expressionList = ExpressionUtil.uncombine(ex);
        int rankIdx = findRankExpressionComparison(expressionList);
        if (rankIdx < 0) {
            return false;
        }
        // find the rank comparison expression
        expressionList.remove(rankIdx);

        AbstractExpression newPredicate = ExpressionUtil.combine(expressionList);
        this.setPredicate(newPredicate);

        return true;
    }

    private void setupLookupType(ExpressionType type) {
        switch (type) {
        case COMPARE_GREATERTHAN:
            m_lookupType = IndexLookupType.GT;
            break;
        case COMPARE_GREATERTHANOREQUALTO:
            m_lookupType = IndexLookupType.GTE;
            break;
        case COMPARE_EQUAL:
            m_lookupType = IndexLookupType.EQ;
            break;
        case COMPARE_LESSTHANOREQUALTO:
            m_lookupType = IndexLookupType.LTE;
            break;
        case COMPARE_LESSTHAN:
            m_lookupType = IndexLookupType.LT;
            break;
        default:
            m_lookupType = IndexLookupType.INVALID;
            break;
        }
    }

    private int findRankExpressionComparison(List<AbstractExpression> expressList) {
        int hasRank = -1;

        for (int i = 00; i < expressList.size(); i++) {
            AbstractExpression ex = expressList.get(i);
            if (ex instanceof ComparisonExpression == false) continue;

            ComparisonExpression ce = (ComparisonExpression) ex;
            setupLookupType(ce.getExpressionType());
            if (m_lookupType == IndexLookupType.INVALID) {
                return hasRank;
            }

            if (ce.getLeft() instanceof RankExpression) {
                m_rankExpression = (RankExpression) ce.getLeft();
                m_searchkeyExpression = ce.getRight();
            } else if (ce.getRight() instanceof RankExpression) {
                m_rankExpression = (RankExpression) ce.getRight();
                m_searchkeyExpression = ce.getLeft();
            } else {
                return hasRank;
            }

            return i;
        }

        return hasRank;
    }

}