/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

public class WindowedExpression extends AbstractExpression {
    public enum Members {
        PARTITION_BY_EXPRESSIONS,
        ORDER_BY_EXPRESSIONS,
        ORDER_BY_DIRECTIONS,
        EXP,
        DIR
    }

    public static SortDirectionType DEFAULT_ORDER_BY_DIRECTION = SortDirectionType.ASC;

    private boolean m_isPercentRank = false;

    private String m_tableName = null;
    private String m_indexName = null;

    private List<Index> m_indexCandidates = new ArrayList<Index>();
    private List<Integer> m_indexColumnsCovered = new ArrayList<Integer>();
    private Index m_index;

    private boolean m_areAllIndexColumnsCovered = false;

    private List<AbstractExpression> m_partitionByExpressions = new ArrayList<>();
    private List<AbstractExpression> m_orderByExpressions = new ArrayList<>();
    private List<SortDirectionType>  m_orderByDirections = new ArrayList<>();

    public WindowedExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public WindowedExpression(
            ExpressionType operationType,  // RANK, MAX, etc.
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs,
            List<SortDirectionType>  orderByDirections,
            Database db, boolean isDecending, boolean isPercentRank)
    {
        super(operationType);
        m_tableName = findTableName(orderbyExprs);
        m_isPercentRank = isPercentRank;
        m_partitionByExpressions.addAll(partitionbyExprs);
        m_orderByExpressions.addAll(orderbyExprs);
        m_orderByDirections.addAll(orderByDirections);

        Index index = findTableIndex(partitionbyExprs, orderbyExprs, db);

        m_index = index;
        m_indexName = (m_index != null) ? index.getTypeName() : null;
    }


    public String getTableName() {
        return m_tableName;
    }

    public void setTableName(String tableName) {
        m_tableName = tableName;
    }

    public String getIndexName() {
        return m_indexName;
    }

    public void setIndexName(String indexName) {
        m_indexName = indexName;
    }

    public int getOrderbySize() {
        return m_orderByExpressions.size();
    }

    public int getPartitionbySize() {
        return m_partitionByExpressions.size();
    }

    public List<AbstractExpression> getPartitionByExpressions() {
        return m_partitionByExpressions;
    }

    public List<AbstractExpression> getOrderByExpressions() {
        return m_orderByExpressions;
    }

    public List<SortDirectionType> getOrderByDirections() {
        return m_orderByDirections;
    }

    public boolean isPercentRank() {
        return m_isPercentRank;
    }

    public void setIsPercentRank(boolean isPercentRank) {
        m_isPercentRank = isPercentRank;
    }

    public boolean areAllIndexColumnsCovered() {
        return m_areAllIndexColumnsCovered;
    }

    public boolean isIndexUnqiue() {
        return m_index.getUnique();
    }

    public Index getIndex() {
        return m_index;
    }

    public boolean isUsingPartialIndex() {
        return ! m_index.getPredicatejson().isEmpty();
    }

    public boolean hasPartitionTableIssue(Database db, Table targetTable, List<AbstractExpression> partitionbyExprs) {
        Column partitionCol = targetTable.getPartitioncolumn() ;
        if (partitionCol == null) {
            // replicated table has no partition data issue
            return false;
        }
        String colName = partitionCol.getTypeName();
        TupleValueExpression tve = new TupleValueExpression(
                m_tableName, m_tableName, colName, colName, partitionCol.getIndex());
        tve.setTypeSizeBytes(partitionCol.getType(), partitionCol.getSize(), partitionCol.getInbytes());

        boolean pkCovered = false;
        for (AbstractExpression ex: partitionbyExprs) {
            if (ex.equals(tve)) {
                pkCovered = true;
                break;
            }
        }
        // If PARTITION BY not covering PK, we can not use that index to calculate ranking
        // because our index is not distributed implemented.
        return ! pkCovered;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) && obj instanceof WindowedExpression) {
            WindowedExpression rankExpr = (WindowedExpression) obj;
            if (rankExpr.getTableName() == null) {
                return m_tableName == null;
            }
            if (m_tableName == null) {
                return false;
            }
            if (false == rankExpr.getTableName().equals(m_tableName)) {
                return false;
            }
            if (rankExpr.getIndexName() == null) {
                return m_indexName == null;
            }
            if (m_indexName == null) {
                return false;
            }
            if (false == rankExpr.getIndexName().equals(m_indexName)) {
                return false;
            }
            if (rankExpr.isPercentRank() == m_isPercentRank) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        if (m_tableName != null) {
            hash += m_tableName.hashCode();
        }
        if (m_indexName != null) {
            hash += m_indexName.hashCode();
        }

        hash += m_isPercentRank ? 1 : 0;

        return hash;
    }

    @Override
    public Object clone() {
        WindowedExpression clone = (WindowedExpression) super.clone();
        clone.setTableName(m_tableName);
        clone.setIndexName(m_indexName);
        clone.setIsPercentRank(m_isPercentRank);
        return clone;
    }

    @Override
    protected void loadFromJSONObject(JSONObject jobj) throws JSONException {
        super.loadFromJSONObject(jobj);
        m_partitionByExpressions.clear();
        m_orderByExpressions.clear();
        m_orderByDirections.clear();

        /*
         * Load the array of partition expressions.  The AbstractExpression class
         * has a useful routine to do just this for us.
         */
        AbstractExpression.loadFromJSONArrayChild(m_partitionByExpressions, jobj, Members.PARTITION_BY_EXPRESSIONS.name(), null);

        /*
         * Unfortunately we cannot use AbstractExpression.loadFromJSONArrayChild here,
         * as we need to get a sort expression and a sort order for each column.
         */
        if (jobj.has(Members.ORDER_BY_EXPRESSIONS.name())) {
            JSONArray jarray = jobj.getJSONArray(Members.ORDER_BY_EXPRESSIONS.name());
            int size = jarray.length();
            for (int ii = 0; ii < size; ii += 1) {
                JSONObject tempObj = jarray.getJSONObject(ii);
                m_orderByDirections.add( SortDirectionType.get(tempObj.getString( Members.DIR.name())) );
                m_orderByExpressions.add( AbstractExpression.fromJSONChild(tempObj, Members.EXP.name()) );
            }
        }
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);
        assert (m_orderByExpressions.size() == m_orderByDirections.size());
        /*
         * Serialize the sort columns.
         */
        stringer.key(Members.ORDER_BY_EXPRESSIONS.name()).array();
        for (int ii = 0; ii < m_orderByExpressions.size(); ii++) {
            stringer.object();
            stringer.key(Members.EXP.name());
            stringer.object();
            m_orderByExpressions.get(ii).toJSONString(stringer);
            stringer.endObject();
            stringer.key(Members.DIR.name()).value(m_orderByDirections.get(ii).toString());
            stringer.endObject();
        }
        stringer.endArray();

        /*
         * Serialize the partition expressions.
         */
        stringer.key(Members.PARTITION_BY_EXPRESSIONS.name()).array();
        for (int idx = 0; idx < m_partitionByExpressions.size(); idx += 1) {
            stringer.object();
            m_partitionByExpressions.get(idx).toJSONString(stringer);
            stringer.endObject();
        }
        stringer.endArray();
    }

    @Override
    public void finalizeValueTypes() {
//      if (m_isPercentRank) {
//          m_valueType = VoltType.DECIMAL;
//            m_valueSize = VoltType.DECIMAL.getLengthInBytesForFixedTypes();
//      } else {
//          m_valueType = VoltType.BIGINT;
//            m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
//      }

        // HACK!!! for Percent_rank look up later...
        m_valueType = VoltType.BIGINT;
        m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        StringBuilder sb = new StringBuilder();
        sb.append("WINDOW expression").append(" expression");
        if (m_index != null) {
            sb.append(" expression using index " + m_indexName);
        }
        return sb.toString();
    }

    private static String findTableName(List<AbstractExpression> orderbyExprs) {
        String tableName = null;
        for (AbstractExpression expr : orderbyExprs) {
            List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
            for(TupleValueExpression tve: tves) {
                if (tableName == null) {
                    tableName = tve.getTableName();
                    break;
                }
            }
        }
        return tableName;
    }

    private Index findTableIndex(
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs, Database db) {
        Table targetTable = db.getTables().get(m_tableName);
        // This could be a subquery.  It might not be a persistent
        // table.  That's ok, but there are no indices here.
        if (targetTable == null) {
            return null;
        }
        CatalogMap<Index> allIndexes = targetTable.getIndexes();

        for (Index index : allIndexes) {
            if ( ! IndexType.isScannable(index.getType())) {
                continue;
            }

            ArrayList<AbstractExpression> allExprs = new ArrayList<AbstractExpression>();
            allExprs.addAll(partitionbyExprs);

            if (hasPartitionTableIssue(db, targetTable, partitionbyExprs)) {
                continue;
            }
            allExprs.addAll(orderbyExprs);

            isExpressionListSubsetOfIndex(allExprs, index);
        }
        if (m_indexCandidates.size() == 0) {
            return null;
        }

        int minValue = Collections.min(m_indexColumnsCovered);
        int minIndex = m_indexColumnsCovered.indexOf(minValue);
        Index index = m_indexCandidates.get(minIndex);
        refreshWithBestIndex(index, minValue);
        return index;
    }

    private void isExpressionListSubsetOfIndex(List<AbstractExpression> exprs, Index index) {
        String exprsjson = index.getExpressionsjson();
        if (exprsjson.isEmpty()) {
            List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
            if (exprs.size() > indexedColRefs.size()) {
                return;
            }

            for (int j = 0; j < exprs.size(); j++) {
                AbstractExpression expr = exprs.get(j);
                if (expr instanceof TupleValueExpression == false) {
                    return;
                }
                String indexColumnName = indexedColRefs.get(j).getColumn().getName();
                String tveColumnName = ((TupleValueExpression)expr).getColumnName();
                if (! (tveColumnName.equals(indexColumnName))) {
                    return;
                }
            }
            // find index candidate
            Integer left = indexedColRefs.size() - exprs.size();
            assert(left >= 0);
            m_indexColumnsCovered.add(left);
            m_indexCandidates.add(index);
        } else {
            // TODO(xin): add support of expression index for rank
        }
    }

    private void refreshWithBestIndex(Index index, int indexLeftCovered) {
        m_index = index;
        m_indexName = index.getTypeName();
        m_areAllIndexColumnsCovered = indexLeftCovered == 0 ? true : false;
    }

    public void updateWithTheBestIndex(AbstractExpression predicate, StmtTableScan tableScan) {
        List<AbstractExpression> predicateList = ExpressionUtil.uncombine(predicate);
        List<AbstractExpression> coveringExprs = new ArrayList<AbstractExpression>();
        for (AbstractExpression expr: predicateList) {
            if (ExpressionUtil.containsTVEFromTable(expr, m_tableName)) {
                coveringExprs.add(expr);
            }
        }
        if (m_indexColumnsCovered.size() == 0
                || m_indexCandidates.size() == 0) {
            return;
        }

        if (coveringExprs.isEmpty()) {
            // case covered already
            return;
        }

        // where clause has predicates, check partial index filter and match them
        for (int i = 0 ; i < m_indexCandidates.size(); i++) {
            Index index = m_indexCandidates.get(i);
            // check partial index
            if (index.getPredicatejson().isEmpty()) {
                continue;
            }

            List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<AbstractExpression>();
            if (! SubPlanAssembler.isPartialIndexPredicateCovered(tableScan, coveringExprs,
                    index, exactMatchCoveringExprs) ) {
                continue;
            }

            // may guard against more extra where clause filters

            //
            int left = m_indexColumnsCovered.get(i);
            refreshWithBestIndex(index, left);

            return;
        }
        throw new PlanningErrorException( m_isPercentRank ? "PERCENT_RANK" : "RANK" +
                " clause without using partial index matching table where clause is not allowed.");
    }

    /**
     * Given an expression, E, in the partition by list, if E is in the
     * order by list, then return the order direction of E in that list.
     * Otherwise, return the default value.
     *
     * There's got to be a better way to do this.
     *
     * @param partitionByExpr
     * @return
     */
    public SortDirectionType getOrderByDirectionOfExpression(AbstractExpression partitionByExpr) {
        for (int idx = 0; idx < m_orderByExpressions.size(); idx += 1) {
            if (m_orderByExpressions.get(idx).equals(partitionByExpr)) {
                return m_orderByDirections.get(idx);
            }
        }
        return DEFAULT_ORDER_BY_DIRECTION;
    }
}

