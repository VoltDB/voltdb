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
package org.voltdb.expressions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import org.voltdb.utils.CatalogUtil;

public class RankExpression extends AbstractExpression {
    public enum Members {
        TARGET_TABLE_NAME,
        TARGET_INDEX_NAME,
        PARTITIONBY_SIZE,
        ORDERBY_SIZE,
        IS_DECENDING_ORDER;
    }

    private String m_tableName = null;
    private String m_indexName = null;
    private int m_partitionbySize = -1;
    private int m_orderbySize = -1;
    private boolean m_isDecending = false;

    private List<Index> m_indexCandidates = new ArrayList<Index>();
    private List<Integer> m_indexColumnsCovered = new ArrayList<Integer>();
    private Index m_index;

    private boolean m_areAllIndexColumnsCovered = false;

    public RankExpression() {
        //
        // This is needed for serialization
        //
        super();
    }

    public RankExpression(
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs,
            Database db, boolean isDecending)
    {
        super(ExpressionType.WINDOWING_RANK);
        m_tableName = findTableName(orderbyExprs);
        m_partitionbySize = partitionbyExprs.size();
        m_orderbySize = orderbyExprs.size();
        m_isDecending = isDecending;

        Index index = findTableIndex(partitionbyExprs, orderbyExprs, db);

        m_index = index;
        m_indexName = index.getTypeName();
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
        return m_orderbySize;
    }

    public void setOrderbySize(int orderbySize) {
        m_orderbySize = orderbySize;
    }

    public int getPartitionbySize() {
        return m_partitionbySize;
    }

    public void setPartitionbySize(int partitionbySize) {
        m_partitionbySize = partitionbySize;
    }

    public boolean isDecending() {
        return m_isDecending;
    }

    public void setIsDecending(boolean isDecending) {
        m_isDecending = isDecending;
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
        if (super.equals(obj) && obj instanceof RankExpression) {
            RankExpression rankExpr = (RankExpression) obj;
            if (rankExpr.getTableName().equals(m_tableName)
                    && rankExpr.getIndexName().equals(m_indexName)
                    && rankExpr.getPartitionbySize() == m_partitionbySize
                    && rankExpr.getOrderbySize() == m_orderbySize
                    && rankExpr.isDecending() == m_isDecending) {
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

        hash += m_partitionbySize;
        hash += m_orderbySize;
        hash += m_isDecending ? 1 : 0;

        return hash;
    }

    @Override
    public Object clone() {
        RankExpression clone = (RankExpression) super.clone();
        clone.setTableName(m_tableName);
        clone.setIndexName(m_indexName);
        clone.setPartitionbySize(m_partitionbySize);
        clone.setOrderbySize(m_orderbySize);
        clone.setIsDecending(m_isDecending);
        return clone;
    }

    @Override
    protected void loadFromJSONObject(JSONObject obj) throws JSONException {
        super.loadFromJSONObject(obj);
        m_tableName = obj.getString(Members.TARGET_TABLE_NAME.name());
        m_indexName = obj.getString(Members.TARGET_INDEX_NAME.name());
        m_partitionbySize = obj.getInt(Members.PARTITIONBY_SIZE.name());
        m_orderbySize = obj.getInt(Members.ORDERBY_SIZE.name());
        m_isDecending = obj.getInt(Members.IS_DECENDING_ORDER.name()) == 1 ? true : false;
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        super.toJSONString(stringer);

        stringer.key(Members.TARGET_TABLE_NAME.name()).value(m_tableName);
        stringer.key(Members.TARGET_INDEX_NAME.name()).value(m_indexName);
        stringer.key(Members.PARTITIONBY_SIZE.name()).value(m_partitionbySize);
        assert(m_orderbySize > 0);
        stringer.key(Members.ORDERBY_SIZE.name()).value(m_orderbySize);
        stringer.key(Members.IS_DECENDING_ORDER.name()).value(m_isDecending? 1 : 0);
    }

    @Override
    public void finalizeValueTypes() {
        m_valueType = VoltType.BIGINT;
        m_valueSize = VoltType.BIGINT.getLengthInBytesForFixedTypes();
    }

    @Override
    public String explain(String impliedTableName) {
        String str = "RANK expression using index " + m_indexName;
        //        List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(m_catalogIndex.getColumns(), "index");
        //        if (m_partitionbySize > 0) {
        //            str += " partition by " + indexedColRefs.get(m_partitionbySize - 1).getTypeName();
        //        }
        //        str += " order by ";
        //        for (int i = 0; i < m_orderbySize; i++) {
        //            str += indexedColRefs.get(m_partitionbySize + i).getTypeName() + " ";
        //        }

        if (m_partitionbySize > 0) {
            str += " partition by index column #" + (m_partitionbySize - 1);
        }
        str += " order by index column ";
        for (int i = 0; i < m_orderbySize; i++) {
            int idx = i + m_partitionbySize;
            str += "#" + idx + " ";
        }
        return str;
    }

    private static String findTableName(List<AbstractExpression> orderbyExprs) {
        String tableName = null;
        for (AbstractExpression expr : orderbyExprs) {
            List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(expr);
            for(TupleValueExpression tve: tves) {
                assert(tve.getTableName() != null);
                if (tableName == null) {
                    tableName = tve.getTableName();
                } else if (tableName != tve.getTableName()) {
                    throw new PlanningErrorException("RANK ORDER BY expression comes from multiple tables");
                }
            }
        }
        return tableName;
    }

    private Index findTableIndex(
            List<AbstractExpression> partitionbyExprs,
            List<AbstractExpression> orderbyExprs, Database db) {
        Table targetTable = db.getTables().get(m_tableName);
        assert(targetTable != null);
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
            throw new PlanningErrorException("RANK expressions of PARTITION BY and ORDER BY do not match any "
                    + "Tree INDEX defined in its table.");
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
            // TODO(xin): add support for expression index
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
        assert(m_indexColumnsCovered.size() > 0);
        assert(m_indexCandidates.size() > 0);

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
            if (! SubPlanAssembler.isPartialIndexPredicateIsCovered(tableScan, coveringExprs,
                    index, exactMatchCoveringExprs) ) {
                continue;
            }

            // may guard against more extra where clause filters

            //
            int left = m_indexColumnsCovered.get(i);
            refreshWithBestIndex(index, left);

            return;
        }
        throw new PlanningErrorException(
                "Rank clause without using partial index matching table where clause is not allowed.");
    }

}
