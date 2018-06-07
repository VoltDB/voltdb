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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javafx.util.Pair;

import org.voltdb.catalog.*;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.Encoder;
import org.voltdb.expressions.*;
import org.json_voltpatches.JSONException;

/**
 * Tries to find a matching materialized view for the given SELECT statement, and rewrite the query.
 */
final class MVQueryRewriter {
    private final ParsedSelectStmt m_stmt;
    private MaterializedViewInfo m_mvi = null;
    /**
     * Relation of SELECT stmt's display column name, display column index ===>
     * VIEW table's column name, column index
     */
    private Map<Pair<String, Integer>, Pair<String, Integer>> m_rel = null;

    public MVQueryRewriter(ParsedSelectStmt stmt) {            // Constructor does not modify SELECT stmt
        m_stmt = stmt;
        if (!m_stmt.hasOrderByColumns() && m_stmt.isGrouped()) {   // MVI has GBY, and does not have OBY.
            final Optional<Pair<MaterializedViewInfo, Map<Pair<String, Integer>, Pair<String, Integer>>>>
                    any = getMviAndViews(m_stmt.m_tableList).entrySet().stream()          // Scan all MV associated with SEL source tables,
                    .flatMap(kv -> {
                        final MaterializedViewInfo mv = kv.getKey();                // Filter first by #columns of VIEW tables,
                        final Map<Pair<String, Integer>, Pair<String, Integer>> rel = gbyMatches(mv);
                        return rel != null ?                                      // and then by individual column index/agg type
                                Stream.of(new Pair<>(mv, rel)) : // tracing back to MV's source table
                                null;
                    }).findFirst();
            if (any.isPresent()) {                                                   // SELECT query can be rewritten
                final Pair<MaterializedViewInfo, Map<Pair<String, Integer>, Pair<String, Integer>>>
                        entry = any.get();
                m_rel = entry.getValue();
                m_mvi = entry.getKey();
            }
        }
    }

    /**
     * Try to rewrite SELECT stmt if there is a matching materialized view.
     * @return if SELECT stmt had been rewritten. Updates SELECT stmt transactionally.
     */
    public boolean rewrite() {
        if (m_mvi != null) {
            final Table view = m_mvi.getDest();
            final String viewName = view.getTypeName();
            // Get the map of select stmt's display column index -> view table (column name, column index)
            m_stmt.getFinalProjectionSchema()
                    .resetTableName(viewName, viewName)
                    .toTVEAndFixColumns(m_rel.entrySet().stream()
                            .collect(Collectors.toMap(kv -> kv.getKey().getKey(), Map.Entry::getValue)));
            // change to display column index-keyed map
            final Map<Integer, Pair<String, Integer>> colSubIndx =
                    m_rel.entrySet().stream().collect(Collectors.toMap(kv -> kv.getKey().getValue(), Map.Entry::getValue));
            ParsedSelectStmt.updateTableNames(m_stmt.m_aggResultColumns, viewName);
            ParsedSelectStmt.fixColumns(m_stmt.m_aggResultColumns, colSubIndx);
            ParsedSelectStmt.updateTableNames(m_stmt.m_displayColumns, viewName);
            ParsedSelectStmt.fixColumns(m_stmt.m_displayColumns, colSubIndx);
            m_stmt.rewriteAsMV(view);
            m_mvi = null; // makes this method re-entrant safe
            return true;
        } else {      // scans all sub-queries for rewriting opportunities
            return m_stmt.allScans().stream()
                    .map(scan -> scan instanceof StmtSubqueryScan && rewriteTableAlias((StmtSubqueryScan) scan))
                    .reduce(Boolean::logicalOr)
                    .equals(true);
        }
    }

    /**
     * Checks for any opportunity to rewrite sub-queries
     * @param scan target subquery
     * @return whether rewritten was applied
     */
    private static boolean rewriteTableAlias(StmtSubqueryScan scan) {
        final AbstractParsedStmt stmt = scan.getSubqueryStmt();
        return stmt instanceof ParsedSelectStmt && (new MVQueryRewriter((ParsedSelectStmt)stmt)).rewrite();
    }

    private boolean gbyTablesEqual(MaterializedViewInfo mv) {
        if (m_stmt.hasComplexGroupby() ^ mv.getGroupbyexpressionsjson().isEmpty()) {
            if (m_stmt.hasComplexGroupby()) {     // when both have complex GBY expressions, anonymize table/column of SEL stmt and compare the two expressions.
                // And check expression tree structure, ignoring table/column names.
                return getGbyExpressions(mv).equals(
                        m_stmt.groupByColumns().stream()  // convert PVE (as in "a = ?") to CVE, avoid modifying SELECT stmt
                                .map(ci -> copyAsCVE(ci.expression).anonymize())
                                .collect(Collectors.toList()));
            } else {    // when neither has complex GBY expression, check whether GBY table names match.
                return m_stmt.groupByColumns().get(0).tableName
                        .equals(mv.getDest().getMaterializer().getTypeName());
            }
        } else          // unequal when one has complex gby and the other doesn't
            return false;
    }

    private boolean gbyColumnsMatch(MaterializedViewInfo mv) {
        return m_stmt.hasComplexGroupby() ||  // if SEL has complex GBY expr, then at this point we already checked their column index matching.
                m_stmt.groupByColumns().stream()
                        .map(it -> it.columnName).collect(Collectors.toList())  // compare as lists because "GROUP BY a, b" and "GROUP BY b, a" are different
                        .equals(StreamSupport.stream(((Iterable<ColumnRef>) () -> mv.getGroupbycols().iterator()).spliterator(), false)
                                .map(cr -> cr.getColumn().getTypeName()).collect(Collectors.toList()));
    }

    /**
     * Apply matching rules of SELECT stmt against a materialized view, and gives back column relationship
     * between the two.
     * @pre !m_stmt.groupByColumns().isEmpty() -- guarded by constructor: m_stmt.isGrouped()
     * @param mv target materialized view
     * @return the map of (select stmt's display column name, select stmt's display column index) =>
     * (view table column name, view table column index) if matches; else null.
     */
    private Map<Pair<String, Integer>, Pair<String, Integer>> gbyMatches(MaterializedViewInfo mv) {
        final FilterMatcher filter = new FilterMatcher(m_stmt.m_joinTree.getJoinExpression(), predicate_of(mv));
        //  *** Matching criteria/order: ***
        // 1. Filters match;
        // 2. Select stmt contains group-by column;
        // 3. Group-by-columns' table is same as MV's source table;
        // 4. Those group-by-column's columns are same as MV's group-by columns;
        // 5. Select stmt's group-by column names match with MV's
        // 6. Each column's aggregation type match, in the sense of set equality;
        if(filter.match() && gbyTablesEqual(mv) && gbyColumnsMatch(mv)) {
            return getViewColumnMaps(mv);
        } else {
            return null;
        }
    }

    // Get the map of (select stmt's display column name, select stmt's display column index) =>
    //                  (view table column name, view table column index) if given materialized view's
    // columns matches with SELECT stmt; else return null.
    private Map<Pair<String, Integer>, Pair<String, Integer>> getViewColumnMaps(MaterializedViewInfo mv) {
        Iterable<Column> ciViewColumns = () -> mv.getDest().getColumns().iterator();
        final boolean hasComplexGroupBy = m_stmt.hasComplexGroupby();
        // deals with complex GBY expressions
        final List<AbstractExpression> mvGbys = hasComplexGroupBy ? getGbyExpressions(mv) : new ArrayList<>();
        final List<AbstractExpression> selGbyExpr = hasComplexGroupBy ?
                m_stmt.getGroupByColumns().stream().map(ci -> copyAsCVE(ci.expression).anonymize()).collect(Collectors.toList()) :
                new ArrayList<>();
        if (mvGbys.equals(selGbyExpr)) {                                   // When GBY expressions match
            final Map<Integer, Integer> selGbyColIndexMap =                // SELECT stmt GBY column index ==> VIEW table column index
                    m_stmt.displayColumns().stream().flatMap(ci -> {
                        if (ci.expression instanceof OperatorExpression) {
                            int indx = mvGbys.indexOf(copyAsCVE(ci.expression).anonymize());
                            if (indx >= 0) {
                                return Stream.of(new AbstractMap.SimpleEntry<>(ci.index, indx));
                            }
                        }
                        return null;
                    }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            // <aggType, viewSrcTblColIndx> ==> <viewTblColName, viewTblColIndx>
            final Map<Pair<Integer, Integer>, Pair<String, Integer>> nonGbyFromView =
                    StreamSupport.stream(ciViewColumns.spliterator(), false)
                            .skip(mvGbys.size())        // safe to use skip on view table, as first k columns are always in GBY
                            .collect(Collectors.toMap(col -> {
                                        final Column matCol = col.getMatviewsource();
                                        return new Pair<>(col.getAggregatetype(), matCol == null ? -1 : matCol.getIndex());
                                    },
                                    col -> new Pair<>(col.getTypeName(), col.getIndex())));
            // <aggType, viewSrcTblColIndx> ==> <displayColName, displayColIndx>
            final Map<Pair<Integer, Integer>, Pair<String, Integer>> nonGbyFromStmt =
                    m_stmt.displayColumns().stream()
                            .filter(ci -> !selGbyColIndexMap.containsKey(ci.index))
                            .map(ci -> {
                                final Pair<String, Integer> value = new Pair<>(ci.columnName, ci.index);
                                if (ci.expression instanceof AggregateExpression) {
                                    AbstractExpression left = ci.expression.getLeft();
                                    return new Pair<>(new Pair<>(ci.expression.getExpressionType().getValue(), // aggregation type value
                                            // aggregate on what? For simple column, it contains column index; for "count(*)", set to -1.
                                            left == null ? -1 : ((TupleValueExpression) left).getColumnIndex()), value);
                                } else {    // otherwise, it's either column or distinct column
                                    return new Pair<>(new Pair<>(ExpressionType.VALUE_TUPLE.getValue(),
                                            ((TupleValueExpression) ci.expression).getColumnIndex()), value);
                                }
                            })
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            if (nonGbyFromView.keySet().containsAll(nonGbyFromStmt.keySet())) {   // when {SELECT stmt non-GBY columns} \belongs {VIEW columns non-GBY columns}
                final List<String> gbyNames = StreamSupport.stream(ciViewColumns.spliterator(), false)
                        .limit(mvGbys.size()).map(Column::getTypeName).collect(Collectors.toList());
                final Map<Pair<String, Integer>, Pair<String, Integer>> rel = nonGbyFromStmt.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, kv -> nonGbyFromView.get(kv.getKey())));
                rel.putAll(m_stmt.m_displayColumns.stream()
                        .filter(ci -> selGbyColIndexMap.containsKey(ci.index))
                        .map(ci -> {
                            final Integer viewColIndx = selGbyColIndexMap.get(ci.index);
                            return new Pair<>(new Pair<>(ci.columnName, ci.index), new Pair<>(gbyNames.get(viewColIndx), viewColIndx));
                        }).collect(Collectors.toMap(Pair::getKey, Pair::getValue)));
                return rel;
            }
        }
        return null;
    }

    // returns all materialized view info => view table from table list
    private static Map<MaterializedViewInfo, Table> getMviAndViews(List<Table> tbls) {
        Map<MaterializedViewInfo, Table> map = new HashMap<>();
        tbls.forEach(tbl -> StreamSupport.stream(((Iterable<MaterializedViewInfo>) () -> tbl.getViews().iterator()).spliterator(), false)
                .forEach(mv -> map.put(mv, mv.getDest())));
        return map;
    }

    // Get a copy with all subexpressions that are PVE converted to CVE
    private static AbstractExpression copyAsCVE(AbstractExpression src) {
        AbstractExpression left = src.getLeft(), right = src.getRight();
        if (left != null) {
            left = copyAsCVE(left);
        }
        if (right != null) {
            right = copyAsCVE(right);
        }
        AbstractExpression dst = src instanceof ParameterValueExpression ?
                ((ParameterValueExpression) src).getOriginalValue().clone() : src.clone();
        dst.setLeft(left);
        dst.setRight(right);
        return dst;
    }

    // JSON deserializers -- pretend that deserialization will never fail
    private static AbstractExpression predicate_of(MaterializedViewInfo mv) {
        try {
            return AbstractExpression.fromJSONString(Encoder.hexDecodeToString(mv.getPredicate()), null);
        } catch (JSONException e) {
            return null;
        }
    }

    private static List<AbstractExpression> getGbyExpressions(MaterializedViewInfo mv) {
        try {
            return AbstractExpression.fromJSONArrayString(mv.getGroupbyexpressionsjson(), null);
        } catch (JSONException e) {
            return new ArrayList<>();
        }
    }

    /**
     * Recursively compares if two filter expressions match on the view table and select table.
     * Now, checks that expressions perfectly match each other, e.g. "WHERE a > b" and "WHERE b < a"
     * is considered mismatch. A fancier way to do some cumbersome recursive task.
     * It handles equivalence between ConstantValueExpression and ParameterValueExpression.
     *
     * Given filter expression from SELECT stmt's source table, filter expression from view table,
     * (mapping of  SELECT stmt's table column index ==> view table column index is NOT needed here, as
     *  the column are indexed on view's source table),
     * returns whether two filter expressions match with each other
     */
    private static final class FilterMatcher {
        private static final Set<ExpressionType> EXCHANGEABLE_EXPRESSIONS = new HashSet<ExpressionType>() {
            {
                add(ExpressionType.CONJUNCTION_AND);
                add(ExpressionType.CONJUNCTION_OR);
            }
        };
        private final AbstractExpression m_expr1, m_expr2;
        public FilterMatcher(AbstractExpression e1, AbstractExpression e2) {
            m_expr1 = e1;
            m_expr2 = e2;
        }
        public boolean match() {
            if (m_expr1 == null || m_expr2 == null) {
                return m_expr1 == null && m_expr2 == null;
            } else if (!exprTypesMatch(m_expr1, m_expr2)) {
                // Exception to the rule: comparisons could be reversed, e.g. "a >= b" and "b <= a" are the same relation
                return m_expr1 instanceof ComparisonExpression &&
                        m_expr1.getExpressionType().equals(ComparisonExpression.reverses.get(m_expr2.getExpressionType())) &&
                        (new FilterMatcher(((ComparisonExpression)m_expr1).reverseOperator(), m_expr2)).match();
            } else if (m_expr1 instanceof TupleValueExpression) {
                return tvesMatch((TupleValueExpression) m_expr1, (TupleValueExpression) m_expr2);
            } else if (m_expr1 instanceof VectorValueExpression) {
                return vectorsMatch((VectorValueExpression) m_expr1, (VectorValueExpression) m_expr2) &&
                        subExprsMatch(m_expr1, m_expr2);
            } else if(EXCHANGEABLE_EXPRESSIONS.contains(m_expr1.getExpressionType())) {
                // For AND/OR, left/right sub-expr are exhangeable
                return subExprsMatch(m_expr1, m_expr2) ||
                        ((new FilterMatcher(m_expr1.getLeft(), m_expr2.getRight())).match() &&
                                (new FilterMatcher(m_expr1.getRight(), m_expr2.getLeft())).match());
            } else
                return subExprsMatch(m_expr1, m_expr2);
        }
        private static boolean subExprsMatch(AbstractExpression e1, AbstractExpression e2) {
            return (new FilterMatcher(e1.getLeft(), e2.getLeft())).match() &&
                    (new FilterMatcher(e1.getRight(), e2.getRight())).match();
        }
        // Check that typeof() both args equal, with additional equivalence of one of
        // ParameterValueExpression type (from SELECT stmt); and the other from ConstantValueExpression
        // type (from VIEW definition).
        private static boolean exprTypesMatch(AbstractExpression e1, AbstractExpression e2) {
            return e1.getExpressionType().equals(e2.getExpressionType()) ||
                    // types must be either (PVE, CVE) or (CVE, PVE)
                    ((e1 instanceof ParameterValueExpression && e2 instanceof ConstantValueExpression ||
                            e1 instanceof ConstantValueExpression && e2 instanceof ParameterValueExpression) &&
                            asCVE(e1).equals(asCVE(e2)));
        }
        /**
         * Convert a ConstantValueExpression or ParameterValueExpression into a ConstantValueExpression.
         * \pre argument must be either of the two.
         * @param expr expression to be casted
         * @return casted ConstantValueExpression
         */
        private static ConstantValueExpression asCVE(AbstractExpression expr) {
            return expr instanceof ConstantValueExpression ? (ConstantValueExpression) expr :
                    ((ParameterValueExpression) expr).getOriginalValue();
        }
        /**
         * Compare two vectors as sets, e.g. "WHERE a in (1, 2, 3)" vs. "WHERE a in (2, 1, 3)"
         */
        private static boolean vectorsMatch(VectorValueExpression e1, VectorValueExpression e2) {
            return e1.getArgs().stream().map(FilterMatcher::asCVE).collect(Collectors.toSet())
                    .equals(e2.getArgs().stream().map(FilterMatcher::asCVE).collect(Collectors.toSet()));
        }
        /**
         * Matches two tuple value expressions, using column indexing map that converts SELECT stmt src table column indices
         * into VIEW table column indices.
         */
        private static boolean tvesMatch(TupleValueExpression sel, TupleValueExpression view) {
            return sel.getColumnIndex() == view.getColumnIndex() && subExprsMatch(sel, view);
        }
    }
}
