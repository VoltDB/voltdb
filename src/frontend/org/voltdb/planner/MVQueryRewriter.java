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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.voltcore.utils.Pair;

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
    private final ParsedUnionStmt m_unionStmt;
    private final ParsedSelectStmt m_selectStmt;
    private MaterializedViewInfo m_mvi = null;
    /**
     * Relation of SELECT stmt's display column name, display column index ===>
     * VIEW table's column name, column index
     */
    private Map<Pair<String, Integer>, Pair<String, Integer>> m_rel = null;

    public MVQueryRewriter(ParsedSelectStmt stmt) {            // Constructor does not modify SELECT stmt
        m_unionStmt = null;
        m_selectStmt = stmt;
        if (m_selectStmt.m_tableList.size() == 1 &&                   // For now, support rewrite SELECT from a single table
                !m_selectStmt.hasOrderByColumns() && m_selectStmt.isGrouped() && m_selectStmt.getHavingPredicate() == null) {   // MVI has GBY, does not have OBY or HAVING clause
            final Optional<Pair<MaterializedViewInfo, Map<Pair<String, Integer>, Pair<String, Integer>>>>
                    any = getMviAndViews(m_selectStmt.m_tableList).entrySet().stream()          // Scan all MV associated with SEL source tables,
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
                m_rel = entry.getSecond();
                m_mvi = entry.getFirst();
            }
        }
    }

    public MVQueryRewriter(ParsedUnionStmt stmt) {
        m_unionStmt = stmt;
        m_selectStmt = null;    // not a SELECT
    }

    public boolean rewrite() {
        if (m_selectStmt != null) {
            return rewriteSelectStmt();
        } else {
            return rewriteUnionStmt(m_unionStmt);
        }
    }

    static private boolean rewriteUnionStmt(ParsedUnionStmt stmt) {
        final AtomicInteger indx = new AtomicInteger(0);
        final AtomicBoolean updated = new AtomicBoolean(false);
        stmt.m_children.stream().flatMap(c -> {
            if(c instanceof ParsedSelectStmt) {
                final ParsedSelectStmt s = (ParsedSelectStmt) c;
                if ((new MVQueryRewriter(s)).rewrite()) {
                    updated.set(true);
                    return Stream.of(new Pair<>(indx.getAndIncrement(), s));
                }
            } else if (c instanceof ParsedUnionStmt) {
                if ((new MVQueryRewriter((ParsedUnionStmt) c)).rewrite()) {
                    updated.set(true);
                }
            } else {
                assert(false);
            }
            return null;
        }).forEach(kv -> {         // update other fields: table list, table alias map, ?orderColumns
            final int i = kv.getFirst();
            final ParsedSelectStmt sel = kv.getSecond();
            assert (sel.m_tableList.size() == 1);        // For now, only MV from single table gets rewritten
            stmt.m_tableList.set(i, sel.m_tableList.get(0));
            final Table view = sel.m_tableList.get(0);
            stmt.m_tableAliasMap.forEach((k, v) -> { // awkward Map value match against (un-aliased) table name
                if (v.getTableName().equals(view.getMaterializer().getTypeName())) {
                    sel.generateStmtTableScan(view);
                }
            });
        });
        return updated.get();
    }

    /**
     * Try to rewrite SELECT stmt if there is a matching materialized view.
     * @return if SELECT stmt had been rewritten. Updates SELECT stmt transactionally.
     */
    private boolean rewriteSelectStmt() {
        if (m_mvi != null) {
            final Table view = m_mvi.getDest();
            final String viewName = view.getTypeName();
            // Get the map of select stmt's display column index -> view table (column name, column index)
            m_selectStmt.getFinalProjectionSchema()
                    .resetTableName(viewName, viewName)
                    .toTVEAndFixColumns(m_rel.entrySet().stream()
                            .collect(Collectors.toMap(kv -> kv.getKey().getFirst(), Map.Entry::getValue)));
            // change to display column index-keyed map
            final Map<Integer, Pair<String, Integer>> colSubIndx =
                    m_rel.entrySet().stream().collect(Collectors.toMap(kv -> kv.getKey().getSecond(), Map.Entry::getValue));
            ParsedSelectStmt.updateTableNames(m_selectStmt.m_aggResultColumns, viewName);
            ParsedSelectStmt.fixColumns(m_selectStmt.m_aggResultColumns, colSubIndx);
            ParsedSelectStmt.updateTableNames(m_selectStmt.m_displayColumns, viewName);
            ParsedSelectStmt.fixColumns(m_selectStmt.m_displayColumns, colSubIndx);
            m_selectStmt.rewriteAsMV(view);
            m_mvi = null; // makes this method re-entrant safe
            return true;
        } else {      // scans all sub-queries for rewriting opportunities
            return m_selectStmt.allScans().stream()
                    .map(scan -> scan instanceof StmtSubqueryScan && rewriteTableAlias((StmtSubqueryScan) scan))
                    .reduce(Boolean::logicalOr).get();
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
        if (m_selectStmt.hasComplexGroupby() ^ mv.getGroupbyexpressionsjson().isEmpty()) {
            if (m_selectStmt.hasComplexGroupby()) {     // when both have complex GBY expressions, anonymize table/column of SEL stmt and compare the two expressions.
                // And check expression tree structure, ignoring table/column names.
                return getGbyExpressions(mv).equals(
                        m_selectStmt.groupByColumns().stream()  // convert PVE (as in "a = ?") to CVE, avoid modifying SELECT stmt
                                .map(ci -> copyAsCVE(ci.m_expression).anonymize())
                                .collect(Collectors.toList()));
            } else {    // when neither has complex GBY expression, check whether GBY table names match.
                return m_selectStmt.groupByColumns().get(0).m_tableName
                        .equals(mv.getDest().getMaterializer().getTypeName());
            }
        } else          // unequal when one has complex gby and the other doesn't
            return false;
    }

    private boolean gbyColumnsMatch(MaterializedViewInfo mv) {
        return m_selectStmt.hasComplexGroupby() ||  // if SEL has complex GBY expr, then at this caller point we already checked their column index matching.
                m_selectStmt.groupByColumns().stream()    // compares GBY columns, ignoring order
                        .map(it -> it.m_columnName).collect(Collectors.toSet())
                        .equals(StreamSupport.stream(((Iterable<ColumnRef>) () -> mv.getGroupbycols().iterator()).spliterator(), false)
                                .map(cr -> cr.getColumn().getTypeName()).collect(Collectors.toSet()));
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
        final FilterMatcher filter = new FilterMatcher(m_selectStmt.m_joinTree.getJoinExpression(), predicate_of(mv));
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
        final boolean hasComplexGroupBy = m_selectStmt.hasComplexGroupby();
        // deals with complex GBY expressions
        final List<AbstractExpression> mvGbys = hasComplexGroupBy ? getGbyExpressions(mv) : new ArrayList<>();
        final Set<AbstractExpression> selGbyExpr = hasComplexGroupBy ?
                m_selectStmt.getGroupByColumns().stream().map(ci -> copyAsCVE(ci.m_expression).anonymize()).collect(Collectors.toSet()) :
                new HashSet<>();
        if (mvGbys.stream().collect(Collectors.toSet()).equals(selGbyExpr)) { // When GBY expressions match (ignoring order),
            final Map<Integer, Integer> selGbyColIndexMap =                // SELECT stmt GBY column index ==> VIEW table column index
                    m_selectStmt.displayColumns().stream().flatMap(ci -> {
                        if (ci.m_expression instanceof OperatorExpression || ci.m_expression instanceof FunctionExpression) {
                            int indx = mvGbys.indexOf(copyAsCVE(ci.m_expression).anonymize());
                            if (indx >= 0) {
                                return Stream.of(new AbstractMap.SimpleEntry<>(ci.m_index, indx));
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
                    m_selectStmt.displayColumns().stream()
                            .filter(ci -> !selGbyColIndexMap.containsKey(ci.m_index))
                            .map(ci -> {
                                final Pair<String, Integer> value = new Pair<>(ci.m_columnName, ci.m_index);
                                if (ci.m_expression instanceof AggregateExpression) {
                                    AbstractExpression left = ci.m_expression.getLeft();
                                    return new Pair<>(new Pair<>(ci.m_expression.getExpressionType().getValue(), // aggregation type value
                                            // aggregate on what? For simple column, it contains column index; for "count(*)", set to -1.
                                            left == null ? -1 : ((TupleValueExpression) left).getColumnIndex()), value);
                                } else {    // otherwise, it's either column or distinct column
                                    return new Pair<>(new Pair<>(ExpressionType.VALUE_TUPLE.getValue(),
                                            ((TupleValueExpression) ci.m_expression).getColumnIndex()), value);
                                }
                            })
                            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
            if (nonGbyFromView.keySet().containsAll(nonGbyFromStmt.keySet())) {   // when {SELECT stmt non-GBY columns} \belongs {VIEW columns non-GBY columns}
                final List<String> gbyNames = StreamSupport.stream(ciViewColumns.spliterator(), false)
                        .limit(mvGbys.size()).map(Column::getTypeName).collect(Collectors.toList());
                final Map<Pair<String, Integer>, Pair<String, Integer>> rel = nonGbyFromStmt.entrySet().stream()
                        .collect(Collectors.toMap(Map.Entry::getValue, kv -> nonGbyFromView.get(kv.getKey())));
                rel.putAll(m_selectStmt.m_displayColumns.stream()
                        .filter(ci -> selGbyColIndexMap.containsKey(ci.m_index))
                        .map(ci -> {
                            final Integer viewColIndx = selGbyColIndexMap.get(ci.m_index);
                            return new Pair<>(new Pair<>(ci.m_columnName, ci.m_index), new Pair<>(gbyNames.get(viewColIndx), viewColIndx));
                        }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
                return rel;
            }
        }
        return null;
    }

    // returns all materialized view info => view table from table list
    private static Map<MaterializedViewInfo, Table> getMviAndViews(List<Table> tbls) {
        return tbls.stream().flatMap(tbl ->
                StreamSupport.stream(((Iterable<MaterializedViewInfo>) () -> tbl.getViews().iterator()).spliterator(), false)
                        .map(mv -> new Pair<>(mv, mv.getDest())))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    }

    // Get a copy of *any* AbstractExpression, with itself and all subexpressions that are PVE converted to CVE
    // TODO: this might cause trouble with caching stored procedure with values and reusing it wrongly.
    // For scope of ENG-2878, caching would not cause this trouble because parameter
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

}
