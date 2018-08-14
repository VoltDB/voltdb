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
import java.util.function.Function;
import java.util.function.UnaryOperator;
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
import scala.Int;

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
    private Map<Pair<String, Integer>, Pair<String, Integer>> m_QueryColumnNameAndIndx_to_MVColumnNameAndIndx = null;

    public MVQueryRewriter(ParsedSelectStmt stmt) {            // Constructor does not modify SELECT stmt
        m_unionStmt = null;
        m_selectStmt = stmt;
        // NOTE: a MV creation stmt can be without group-by, e.g. "SELECT min(c1), max(c2), COUNT(*) FROM FOO"
        if (m_selectStmt.m_tableList.size() == 1 &&                   // For now, support rewrite SELECT from a single table
                !m_selectStmt.hasOrderByColumns() && m_selectStmt.getHavingPredicate() == null) {   // MVI has GBY, does not have OBY or HAVING clause
            final Optional<Pair<MaterializedViewInfo, Map<Pair<String, Integer>, Pair<String, Integer>>>>
                    any = getMviAndViews(m_selectStmt.m_tableList).entrySet().stream()          // Scan all MV associated with SEL source tables,
                    .flatMap(kv -> {
                        final MaterializedViewInfo mv = kv.getKey();                // Filter first by #columns of VIEW tables,
                        final Map<Pair<String, Integer>, Pair<String, Integer>> rel = gbyMatches(mv);
                        return rel != null ?                                      // and then by individual column index/agg type
                                Stream.of(Pair.of(mv, rel)) : // tracing back to MV's source table
                                null;
                    }).findFirst();
            if (any.isPresent()) {                                                   // SELECT query can be rewritten
                final Pair<MaterializedViewInfo, Map<Pair<String, Integer>, Pair<String, Integer>>>
                        entry = any.get();
                m_QueryColumnNameAndIndx_to_MVColumnNameAndIndx = entry.getSecond();
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
        // indx/updated are captured inside lambda (required to be {\code final}).
        // The whole parsing/optimization job is done synchronously in a single thread.
        final AtomicInteger indx = new AtomicInteger(0);
        final AtomicBoolean updated = new AtomicBoolean(false);
        stmt.m_children.stream().flatMap(c -> {
            if (c instanceof ParsedSelectStmt) {
                final ParsedSelectStmt s = (ParsedSelectStmt) c;
                if ((new MVQueryRewriter(s)).rewrite()) {
                    updated.set(true);
                    return Stream.of(Pair.of(indx.getAndIncrement(), s));
                }
            } else if (c instanceof ParsedUnionStmt) {
                if ((new MVQueryRewriter((ParsedUnionStmt) c)).rewrite()) {
                    updated.set(true);
                }
            } else {
                assert(false);
            }
            return null;
        }).forEach(kv -> {         // update other fields: table list, table alias map
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
                    .toTVEAndFixColumns(m_QueryColumnNameAndIndx_to_MVColumnNameAndIndx.entrySet().stream()
                            .collect(Collectors.toMap(kv -> kv.getKey().getFirst(), Map.Entry::getValue)));
            // change to display column index-keyed map
            final Map<Integer, Pair<String, Integer>> colSubIndx = m_QueryColumnNameAndIndx_to_MVColumnNameAndIndx
                    .entrySet().stream().collect(Collectors.toMap(kv -> kv.getKey().getSecond(), Map.Entry::getValue));
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

    /**
     * Checks if the group-by column/expression of given candidate MV matches with that of SELECT stmt.
     * To do this, we first check if both or neither of the stmt and MV contains complex group-by expressions (e.g.
     * "group by a + b" or "group by abs(a)"):
     * 1. When both contain complex group-by expressions, extract the group-by expressions, transform those expressions to:
     *   1.1 Change any PVE in the SELECT stmt to CVE. This is because the MV contains only CVE, while a SELECT stmt and
     *       stored procedure represents constants as PVE.
     *   1.2 Erase table names/aliases and column names/aliases in the expressions of SELECT stmt and MV, leaving only
     *       column indices. This is because view is stored as a different table, whose columns are aliases when creating
     *       the view; and SELECT stmt could alias table/columns too.
     * 2. When neither contains any complex group-by expressions, check that source tables of the group-by columns should
     * match.
     * \pre both SELECT stmt and candidate MV has a single table source. This is guaranteed in the constructor logic.
     *
     * @param mv candidate materialized view
     * @return whether given candidate MV might match SELECT stmt, by looking only at the group-by expressions.
     */
    private boolean gbyTablesEqual(MaterializedViewInfo mv) {
        if (m_selectStmt.hasComplexGroupby() != mv.getGroupbyexpressionsjson().isEmpty()) {
            if (m_selectStmt.hasComplexGroupby()) {     // when both have complex GBY expressions, anonymize table/column of SEL stmt and compare the two expressions.
                final Set<AbstractExpression>
                        selGby = m_selectStmt.groupByColumns().stream()
                        .map(ci -> transformExpressionRidofPVE(ci.m_expression).anonymize())
                        .collect(Collectors.toSet()),
                        viewGby = new HashSet<>(getGbyExpressions(mv));
                return selGby.equals(viewGby) &&        // NOTE: as TVE's equal() method misses column index comparison,
                        // which is almost the whole point, and we need to do the checking here. This is quite hacky, but
                        // is the only way to do it here.
                        selGby.stream().map(MVQueryRewriter::extractTVEIndices).collect(Collectors.toSet()).equals(
                                viewGby.stream().map(MVQueryRewriter::extractTVEIndices).collect(Collectors.toSet()));
            } else {    // when neither has complex GBY expression, we already know that GBY table names match, since
                return true;    // we are only checking SELECT query from a single table.
            }
        } else {        // unequal when one has complex gby and the other doesn't
            return false;
        }
    }

    /**
     * Helper method to extract all TVE column indices from an expression.
     * @param e source expression to check/extract
     * @param accum accumulator
     * @return accumulated indices.
     */
    private static List<Integer> extractTVEIndices(AbstractExpression e, List<Integer> accum) {
        if (e != null) {
            if (e instanceof TupleValueExpression) {
                accum.add(((TupleValueExpression) e).getColumnIndex());
            } else {
                extractTVEIndices(e.getRight(), extractTVEIndices(e.getLeft(), accum));
                if (e.getArgs() != null) {
                    e.getArgs().forEach(ex -> extractTVEIndices(ex, accum));
                }
            }
        }
        return accum;
    }

    /**
     * Map/extract all internal TVE's column indices in the order of left, right, args.
     * This is quite hacky, but we need to do it here because the equal() method in TVE does not compare the
     * column index, which is the whole point of comparing two TVEs.
     * @param e expression
     * @return All TVE indices inside expression, e.g. when e is a TVE, a function with TVE arg(s), a VVE with TVEs, etc.
     */
    private static List<Integer> extractTVEIndices(AbstractExpression e) {
        return extractTVEIndices(e, new ArrayList<>());
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
        // 2. Group-by-columns' table is same as MV's source table;
        // 3. Those group-by-column's columns are same as MV's group-by columns;
        // 4. Select stmt's group-by column names match with MV's
        // 5. Each column's aggregation type match, in the sense of set equality;
        if (filter.match() && gbyTablesEqual(mv) && gbyColumnsMatch(mv)) {
            return getViewColumnMaps(mv);
        } else {
            return null;
        }
    }

    /**
     * Get the map of (select stmt's display column name, select stmt's display column index) =>
     *                   (view table column name, view table column index) if given materialized view's
     * columns matches with SELECT stmt; else return null.
     *
     * This mapping is useful, because ultimately in the query rewriting we need to transform column indices or expressions
     * from original table to MV table. To do this:
     * 1. Find map from SELECT stmt's GBY column index to MV table column index, if GBY contains complex expressions.
     * 2. Find map from pair {aggregation type (min, max, sum, etc.), table column index} to pair {MV table column name,
     *    MV table column index}. e.g. SELECT stmt "... ABS(c1), ... FROM FOO" is an entry of {"ABS", "c1"} ==> {"mv_c1", 1}
     *    if the MV was created as "SELECT ABS(c1) AS mv_c1, ... FROM FOO", for all the columns from MV table that are NOT in
     *    GBY column/expression.
     * 3. Find map from same source pair to pair {SELECT stmt display column name and column index} for all the display columns
     *    from SELECT that are NOT in the GBY column/expression.
     * 4. Now (right-inner) join the two maps and check that the SELECT stmt's non-GBY display columns must be a subset of
     *    MV's non-GBY columns. Only if this is satisfied can SELECT query be answered by the given MV table.
     * 5. When join succeeds, augment the joined relation with GBY relation, to get the complete map.
     *
     * \pre SELECT stmt and MV have identical GBY expressions.
     * @param mv candidate materialized view
     * @return (select stmt's display column name, select stmt's display column index) =>
     * (view table column name, view table column index) if candidate MV's columns match with SELECT stmt;
     * null otherwise.
     */
    private Map<Pair<String, Integer>, Pair<String, Integer>> getViewColumnMaps(MaterializedViewInfo mv) {
        // A functor to iterate table columns from given MV, the first k entries are k GBY columns/expressions.
        final Iterable<Column> ciViewColumns = () -> mv.getDest().getColumns().iterator();
        // NOTE: to establish mapping between GBY expressions, we have to maintain some order on either (but not both)
        // of MV GBY expression, or SELECT stmt GBY expression.
        final List<AbstractExpression> mvGbys =          // Ordered collection of MV's group-by expressions (ordered by MV's creation stmt)
                m_selectStmt.hasComplexGroupby() ? getGbyExpressions(mv) : new ArrayList<>(); //  when it contains complex GBY expression.
        // Assertion that both MV and SELECT stmt have same group-by expressions. This is guaranteed by caller in gbyMatchers()
        // method, which is guaranteed by calling gbyTablesEqual() method.
        assert(new HashSet<>(mvGbys).equals(m_selectStmt.hasComplexGroupby() ?
                m_selectStmt.getGroupByColumns().stream()
                        .map(ci -> transformExpressionRidofPVE(ci.m_expression).anonymize())
                        .collect(Collectors.toSet()) : new HashSet<>()));              // warranted by precondition (gbyMatcher() caller).
        final Map<Integer, Integer> selGbyColIndexMap = // SELECT stmt GBY column index ==> VIEW table column index
                m_selectStmt.displayColumns().stream().flatMap(ci -> {
                    // find index of a SELECT's GBY expression into MV's GBY expression
                    final int index = mvGbys.indexOf(transformExpressionRidofPVE(ci.m_expression).anonymize());
                    return index >= 0 ?     // mark index only for GBY expressions.
                            Stream.of(new AbstractMap.SimpleEntry<>(ci.m_index, index)) : Stream.empty();
                }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        // <aggType, viewSrcTblColIndx> ==> <viewTblColName, viewTblColIndx>
        final Map<Pair<Integer, Integer>, Pair<String, Integer>> nonGbyFromView =
                StreamSupport.stream(ciViewColumns.spliterator(), false)    // iterate over MV table columns,
                        .skip(mvGbys.size())        // where the first k columns correspond to k GBY columns/expressions,
                        .collect(Collectors.toMap(col -> {
                                    final Column matCol = col.getMatviewsource();   // source column of MV column/aggregation
                                    return Pair.of(col.getAggregatetype(),
                                            matCol == null ? -1 : matCol.getIndex());    // -1 when it's arithmetic expression like "C1 - C2"
                                },
                                col -> Pair.of(col.getTypeName(), col.getIndex()),
                                (a, b) -> a));      // dedup when MV contains duplicated entries
        // <aggType, viewSrcTblColIndx> ==> <displayColName, displayColIndx>
        final AtomicBoolean hasAggregateOnGbyCol = new AtomicBoolean(false);        // when SELECT contains aggregate on its GBY column, and view doesn't
        final Map<Pair<Integer, Integer>, Pair<String, Integer>> nonGbyFromStmt =
                m_selectStmt.displayColumns().stream()                              // iterate over SELECT stmt's display columns and
                        .filter(ci -> !selGbyColIndexMap.containsKey(ci.m_index))   // collect all but GBY columns/expressions,
                        .flatMap(ci -> {
                            final Pair<String, Integer> value = Pair.of(ci.m_columnName, ci.m_index);  // Mapped value: display column name and index
                            if (ci.m_expression instanceof AggregateExpression) {   // if GBY over an aggregation function, then
                                final AbstractExpression left = ci.m_expression.getLeft();
                                return Stream.of(Pair.of(Pair.of(ci.m_expression.getExpressionType().getValue(), // extract aggregation type and argument:
                                        // aggregate on what? For simple column, it contains column index; for "count(*)", set to -1;
                                        left == null ? -1 : ((TupleValueExpression) left).getColumnIndex()), value));    // use it to map into value type.
                            } else if (ci.m_expression instanceof TupleValueExpression) {    // otherwise, when it's either column or distinct column: aggregation type is set to VALUE_TUPLE.
                                return Stream.of(Pair.of(Pair.of(ExpressionType.VALUE_TUPLE.getValue(),
                                        ((TupleValueExpression) ci.m_expression).getColumnIndex()), value));
                            } else {        // it could also be an aggregate function on a group-by column: do not support for now.
                                hasAggregateOnGbyCol.set(true);
                                return Stream.empty();
                            }
                        }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
        if (! hasAggregateOnGbyCol.get() &&
                nonGbyFromView.keySet().containsAll(nonGbyFromStmt.keySet())) {   // when {SELECT stmt non-GBY columns} \belongs {VIEW columns non-GBY columns}
            // Join the above 2 maps to get <display column name, display column index> ==> <MV column name, MV column index> for non-GBY display columns in SELECT stmt
            final Map<Pair<String, Integer>, Pair<String, Integer>> rel = nonGbyFromStmt.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getValue, kv -> nonGbyFromView.get(kv.getKey())));
            // Now that we know that the candidate MV can be used to answer the SELECT query, check GBY relations from MV
            final List<String> gbyNames = StreamSupport.stream(ciViewColumns.spliterator(), false)
                    .limit(mvGbys.size()).map(Column::getTypeName).collect(Collectors.toList());
            rel.putAll(m_selectStmt.m_displayColumns.stream()                 // Augment the map with GBY columns:
                    .filter(ci -> selGbyColIndexMap.containsKey(ci.m_index))  // starting from SELECT stmt's non-GBY display columns,
                    .map(ci -> {                                              // and index into MV's GBY columns to complete the relations.
                        final Integer viewColIndx = selGbyColIndexMap.get(ci.m_index);
                        return Pair.of(Pair.of(ci.m_columnName, ci.m_index),
                                Pair.of(gbyNames.get(viewColIndx), viewColIndx));
                    }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond)));
            return rel;
        } else {
            return null;
        }
    }

    // returns all materialized view info => view table from table list
    private static Map<MaterializedViewInfo, Table> getMviAndViews(List<Table> tbls) {
        return tbls.stream().flatMap(tbl ->
                StreamSupport.stream(((Iterable<MaterializedViewInfo>) () -> tbl.getViews().iterator()).spliterator(), false)
                        .map(mv -> Pair.of(mv, mv.getDest())))
                .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    }

    // Get a copy of *any* AbstractExpression, with itself and all subexpressions that are PVE converted to CVE
    // For scope of ENG-2878, caching would not cause this trouble because parameter
    private static AbstractExpression transformExpressionRidofPVE(AbstractExpression src) {
        AbstractExpression left = src.getLeft(), right = src.getRight();
        if (left != null) {
            left = transformExpressionRidofPVE(left);
        }
        if (right != null) {
            right = transformExpressionRidofPVE(right);
        }
        final AbstractExpression dst;
        if (src instanceof ParameterValueExpression) {      //
            assert(((ParameterValueExpression) src).getOriginalValue() != null);
            dst = ((ParameterValueExpression) src).getOriginalValue().clone();
        } else {
            dst = src.clone();
        }
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

    /**
     * Get group-by expression
     * @param mv
     * @return
     */
    private static List<AbstractExpression> getGbyExpressions(MaterializedViewInfo mv) {
        try {
            return AbstractExpression.fromJSONArrayString(mv.getGroupbyexpressionsjson(), null);
        } catch (JSONException e) {
            return new ArrayList<>();
        }
    }

}
