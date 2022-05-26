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

package org.voltdb.plannerv2.rules.logical;

import com.google_voltpatches.common.collect.Sets;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.voltcore.utils.Pair;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.plannerv2.rel.logical.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Utility class to check whether the of table joining operation results in MP or SP query.
 * The reason we need this mechanism in the first place is that Calcite does not have concept of
 * table partitioning, nor MP/SP.
 * Some of the methods here are more debuggable than readable. Edit at your own risk.
 */
final class RelDistributionUtils {
    private RelDistributionUtils() {}

    /**
     * Collect all the equivalent column references into equivalent sets.
     * For example, SELECT ... FROM foo INNER JOIN bar ON foo.i = foo.j AND foo.j = bar.i AND foo.k = bar.k
     * or
     * SELECT ... FROM foo, bar WHERE foo.i = foo.j AND foo.j = bar.i AND foo.k = bar.k
     * gives 2 equivalent sets: {foo.i, foo.j, bar.i}, {foo.k, bar.k}.
     * The column indices of the inner table are their column indices plus the outer relation column count.
     * @param joinCondition The join condition to check against
     * @return the collection of equivalent columns.
     */
    private static Set<Set<Integer>> getAllJoiningColumns(RexCall joinCondition) {
        final Set<Set<Integer>> r = new HashSet<>();
        if (joinCondition.isA(SqlKind.AND)) {
            // union find for all equivalent relations between columns, like
            // WHERE col1 = col2 AND col1 = col3 AND col4 = col2 AND col5 = col6 ==>
            // {col1, col2, col3, col4}, {col5, col6}

            // Calcite flattens cascaded AND into a list: (x1 AND x2 AND x3 ...) => AND(x1, x2, x3, ...)
            joinCondition.getOperands().stream().filter(node -> node instanceof RexCall)
                    .forEach(node ->
                            getJoiningColumns((RexCall) node).ifPresent(columnPair -> {
                                final Integer fst = columnPair.getFirst(), snd = columnPair.getSecond();
                                boolean updated = false;
                                for (Set<Integer> entry : r) {
                                    if (entry.contains(fst) || entry.contains(snd)) {
                                        entry.add(fst);
                                        entry.add(snd);
                                        updated = true;
                                        break;
                                    }
                                }
                                if (! updated) {
                                    r.add(new HashSet<Integer>() {{
                                        add(columnPair.getFirst());
                                        add(columnPair.getSecond());
                                    }});
                                }
                            }));
        } else {
            getJoiningColumns(joinCondition).ifPresent(pair ->
                    r.add(new HashSet<Integer>(){{
                        add(pair.getFirst());
                        add(pair.getSecond());
                    }}));
        }
        return r;
    }

    /**
     * Map column indices from a table scan node using the Calc node's Program's projection. The result is
     * @param program The Program from Calc node that contains projection
     * @param indices column indices with reference to table scan
     * @return  column indices with reference to given Program's projection relation
     */
    static Set<Integer> adjustProjection(RexProgram program, Collection<Integer> indices) {
        final List<RexLocalRef> projections = program.getProjectList();
        final List<RexNode> expressions = program.getExprList();
        return IntStream.range(0, projections.size())
                .filter(index -> {
                    final int localIndex = projections.get(index).getIndex();
                    assert localIndex < expressions.size() :
                            "RexLocalRef index out of bounds: " + localIndex + " >= " + expressions.size();
                    final RexNode node = expressions.get(localIndex);
                    return node instanceof RexInputRef && indices.contains(((RexInputRef) node).getIndex());
                }).boxed().collect(Collectors.toSet());
    }

    /**
     * Checks if the given condition is ColumnRef = Literal.
     * @param call Condition to check
     * @return (ColumnIndex, Literal) when the condition is in this form.
     */
    private static Stream<Pair<Integer, RexNode>> getColumnValuePairs(RexCall call) {
        if (call.isA(SqlKind.EQUALS)) {
            final RexNode left = uncast(call.getOperands().get(0)), right = uncast(call.getOperands().get(1));
            if (left instanceof RexInputRef && RexUtil.isTransitivelyLiteral(right) ||
                    right instanceof RexInputRef && RexUtil.isTransitivelyLiteral(left)) {
                final int col;
                final RexNode literal;
                if (RexUtil.isTransitivelyLiteral(right)) {
                    col = ((RexInputRef) left).getIndex();
                    literal = right;
                } else {
                    col = ((RexInputRef) right).getIndex();
                    literal = left;
                }
                return Stream.of(Pair.of(col, literal));
            }
        }
        return Stream.empty();
    }

    /**
     * Collects all the conjunction of ColumnRef = Literal forms.
     * e.g. foo.i = 1 AND foo.j = 'foo' AND bar.k = 0, with foo(i int, j varchar), bar(i int, k int), gives:
     * {(0, 1), (1, 'foo'), (1, 0)}.
     * @param call condition to be checked
     * @return A collection of column literal values.
     */
    private static Map<Integer, RexNode> getAllColumnValuePairs(RexCall call) {
        if (call.isA(SqlKind.AND)) {
            return call.getOperands().stream().flatMap(entry -> {
                if (entry instanceof RexCall) {
                    return getColumnValuePairs((RexCall) entry);
                } else {
                    return Stream.empty();
                }
            }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (a, b) -> a));
        } else {
            final Map<Integer, RexNode> m = new HashMap<>();
            getColumnValuePairs(call).findAny().ifPresent(p -> m.put(p.getFirst(), p.getSecond()));
            return m;
        }
    }

    /**
     * Return an un-casted node, if the node is a cast; or the node itself.
     * @param node possibly a CAST node
     * @return uncasted node
     */
    private static RexNode uncast(RexNode node) {
        if (node.isA(SqlKind.CAST)) {
            return ((RexCall) node).getOperands().get(0);
        } else {
            return node;
        }
    }

    /**
     * Given a pair of RexInputRef returns a pair corresponding column indexes (inner, outer)
     * @param leftConj
     * @param rightConj
     * @return
     */
    private static Optional<Pair<Integer, Integer>> pairInputRefToIndexPair(RexNode leftConj, RexNode rightConj) {
        if (!(leftConj instanceof RexInputRef) || !(rightConj instanceof RexInputRef)) {
            return Optional.empty();
        } else {
            final int col1 = ((RexInputRef) leftConj).getIndex(),
                    col2 = ((RexInputRef) rightConj).getIndex();
            if (col1 < col2) {
                return Optional.of(Pair.of(col1, col2));
            } else {
                return Optional.of(Pair.of(col2, col1));
            }
        }
    }

    /**
     * Given a pair of expressions returns an Optional pair corresponding column indexes (inner, outer)
     * if both expressions are "IS NULL(column)" expression or an empty optional otherwise
     *
     * @param leftConj
     * @param rightConj
     * @return
     */
    private static Optional<Pair<Integer, Integer>> pairIsNullToIndePair(RexNode leftConj, RexNode rightConj) {
        if (!(leftConj.isA(SqlKind.IS_NULL)) || !(rightConj.isA(SqlKind.IS_NULL))) {
            return Optional.empty();
        } else {
            return pairInputRefToIndexPair(((RexCall) leftConj).getOperands().get(0),
                    ((RexCall) rightConj).getOperands().get(0));
        }
    }

    /**
     * For the condition of form "T1.c1 = T2.c2" or "T1.c1 IS NOTDISTINCT FROM T2.c2",
     * the c1/c2 indexes are based on the joined table with columns coming
     * from both tables. Return [c1, c2] such that c1 < c2 (i.e. if they come from different table, then c1 comes from
     * outer table and c2 from inner table).
     * For other forms of join condition, return null.
     * @param joinCondition The join condition
     * @return ordered indexes of column references.
     */
    private static Optional<Pair<Integer, Integer>> getJoiningColumns(RexCall joinCondition) {
        if (joinCondition.isA(SqlKind.EQUALS)) {
            final RexNode leftConj = uncast(joinCondition.getOperands().get(0)),
                    rightConj = uncast(joinCondition.getOperands().get(1));
            return pairInputRefToIndexPair(leftConj, rightConj);
        } else if (joinCondition.isA(SqlKind.CASE)) {
            // A T1.c1 IS NOTDISTINCT FROM T2.c2 condition get converted to a CASE expression
            // CASE
            //      WHEN T1.c1 IS NULL THEN t2.c2 IS NULL
            //      WHEN T2.c2 IS NULL THEN t1.c1 IS NULL
            //      ELSE NOT NULL CAST(T1.C1) = NOT NULL CAST(T2.C2)
            // END
            int operandsCount = joinCondition.getOperands().size();
            RexNode last = joinCondition.getOperands().get(operandsCount - 1);
            if (operandsCount == 5 && last instanceof RexCall) {
                Optional<Pair<Integer, Integer>> p1 = pairIsNullToIndePair(joinCondition.getOperands().get(0), joinCondition.getOperands().get(1));
                Optional<Pair<Integer, Integer>> p2 = pairIsNullToIndePair(joinCondition.getOperands().get(2), joinCondition.getOperands().get(3));
                Optional<Pair<Integer, Integer>> p3 = getJoiningColumns((RexCall)last);
                if (p1.equals(p2) && p1.equals(p3)) {
                    return p3;
                }
            }
            return Optional.empty();
        } else {
            return Optional.empty();
        }
    }

    /**
     * Retrieve the partition columns of a table scan node or Calc/Join node.
     * The replicated table scan has no partition column;
     * The partition table scan has a single column index;
     * The Calc/Join node could contain 0 or multiple column indices, so that the
     * upper RelNode could tell if the predicate or join was performed on ANY partition
     * columns.
     *
     * @param tbl table scan node, or Calc/Join node, etc.
     * @return partition columns
     */
    private static Set<Integer> getPartitionColumns(RelNode tbl) {
        final RelDistribution dist;
        if (tbl instanceof TableScan) {
            dist = tbl.getTable().getDistribution();
        } else {
            dist = tbl.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        }
        return new HashSet<>(dist.getKeys());
    }

    /**
     * Get the index of a (possibly casted) local or input column index, if it is.
     * @param node a (possibly casted) input/local reference node
     * @return the index of the (possibly casted) reference node, or null otherwise.
     */
    private static Integer getColumnIndex(RexNode node) {
        final RexNode src = uncast(node);
        if (src.isA(SqlKind.INPUT_REF)) {
            return ((RexInputRef) src).getIndex();
        } else if (src.isA(SqlKind.LOCAL_REF)) {
            return ((RexLocalRef) src).getIndex();
        } else {
            return null;
        }
    }

    /**
     * Given two RexNodes that forms a SqlKind.EQUAL, test if it is in the form of "Column = Literal" or
     * "Column1 = Column2", and make updates respectively.
     * @param map The relation of all "Column = Literal" collected so far
     * @param eqCols The relation of all "Column1 = Column2" collected so far
     * @param lhs  LHS of equality
     * @param rhs RHS of equality
     */
    private static void updateColumnIndexAndLiteral(
            Map<Integer, RexNode> map, Set<Set<Integer>> eqCols, RexNode lhs, RexNode rhs) {
        final RexNode literal;
        final Integer index;
        if (RexUtil.isTransitivelyLiteral(lhs)) {
            literal = lhs;
            index = getColumnIndex(rhs);
        } else if (RexUtil.isTransitivelyLiteral(rhs)) {
            index = getColumnIndex(lhs);
            literal = rhs;
        } else {
            index = null;
            literal = null;
        }
        if (index != null) {
            map.put(index, literal);
        } else {
            final Integer index1 = getColumnIndex(lhs), index2 = getColumnIndex(rhs);
            if (index1 != null && index2 != null) {
                eqCols.stream()
                        .filter(cols -> cols.contains(index1) || cols.contains(index2))
                        .peek(col -> {
                            col.add(index1); col.add(index2);
                        }).findAny()
                        .orElseGet(() -> {
                            final Set<Integer> s = Sets.newHashSet(index1, index2);
                            eqCols.add(s);
                            return s;
                        });
            }
        }
    }

    /**
     * Given all the relations of "Column = LITERAL" and "Column1 = Column2", generate the new map of "Column = LITERAL"
     * that contains both relations.
     * @param mapsToLiteral The original column to literal relation
     * @param eqCols The equivalence relation of column indices
     * @return the expanded map of all column to literals.
     */
    private static Map<Integer, RexNode> transExpand(Map<Integer, RexNode> mapsToLiteral, Set<Set<Integer>> eqCols) {
        final Map<Integer, RexNode> expanded = new HashMap<>(mapsToLiteral);
        mapsToLiteral.forEach((col, literal) ->
                eqCols.forEach(cols -> {
                    if (cols.contains(col)) {
                        expanded.putAll(cols.stream().map(col2 ->
                                new AbstractMap.SimpleEntry<>(col2, literal))
                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a)));
                    }
                })
        );
        return expanded;
    }

    /**
     * Helper to retrieve all "ColumnRef = LiteralValue" pairs from a Calc node
     * @param condRef a local reference in form of "#5" denoting the boolean expression
     * @param exprs the list of expressions that @param condRef refers to
     */
    private static void getEqualValuePredicate(
            RexLocalRef condRef, List<RexNode> exprs, Map<Integer, RexNode> mapToLiteral, Set<Set<Integer>> eqCols) {
        final RexNode condDeref = exprs.get(condRef.getIndex());
        if (condDeref.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) condDeref;
            RexNode left = uncast(call.getOperands().get(0)), right = uncast(call.getOperands().get(1));
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            left = exprs.get(((RexLocalRef) left).getIndex());
            right = exprs.get(((RexLocalRef) right).getIndex());
            updateColumnIndexAndLiteral(mapToLiteral, eqCols, left, right);
        } else if (condDeref.isA(SqlKind.AND)) {
            ((RexCall) (condDeref)).getOperands().forEach(node -> {
                if (node instanceof RexLocalRef) {
                    getEqualValuePredicate((RexLocalRef) node, exprs, mapToLiteral, eqCols);
                }
            });
        }
    }

    /**
     * Collect all "ColumnRef = LiteralValue" pairs from a Calc node
     * @param calc calc node under inspection
     * @return the collection
     */
    private static Map<Integer, RexNode> calcCondition(Calc calc) {
        final RexProgram prog = calc.getProgram();
        final Map<Integer, RexNode> mapsToLiteral = new HashMap<>();
        final Set<Set<Integer>> eqCols = new HashSet<>();
        if (prog.getCondition() != null) {
            getEqualValuePredicate(prog.getCondition(), prog.getExprList(), mapsToLiteral, eqCols);
        }
        return transExpand(mapsToLiteral, eqCols);
    }

    /**
     * Search for the equivalent column set that matches with given partition column(s).
     * The matching set must include all non-null partition columns given. If no such equivalent set
     * exists, returns empty.
     * @param equalCols Equivalent set of column indices
     * @param outerPartCols partition columns of outer rel. It is empty when the outer rel is not partitioned; or when
     *                      the projection does not include partition columns.
     * @param innerPartCols partition columns of inner rel. It is empty when the outer rel is not partitioned; or when*
     *                      the projection does not include partition columns.
     * @param outerTableColumns Number of columns of outer relation, used to tell from equivalent set which indices are
     *                          from the inner rel, and convert to their table-wise column indices.
     * @return an equivalent set of column indices that includes all given partition columns. It is either
     * Optional.empty(), or a non-empty set of equivalent indices that includes all (adjusted) partition column indices
     * from both relations.
     */
    private static Set<Integer> searchEqualPartitionColumns(
            Set<Set<Integer>> equalCols, Set<Integer> outerPartCols, Set<Integer> innerPartCols,
            int outerTableColumns) {
        final Set<Integer> adjustedInnerPartCols =      // to relative column indices, by adding outerTableColumns
                innerPartCols.stream().map(col -> col + outerTableColumns).collect(Collectors.toSet());
        return equalCols.stream()
                .filter(equals ->
                        (outerPartCols.isEmpty() || intersects(outerPartCols, equals)) &&
                                (adjustedInnerPartCols.isEmpty() || intersects(adjustedInnerPartCols, equals)))
                .map(equals -> {        // make sure to add all partition columns from outer/inner rels
                    final Set<Integer> expanded = new HashSet<>(equals);
                    expanded.addAll(outerPartCols);
                    expanded.addAll(adjustedInnerPartCols);
                    return expanded;
                }).findAny().orElse(Collections.emptySet());
    }

    /**
     * Given that we found the set of equivalent column set of eq-join that matches all the partition columns, separate
     * them into column index of outer table and inner table (but do not adjust to absolute column index).
     * For example, if outer table is partitioned on column #1 and has 5 columns, inner table is partitioned on column #2,
     * and the matching set is (1, 6, 7), return (1, 5 + 2);
     * if the inner table is partitioned, and the matching set is (1, 2, 5), return (1, 5) because #2 is still in outer table.
     *
     * @param eqCols The matched equivalent column indices that matches all partition columns. The precondition is that
     *               for any non-empty absolute partition column set, the eqCols should include all of those columns
     * @param partCol1 absolute column index of partition columns of outer rel
     * @param partCol2 absolute column index of partition columns of inner rel
     * @param outerTableColumns column count of outer rel
     * @return a pair of relative column indices that includes all the partition columns (and possibly more equivalent
     * columns) from both joining relations.
     */
    private static Pair<Set<Integer>, Set<Integer>> separateColumns(
            Set<Integer> eqCols, Set<Integer> partCol1, Set<Integer> partCol2, int outerTableColumns) {
        final Set<Integer> adjustedPartCol2 =
                partCol2.stream().map(col -> col + outerTableColumns).collect(Collectors.toSet());
        final Set<Integer> outer = new HashSet<>(), inner = new HashSet<>();
        if ((partCol1.isEmpty() || intersects(partCol1, eqCols)) &&
                (partCol2.isEmpty() || intersects(eqCols, adjustedPartCol2))) {
            eqCols.forEach(col -> {
                if (col < outerTableColumns) {
                    outer.add(col);
                } else {
                    inner.add(col);
                }
            });
        }
        return Pair.of(outer, inner);
    }

    /**
     * Get the distribution trait from RelNode, except when the node is "eventually" an aggregation node, in which case
     * we treat it as a replicated table, and return SINGLETON.
     * The "eventually" means that either the node itself is an aggregation; or when there are some CALC/SORT/LIMIT
     * nodes above an aggregation node, see `isAggregateNode`.
     * @param node
     * @return
     */
    static RelDistribution getDistribution(RelNode node) {
        return node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    private static Pair<Map<Integer, RexNode>, Map<Integer, RexNode>> fillColumnLiteralEqualPredicates(
            RelNode outer, RelNode inner, int outerRelColumns, RexCall joinCond, Set<Integer> joinColumns) {
        final Map<Integer, RexNode> outerFilter, innerFilter;
        if (outer instanceof Calc) {
            outerFilter = calcCondition((Calc) outer);
        } else {
            outerFilter = new HashMap<>();
        }
        if (inner instanceof Calc) {
            innerFilter = calcCondition((Calc) inner).entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(
                            // with adjustment from absolute indices to relative after join
                            entry.getKey() + outerRelColumns, entry.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        } else {
            innerFilter = new HashMap<>();
        }
        getAllColumnValuePairs(joinCond).forEach((colIndex, literal) ->
                IntStream.of(colIndex).flatMap(index -> {
                    if (joinColumns.contains(index)) {
                        return joinColumns.stream().mapToInt(Integer::intValue);
                    } else {
                        return IntStream.of(index);
                    }
                }).forEach(col -> {
                    final Map.Entry<Integer, RexNode> ret =
                            new AbstractMap.SimpleEntry<>(col, literal);
                    if (col < outerRelColumns) {
                        outerFilter.put(ret.getKey(), ret.getValue());
                    } else {
                        innerFilter.put(ret.getKey(), ret.getValue());
                    }
                }));
        return Pair.of(outerFilter, innerFilter);
    }

    /**
     * State of join condition we want to propagate into the distribution trait.
     * In particular, we need to keep track of which column(s) are partition columns
     * from intermediate table scan/Calc/Join nodes up, because the partition equal
     * value of any of these partitioned (and joined) columns could be assigned in
     * a quite outermost join node.
     */
    static final class JoinState {
        final private boolean m_isSP;
        final private RexNode m_literal;
        final private Set<Integer> m_partCols;      // relative column index to the join node
        JoinState(boolean isSP, RexNode literal, Set<Integer> partCols) {
            // NOTE: it could be that partCols.isEmpty() && literal != null, in cases like:
            // SELECT * FROM (SELECT j FROM P1 WHERE i = 5), (SELECT j FROM P2 WHERE i = 5);
            // Here, both relations are partitioned, but their projections do not include partition columns;
            // still, we need their respective literal values in the JOIN node.
            m_isSP = isSP;
            m_literal = literal;
            m_partCols = partCols;
        }
        public boolean isSP() {
            return m_isSP;
        }
        public RexNode getLiteral() {
            return m_literal;
        }
        Set<Integer> getPartCols() {
            return m_partCols;
        }
    }

    private static RexNode literalOr(RexNode left, RexNode right) {
        return left == null ? right : left;
    }

    private static<T> boolean intersects(Set<T> left, Set<T> right) {
        final Set<T> intersected = new HashSet<>(left);
        intersected.retainAll(right);
        return ! intersected.isEmpty();
    }

    /**
     * Check whether a given set op relation is SP.
     * Set Op is SP if all of its children are SP and their partitioning values are
     * either NULL (a child is a replicated scan) or equal each other (implies that there is a
     * "WHERE partitionColumn = LITERAL_VALUE" for each child)
     *
     * @param setOpNodes SetOp children
     * @return true if the result is SP.
     */
    static JoinState isSetOpSP(List<RelNode> setOpNodes) {
        JoinState initSetOpState = new JoinState(true, null, Sets.newHashSet());
        JoinState finalSetOpState = setOpNodes.stream().reduce(
                initSetOpState,
                (currentState, nextChild) -> {
                    RelDistribution nextDist = getDistribution(nextChild);
                    final boolean currentIsPartitioned = !currentState.getPartCols().isEmpty(),
                            nextIsPartitioned = nextDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED || !nextDist.getIsSP();
                    if (!currentIsPartitioned && !nextIsPartitioned) {
                        // SP SetOP so far. Combined partitioning state is still the same
                        return currentState;
                    } else if (currentIsPartitioned) {
                        if (nextIsPartitioned) {
                            // The current state and the next child are both MP.
                            // Need to make sure they have compatible partitioning
                            RexNode currentPartValue = currentState.getLiteral();
                            RexNode nextPartValue = nextDist.getPartitionEqualValue();
                            if (currentPartValue == null || !currentPartValue.equals(nextPartValue)) {
                                throw new PlanningErrorException("SQL error while compiling query: " +
                                        "Statements are too complex in set operation using multiple partitioned tables.");
                            }
                        }
                        // Either the next child is SP or partitioning values match
                        return currentState;
                    } else {
                        assert (nextIsPartitioned);
                        // All previous children are SP and this is the first MP one
                        final Set<Integer> nextPartColumns = getPartitionColumns(nextChild);
                        return new JoinState(false, nextDist.getPartitionEqualValue(), nextPartColumns);
                    }
                },
                (currentState, nextState) -> nextState);
        return finalSetOpState;
    }

    /**
     * Check if a node is "eventually" an aggregation node. It can have arbitrary/combinations of
     * Calc/Sort/Limit on top of the aggregation node.
     * @param node source node under inspection
     * @return whether the given node contains an aggregation node somewhere.
     */
    private static boolean isAggregateNode(RelNode node) {
        if (node instanceof VoltLogicalAggregate) {
            return true;
        } else if (node instanceof VoltLogicalCalc ||
                node instanceof VoltLogicalLimit ||
                node instanceof VoltLogicalSort) {
            return isAggregateNode(((HepRelVertex) node.getInput(0)).getCurrentRel());
        } else {
            return false;
        }
    }

    /**
     * Check whether the given join relation is SP.
     * If it is SP, and contains partitioned tables, (which implies that there is a
     * "WHERE partitionColumn = LITERAL_VALUE"), then also set the literal value in the distribution trait.
     * @param join join node
     * @param outer outer relation
     * @param inner inner relation
     * @return true if the result of the join is SP, and sets the distribution trait for all partitioned relations.
     */
    static JoinState isJoinSP(VoltLogicalJoin join, RelNode outer, RelNode inner) {
        final RelDistribution outerDist = getDistribution(outer), innerDist = getDistribution(inner);
        final int outerTableColumns = outer.getRowType().getFieldCount();
        final Set<Integer> outerPartColumns = getPartitionColumns(outer),
                innerPartColumns = getPartitionColumns(inner),
                adjustedInnerPartColumns =
                        innerPartColumns.stream().map(col -> col + outerTableColumns).collect(Collectors.toSet()),
                combinedPartColumns = new HashSet<>(outerPartColumns);
        combinedPartColumns.addAll(adjustedInnerPartColumns);
        final boolean outerIsPartitioned = outerDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED || !outerDist.getIsSP(),
                innerIsPartitioned = innerDist.getType() == RelDistribution.Type.HASH_DISTRIBUTED || !innerDist.getIsSP(),
                outerHasPartitionKey = outerDist.getPartitionEqualValue() != null,
                innerHasPartitionKey = innerDist.getPartitionEqualValue() != null,
                outerHasExchange = outerDist.getType() == RelDistribution.Type.SINGLETON && !outerDist.getIsSP(),
                innerHasExchange = innerDist.getType() == RelDistribution.Type.SINGLETON && !innerDist.getIsSP();

        if ((outerHasExchange && innerIsPartitioned) ||
                (innerHasExchange && outerIsPartitioned)) {
            // Both relations are partitioned; and one of them already has an Exchange node.
            // Since a join is distributed and would also require an Exchange node to be added
            // on top it, the final plan would have two Exchanges resulting in more than one
            // coordinator fragment.
            throw new PlanningErrorException("SQL error while compiling query: " +
                    "This join is too complex using multiple partitioned tables");
        }

        final RexNode srcLitera = literalOr(outerDist.getPartitionEqualValue(), innerDist.getPartitionEqualValue());
        switch (join.getJoinType()) {
            case INNER:
                if (outerIsPartitioned || innerIsPartitioned) {
                    if (join.getCondition().isA(SqlKind.LITERAL)) {
                        assert join.getCondition().isAlwaysFalse() || join.getCondition().isAlwaysTrue();
                        if (join.getCondition().isAlwaysFalse()) {
                            // join condition might be false (TestBooleanLiteralsSuite)
                            return new JoinState(true, srcLitera, combinedPartColumns);
                        } else if (outerIsPartitioned && innerIsPartitioned) {
                            // if outer and / or inner  has partitioning key they better match
                            // because the join's condition is a LITERAL and doesn't have an equality expression
                            // involving partitioning columns
                            if ((outerHasPartitionKey &&
                                    !outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue())) ||
                                    (innerHasPartitionKey &&
                                            !innerDist.getPartitionEqualValue().equals(outerDist.getPartitionEqualValue()))) {
                                throw new PlanningErrorException("SQL error while compiling query: " +
                                        "Outer and inner statements use conflicting partitioned table filters.");
                            }
                            return new JoinState(
                                    outerDist.getIsSP() && innerDist.getIsSP() && (! outerHasPartitionKey ||   // either outer is replicated (SINGLE); or partitioned with a key
                                            outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue())),
                                    srcLitera, combinedPartColumns);
                        } else {
                            return new JoinState(
                                    outerHasPartitionKey || innerHasPartitionKey, srcLitera, combinedPartColumns);
                        }
                    }
                    assert join.getCondition() instanceof RexCall;
                    final RexCall joinCondition = (RexCall) join.getCondition();
                    final Set<Set<Integer>> joinColumnSets = getAllJoiningColumns(joinCondition);
                    // Check that partition columns must be in equal-relations
                    final Set<Integer> equalPartitionColumns =
                            searchEqualPartitionColumns(
                                    joinColumnSets, outerPartColumns, innerPartColumns, outerTableColumns);
                    if (equalPartitionColumns.isEmpty()) {
                        if (outerHasPartitionKey && ! innerIsPartitioned || innerHasPartitionKey && ! outerIsPartitioned) {
                            // The partitioned rel has equal key (but depending on its isSP flag, it
                            // might not be SP yet); the other rel is replicated --> SP
                            return new JoinState(outerDist.getIsSP() && innerDist.getIsSP(),
                                    srcLitera, combinedPartColumns);
                        } else if (outerHasPartitionKey &&
                                outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue())) {
                            // Both relations are partitioned with keys, and they are equal --> SP
                            return new JoinState(true, srcLitera, combinedPartColumns);
                        } else if (outerIsPartitioned && innerIsPartitioned) {
                            // Both are partitioned but thepartitioning columns don't match
                            // and no partitioning filters. Not plannable
                            throw new PlanningErrorException("SQL error while compiling query: " +
                                    "This join is too complex using multiple partitioned tables");
                        } else {
                            return new JoinState(false, srcLitera, combinedPartColumns);
                        }
                    }
                    final Pair<Set<Integer>, Set<Integer>> partCols = separateColumns(
                            equalPartitionColumns, outerPartColumns, innerPartColumns, outerTableColumns);
                    final Set<Integer> outerJoiningCols = partCols.getFirst(), innerJoiningCols = partCols.getSecond();
                    // Does not join on the partition column
                    if (outerIsPartitioned && outerJoiningCols.isEmpty() ||
                            innerIsPartitioned && innerJoiningCols.isEmpty()) {
                        return new JoinState(false, srcLitera, combinedPartColumns);
                    }
                    // missing filters on table scans
                    final Pair<Map<Integer, RexNode>, Map<Integer, RexNode>> r =
                            fillColumnLiteralEqualPredicates(outer, inner, outerTableColumns,
                                    joinCondition, equalPartitionColumns);
                    final Map<Integer, RexNode> outerFilter = r.getFirst(), innerFilter = r.getSecond();
                    if (joinColumnSets.isEmpty() ||
                            joinColumnSets.stream().noneMatch(set ->
                                    (! outerIsPartitioned || intersects(set, outerPartColumns)) &&
                                            (! innerIsPartitioned || intersects(set, adjustedInnerPartColumns)))) {
                        if (outerIsPartitioned && innerIsPartitioned &&
                                !isAggregateNode(outer) && !isAggregateNode(inner)) {
                            // Both relations are partitioned; but the join condition
                            // does **not** join on their partition columns.
                            // We can assert that VoltDB cannot handle such join.
                            //
                            // Note that we also exclude the case when either (or both) join rels is an aggregation,
                            // in which case it is MP query that Volt should be able to plan. An example is:
                            // CREATE TABLE P1(id int not null, tiny int, num int, vchar VARCHAR(64), pt GEOGRAPHY(164)); PARTITION TABLE p1 ON COLUMN id;
                            // CREATE TABLE R1(vchar VARCHAR(64), pt GEOGRAPHY(164));
                            // SELECT 'foo', P1.tiny FROM P1 WHERE vchar != (SELECT MAX(vchar) FROM R1 WHERE pt != P1.pt ORDER BY COUNT(*)) ORDER BY num;
                            throw new PlanningErrorException("SQL error while compiling query: " +
                                    "This query is not plannable.  " +
                                    "The planner cannot guarantee that all rows would be in a single partition.");
                        } else {
                            // only one relation is partitioned. Depending on whether the partitioned table has
                            // the equal value set, it is either SP (if the equal value is set and not null), or
                            // MP (but VoltDB can handle it).
                            return new JoinState(
                                    outerIsPartitioned && outerDist.getPartitionEqualValue() != null ||
                                            innerIsPartitioned && innerDist.getPartitionEqualValue() != null,
                                    srcLitera, combinedPartColumns);
                        }
                    }
                    final boolean
                            isOuterPartitionedSP = outerDist.getPartitionEqualValue() != null,
                            isInnerPartitionedSP = innerDist.getPartitionEqualValue() != null;
                    if (isOuterPartitionedSP || isInnerPartitionedSP) {
                        // multiple join with either of:
                        // 1. a joining multi-partitioned SP node and a replicated table
                        // 2. both are joining multi-partitioned SP nodes
                        // then, the result is still SP
                        return new JoinState(true, srcLitera, combinedPartColumns);
                    }
                    // separate condFilter into outerFilter and innerFilter
                    if (outerFilter.isEmpty() && innerFilter.isEmpty()) {
                        return new JoinState(false, null, combinedPartColumns); // No equal-filter found
                    }
                    final boolean outerTableHasEqValue =
                            outerIsPartitioned && intersects(outerFilter.keySet(), outerPartColumns);
                    final boolean innerTableHasEqValue =
                            innerIsPartitioned && intersects(innerFilter.keySet(), adjustedInnerPartColumns);
                    if (outerTableHasEqValue || innerTableHasEqValue) {
                        // Has an equal-filter on partitioned column
                        // NOTE: in case there are 2 equal predicated on each partitioned column each,
                        // we just take out the first one.
                        return new JoinState(true,
                                (outerTableHasEqValue ? outerFilter.entrySet() : innerFilter.entrySet())
                                        .stream().flatMap(entry -> {
                                    final int columnIndex = entry.getKey();
                                    if (outerPartColumns.contains(columnIndex) ||
                                            adjustedInnerPartColumns.contains(columnIndex)) {
                                        return Stream.of(entry.getValue());
                                    } else {
                                        return Stream.empty();
                                    }
                                }).findAny().orElse(null),
                                combinedPartColumns);
                    } else {    // no equal-filter on partition column
                        return new JoinState(false, srcLitera, combinedPartColumns);
                    }
                } else { // else Both are replicated or SP subqueries: SP
                    return new JoinState(true, srcLitera, combinedPartColumns);
                }
            default: // Outer join type involving at least a partitioned table
                // If both sides are partitioned they better be joined on the respective
                // partitioning columns
                if (outerIsPartitioned && innerIsPartitioned) {
                    assert join.getCondition() instanceof RexCall;
                    final RexCall joinCondition = (RexCall) join.getCondition();
                    final Set<Set<Integer>> joinColumnSets = getAllJoiningColumns(joinCondition);
                    // Check that partition columns must be in equal-relations
                    final Set<Integer> equalPartitionColumns =
                            searchEqualPartitionColumns(
                                    joinColumnSets, outerPartColumns, innerPartColumns, outerTableColumns);
                    if (equalPartitionColumns.isEmpty()) {
                        throw new PlanningErrorException("SQL error while compiling query: " +
                                "This query is not plannable.  " +
                                "The planner cannot guarantee that all rows would be in a single partition.");
                    }
                }
                return new JoinState(!outerIsPartitioned && !innerIsPartitioned, srcLitera, combinedPartColumns);
        }
    }

    /**
     * Flattens a RexCall that likely refers to some local index(es) of a given list of references,
     * so that the flattened RexCall does not refer to any external local references (but could refer to
     * some column of some table).
     *
     * @param node RexCall to flatten
     * @param ref list of reference to look up for local references
     * @return flattened RexCall
     */
    private static RexNode flattenRexCall(RexNode node, List<RexNode> ref) {
        final RexNode src = uncast(node);
        if (src.isA(SqlKind.LOCAL_REF)) {
            final int index = ((RexLocalRef) src).getIndex();
            return flattenRexCall(ref.get(index), ref);
        } else if (src instanceof RexCall) {
            final RexCall call = (RexCall) src;
            return call.clone(
                    call.getType(),
                    call.getOperands().stream().map(elt -> {
                        final RexNode uncasted = uncast(elt);
                        if (uncasted.isA(SqlKind.LOCAL_REF) ||
                                uncasted instanceof RexCall) {
                            return flattenRexCall(uncasted, ref);
                        } else {
                            return uncasted;
                        }
                    }).collect(Collectors.toList()));
        } else {
            return src;
        }
    }

    /**
     * Trace the condition from a Calc node until we found that it is in the form of "column_p = literal",
     * where column_p is the partition column provided.
     *
     * @param cond Calc condition, usually of {\code RexLocalRef}.
     * @param src List of local/input references, or literals to search for
     * @param mapsToLiteral map of column index to literals from the conditions
     * @param colEquivalents equivalence set of column indices
     */
    private static void matchedColumnLiteral(
            RexNode cond, List<RexNode> src, Map<Integer, RexNode> mapsToLiteral, Set<Set<Integer>> colEquivalents) {
        if (cond.isA(SqlKind.LOCAL_REF)) {
            matchedColumnLiteral(src.get(((RexLocalRef) cond).getIndex()), src, mapsToLiteral, colEquivalents);
        } else if (cond.isA(SqlKind.EQUALS)) {
            final RexCall eq = (RexCall) cond;
            final RexNode left = eq.getOperands().get(0), right = eq.getOperands().get(1);
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            final RexNode derefLeft = flattenRexCall(src.get(((RexLocalRef) left).getIndex()), src),
                    derefRight = flattenRexCall(src.get(((RexLocalRef) right).getIndex()), src);
            final boolean isLeftLiteral = RexUtil.isTransitivelyLiteral(derefLeft),
                    isRightLiteral = RexUtil.isTransitivelyLiteral(derefRight);
            if (derefLeft.isA(SqlKind.INPUT_REF) && isRightLiteral ||
                    derefRight.isA(SqlKind.INPUT_REF) && isLeftLiteral) {
                final int colIndex =
                        ((RexInputRef) (derefLeft.isA(SqlKind.INPUT_REF) ? derefLeft : derefRight)).getIndex();
                colEquivalents.add(Sets.newHashSet(colIndex));
                mapsToLiteral.put(colIndex, derefLeft.isA(SqlKind.INPUT_REF) ? derefRight : derefLeft);
            } else if (derefLeft.isA(SqlKind.INPUT_REF) && derefRight.isA(SqlKind.INPUT_REF)) {
                final int col1 = ((RexInputRef) derefLeft).getIndex(),
                        col2 = ((RexInputRef) derefRight).getIndex();
                final Set<Integer> another = Sets.newHashSet(col1, col2);
                colEquivalents.stream()
                        .filter(cols -> cols.contains(col1) || cols.contains(col2))
                        .peek(cols -> cols.addAll(another))
                        .findAny()
                        .orElseGet(() ->{
                            colEquivalents.add(another);
                            return another;
                        });
            }
        } else if (cond.isA(SqlKind.AND)) {
            ((RexCall) cond).getOperands().forEach(node ->
                    matchedColumnLiteral(node, src, mapsToLiteral, colEquivalents));
        } else if (cond.isA(SqlKind.NOT)) {
            // TODO
        }
    }

    private static Optional<RexNode> matchedColumnLiteral(RexNode cond, List<RexNode> src, int partitionCol) {
        final Map<Integer, RexNode> mapsToLiteral = new HashMap<>();
        final Set<Set<Integer>> colEq = new HashSet<>();
        matchedColumnLiteral(cond, src, mapsToLiteral, colEq);
        return Optional.ofNullable(mapsToLiteral.get(partitionCol));
    }

    /**
     * Method used for MPQueryFallBackRule with Calc/Scan nodes
     * @param scan Table scan node
     * @param calc Calc node on top of scan node
     * @return whether the top node is SP, and the partition equal value, if it is partitioned table and SP.
     */
    static Pair<Boolean, RexNode> isCalcScanSP(TableScan scan, Calc calc) {
        final RelDistribution dist = scan.getTable().getDistribution(); // distribution for the scanned table
        final RelDistribution calcDist = calc.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        switch (dist.getType()) {
            case SINGLETON:
                return Pair.of(true, null);
            case HASH_DISTRIBUTED:
                if (dist.getPartitionEqualValue() != null) {    // equal value already present
                    assert false;
                    return Pair.of(true, dist.getPartitionEqualValue());
                } else if (calc.getProgram().getCondition() == null) {
                    return Pair.of(false, null);
                } else {    // find equal value in calc node
                    return matchedColumnLiteral(
                            calc.getProgram().getCondition(), calc.getProgram().getExprList(), dist.getKeys().get(0))
                            .map(literal -> {
                                if (calcDist.getPartitionEqualValue() != null &&
                                        ! calcDist.getPartitionEqualValue().equals(literal)) {
                                    // Calc's distribution key had already been set to a different value
                                    return Pair.of(false, calcDist.getPartitionEqualValue());
                                } else {
                                    return Pair.of(true, literal);
                                }
                            }).orElse(Pair.of(false, null));
                }
            case RANDOM_DISTRIBUTED:        // View
            default:
                return Pair.of(false, null);
        }
    }
}
