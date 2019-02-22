package org.voltdb.plannerv2.rules.logical;

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
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
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

    static void checkedFallBack(boolean fallback) {
        if (fallback) {
            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
        }
    }

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
        if (joinCondition.isA(SqlKind.AND)) {
            // union find for all equivalent relations between columns, like
            // WHERE col1 = col2 AND col1 = col3 AND col4 = col2 AND col5 = col6 ==>
            // {col1, col2, col3, col4}, {col5, col6}
            final Set<Set<Integer>> r = new HashSet<>();
            joinCondition.getOperands()
                    .forEach(node -> {
                        if (node instanceof RexCall) {  // Calcite flattens cascaded AND into a list (x1 AND x2 AND x3 ...) => AND(x1, x2, x3, ...)
                            final Pair<Integer, Integer> columnPair = getJoiningColumns((RexCall) node);
                            if (columnPair != null) {
                                final Integer fst = columnPair.getFirst(), snd = columnPair.getSecond();
                                final AtomicBoolean updated = new AtomicBoolean(false);
                                for (Set<Integer> entry : r) {
                                    if (entry.contains(fst) || entry.contains(snd)) {
                                        entry.add(fst);
                                        entry.add(snd);
                                        updated.set(true);
                                        break;
                                    }
                                }
                                if (! updated.get()) {
                                    r.add(new HashSet<Integer>() {{
                                        add(columnPair.getFirst());
                                        add(columnPair.getSecond());
                                    }});
                                }
                            }
                        }
                    });
            return r;
        } else {
            final Pair<Integer, Integer> pair = getJoiningColumns(joinCondition);
            if (pair == null) {
                return new HashSet<>();
            } else {
                return Collections.singleton(new HashSet<Integer>(){{
                    add(pair.getFirst());
                    add(pair.getSecond());
                }});
            }
        }
    }

    /**
     * Checks if the given condition is ColumnRef = Literal.
     * @param call Condition to check
     * @return (ColumnIndex, Literal) when the condition is in this form.
     */
    private static Pair<Integer, RexLiteral> getColumnValuePairs(RexCall call) {
        if (call.isA(SqlKind.EQUALS)) {
            final RexNode left = call.getOperands().get(0), right = call.getOperands().get(1);
            if (left instanceof RexInputRef && right.isA(SqlKind.LITERAL) ||
                    right instanceof RexInputRef && left.isA(SqlKind.LITERAL)) {
                final int col;
                final RexLiteral literal;
                if (right.isA(SqlKind.LITERAL)) {
                    col = ((RexInputRef) left).getIndex();
                    literal = (RexLiteral) right;
                } else {
                    col = ((RexInputRef) right).getIndex();
                    literal = (RexLiteral) left;
                }
                return Pair.of(col, literal);
            }
        }
        return null;
    }

    /**
     * Collects all the conjunction of ColumnRef = Literal forms.
     * e.g. foo.i = 1 AND foo.j = 'foo' AND bar.k = 0, with foo(i int, j varchar), bar(i int, i int), gives:
     * {(0, 1), (1, 'foo'), (1, 0)}.
     * @param call condition to be checked
     * @return A collection of column literal values.
     */
    private static Map<Integer, RexLiteral> getAllColumnValuePairs(RexCall call) {
        if (call.isA(SqlKind.AND)) {
            return call.getOperands().stream().flatMap(entry -> {
                if (entry instanceof RexCall) {
                    final Pair<Integer, RexLiteral> p = getColumnValuePairs((RexCall) entry);
                    if (p != null) {
                        return Stream.of(p);
                    }
                }
                return Stream.empty();
            }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (a, b) -> a));
        } else {
            final Map<Integer, RexLiteral> m = new HashMap<>();
            final Pair<Integer, RexLiteral> p = getColumnValuePairs(call);
            if (p != null) {
                m.put(p.getFirst(), p.getSecond());
            }
            return m;
        }
    }

    /**
     * For the condition of form "T1.c1 = T2.c2", the c1/c2 indexes are based on the joined table with columns coming
     * from both tables. Return [c1, c2] such that c1 < c2 (i.e. if they come from different table, then c1 comes from
     * outer table and c2 from inner table).
     * For other forms of join condition, return null.
     * @param joinCondition The join condition
     * @return ordered indexes of column references.
     */
    private static Pair<Integer, Integer> getJoiningColumns(RexCall joinCondition) {
        if (! joinCondition.isA(SqlKind.EQUALS)) {
            return null;
        } else {
            final RexNode leftConj = joinCondition.getOperands().get(0), rightConj = joinCondition.getOperands().get(1);
            if (!(leftConj instanceof RexInputRef) || !(rightConj instanceof RexInputRef)) {
                return null;
            } else {
                final int col1 = ((RexInputRef) leftConj).getIndex(),
                        col2 = ((RexInputRef) rightConj).getIndex();
                if (col1 < col2) {
                    return Pair.of(col1, col2);
                } else {
                    return Pair.of(col2, col1);
                }
            }
        }
    }

    /**
     * Retrieve the underlying table scan node.
     * @param node a relation node that represents a table scan
     * @return the converted table scan node
     */
    private static VoltLogicalTableScan getTableScan(RelNode node) {
        assert node.getInputs().size() == 1;
        final RelNode scan = node.getInput(0);
        assert scan instanceof HepRelVertex;
        final RelNode vscan = ((HepRelVertex) scan).getCurrentRel();
        if (vscan instanceof VoltLogicalTableScan) {
            return (VoltLogicalTableScan) vscan;
        } else {
            assert vscan instanceof VoltLogicalJoin;
            final VoltLogicalJoin join = (VoltLogicalJoin) vscan;
            checkSPAndPropogateDistribution(join, join.getLeft(), join.getRight());
            return null;
        }
    }

    /**
     * Retrieve the partition column of a table scan node. For replicated table, returns null.
     * @param tbl table scan node
     * @return partition column
     */
    private static Integer getPartitionColumn(RelNode tbl) {
        if (tbl instanceof TableScan) {
            final RelDistribution dist = tbl.getTable().getDistribution();
            if (dist.getKeys().isEmpty()) {
                return null;
            } else {
                return dist.getKeys().get(0);
            }
        } else if (tbl instanceof VoltLogicalJoin) {    // join tree with a child being a join node
            final List<Integer> keys =
                    tbl.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE).getKeys();
            return keys.isEmpty() ? null : keys.get(0);
        } else if (tbl instanceof VoltLogicalCalc) {
            final VoltLogicalTableScan scan = getTableScan(tbl);
            if (scan != null) {
                return scan.getVoltTable().getPartitionColumn();
            } else {
                return null;    // TODO
            }
        } else if (tbl instanceof HepRelVertex) {
            return getPartitionColumn(((HepRelVertex) tbl).getCurrentRel());
        } else {
            return null;    // TODO
        }
    }

    private static Integer getColumnIndex(RexNode node) {
        if (node.isA(SqlKind.INPUT_REF)) {
            return ((RexInputRef) node).getIndex();
        } else if (node.isA(SqlKind.CAST) && ((RexCall) node).getOperands().get(0).isA(SqlKind.LOCAL_REF)) {
            return ((RexLocalRef) ((RexCall) node).getOperands().get(0)).getIndex();
        } else {
            return null;
        }
    }
    private static void addColumnIndexAndLiteral(
            Map<Integer, RexLiteral> map, RexNode node1, RexNode node2) {
        final RexLiteral literal;
        final Integer index;
        if (node1.isA(SqlKind.LITERAL)) {
            literal = (RexLiteral) node1;
            index = getColumnIndex(node2);
        } else if (node2.isA(SqlKind.LITERAL)) {
            index = getColumnIndex(node1);
            literal = (RexLiteral) node2;
        } else {
            index = null;
            literal = null;
        }
        if (index != null) {
            map.put(index, literal);
        }
    }

    /**
     * Helper to retrieve all "ColumnRef = LiteralValue" pairs from a Calc node
     * @param condRef a local reference in form of "#5" denoting the boolean expression
     * @param exprs the list of expressions that @param condRef refers to
     * @return a collection of all {column index => literal value} pairs.
     */
    private static Map<Integer, RexLiteral> getEqualValuePredicate(RexLocalRef condRef, List<RexNode> exprs) {
        final RexNode condDeref = exprs.get(condRef.getIndex());
        if (condDeref.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) condDeref;
            RexNode left = call.getOperands().get(0), right = call.getOperands().get(1);
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            left = exprs.get(((RexLocalRef) left).getIndex());
            right = exprs.get(((RexLocalRef) right).getIndex());
            final Map<Integer, RexLiteral> m = new HashMap<>();
            addColumnIndexAndLiteral(m, left, right);
            return m;
        } else if (condDeref.isA(SqlKind.AND)) {
            return ((RexCall) (condDeref)).getOperands().stream().flatMap(node -> {
                if (node instanceof RexLocalRef) {
                    final Map<Integer, RexLiteral> r = getEqualValuePredicate((RexLocalRef) node, exprs);
                    if (! r.isEmpty()) {
                        assert r.size() == 1;
                        return r.entrySet().stream();
                    }
                }
                return Stream.empty();
            }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> x));
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Collect all "ColumnRef = LiteralValue" pairs from a Calc node
     * @param calc calc node under inspection
     * @return the collection
     */
    static Map<Integer, RexLiteral> calcCondition(Calc calc) {
        final RexProgram prog = calc.getProgram();
        return getEqualValuePredicate(prog.getCondition(), prog.getExprList());
    }

    /**
     * Search for the equivalent column set that matches with given partition column(s).
     * The matching set must include all non-null partition columns given. If no such equivalent set
     * exists, returns empty.
     * @param equalCols Equivalent set of column indices
     * @param outerPartCol partition column of outer table, or null if it is replicated.
     * @param innerPartCol partition column of inner table, or null if it is replicated.
     * @param outerTableColumns Number of columns of outer relation, used to tell from equivalent set which indices are
     *                          from the inner rel, and convert to their table-wise column indices.
     * @return an equivalent set of column indices that includes all given partition columns.
     */
    private static Optional<Set<Integer>> searchEqualPartitionColumns(
            Set<Set<Integer>> equalCols, Integer outerPartCol, Integer innerPartCol, int outerTableColumns) {
        assert outerPartCol != null || innerPartCol != null;
        if (outerPartCol != null && innerPartCol != null) {
            // Both tables are partitioned: the partition columns must appear in the same equivalent relation
            return equalCols.stream().filter(entry -> entry.contains(outerPartCol) &&
                    entry.contains(outerTableColumns + innerPartCol)).findAny();
        } else if (outerPartCol != null) {
            // Only outer table is partitioned: the partition column of outer table must equal to any column in inner table.
            return equalCols.stream().filter(entry ->
                    entry.contains(outerPartCol) && entry.stream().anyMatch(col -> col >= outerTableColumns))
                    .findAny();
        } else {
            // Only inner table is partitioned: the partition column of inner table must equal to any column in outer table.
            return equalCols.stream().filter(entry ->
                    entry.contains(outerTableColumns + innerPartCol) && entry.stream().anyMatch(col -> col < outerTableColumns))
                    .findAny();
        }
    }

    /**
     * Given that we found the set of equivalent column set of eq-join that matches all the partition columns, separate
     * them into column index of outer table and inner table (but do not adjust to absolute column index).
     * For example, if outer table is partitioned on column #1 and has 5 columns, inner table is partitioned on column #2,
     * and the matching set is (1, 6, 7), return (1, 5 + 2);
     * if the inner table is partitioned, and the matching set is (1, 2, 5), return (1, 5) because #2 is still in outer table.
     * @param eqCols The matched equivalent column indices that matches all partition columns
     * @param partCol1 absolute column index of partition column of outer rel
     * @param partCol2 absolute column index of partition column of inner rel
     * @param outerTableColumns column count of outer rel
     * @return an ordered pair of relative column indices that includes all the partition columns (and possibly more) from
     * both joining relations.
     */
    private static Pair<Integer, Integer> separateColumns(
            Set<Integer> eqCols, Integer partCol1, Integer partCol2, int outerTableColumns) {
        assert partCol1 != null || partCol2 != null;
        if (partCol1 != null && partCol2 != null) {
            return Pair.of(partCol1, partCol2 + outerTableColumns);
        } else if (partCol1 != null) {
            return Pair.of(partCol1, eqCols.stream().filter(col -> col >= outerTableColumns).findAny().get());
        } else {
            return Pair.of(eqCols.stream().filter(col -> col < outerTableColumns).findAny().get(),
                    partCol2 + outerTableColumns);
        }
    }

    static RelDistribution getDistribution(RelNode node) {
        if (node instanceof VoltLogicalTableScan) {
            return node.getTable().getDistribution();
        }
        return node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    private static void cleanseTableScan(RelNode node) {
        if (node instanceof VoltLogicalTableScan) {
            final RelDistribution dist = getDistribution(node);
            dist.setPartitionEqualValue(null);
        }
    }

    private static Pair<Map<Integer, RexLiteral>, Map<Integer, RexLiteral>> fillColumnLiteralEqualPredicates(
            RelNode outer, RelNode inner, int outerRelColumns, RexCall joinCond, Set<Integer> joinColumns) {
        final Map<Integer, RexLiteral> outerFilter, innerFilter;
        if (outer instanceof Calc) {
            outerFilter = calcCondition((Calc) outer);
        } else {
            outerFilter = new HashMap<>();
        }
        if (inner instanceof Calc) {
            innerFilter = calcCondition((Calc) inner).entrySet().stream()
                    .map(entry -> new AbstractMap.SimpleEntry<>(
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
                    final Map.Entry<Integer, RexLiteral> ret =
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
     * Check whether the given join relation is SP.
     * If it is SP, and contains partitioned tables, (which implies that there is a
     * "WHERE partitionColumn = LITERAL_VALUE"), then also set the literal value in the distribution trait.
     * @param join join node
     * @param outer outer relation
     * @param inner inner relation
     * @return true if the result of the join is SP, and sets the distribution trait for all partitioned relations.
     */
    static boolean checkSPAndPropogateDistribution(VoltLogicalJoin join, RelNode outer, RelNode inner) {
        // NOTE: we MUST cleanse the table scans to get rid of partitionEqualValue that got propagated inside,
        // because for multi-join, the result of outer join tree will set its partition equal value, which gets
        // propagated to inner table scan.
        cleanseTableScan(outer);
        cleanseTableScan(inner);
        final RelDistribution outerDist = getDistribution(outer), innerDist = getDistribution(inner);
        final Integer outerPartColumn = getPartitionColumn(outer), innerPartColumn = getPartitionColumn(inner);
        final boolean outerIsPartitioned = outerPartColumn != null,
                innerIsPartitioned = innerPartColumn != null,
                outerHasPartitionKey = outerDist.getPartitionEqualValue() != null,
                innerHasPartitionKey = innerDist.getPartitionEqualValue() != null;
        switch (join.getJoinType()) {
            case INNER:
                if (outerIsPartitioned || innerIsPartitioned) {
                    if (join.getCondition().isA(SqlKind.LITERAL)) {
                        assert join.getCondition().isAlwaysFalse() || join.getCondition().isAlwaysTrue();
                        if (join.getCondition().isAlwaysFalse()) {
                            return true;        // TODO: could join condition ever be false?
                        } else if (outerIsPartitioned && innerIsPartitioned) {
                            return outerHasPartitionKey &&
                                    outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue());
                        } else {
                            return outerHasPartitionKey || innerHasPartitionKey;
                        }
                    }
                    assert join.getCondition() instanceof RexCall;
                    final RexCall joinCondition = (RexCall) join.getCondition();
                    final Set<Set<Integer>> joinColumnSets = getAllJoiningColumns(joinCondition);
                    final int outerTableColumns = outer.getRowType().getFieldCount();
                    // Check that partition columns must be in equal-relations
                    final Set<Integer> equalPartitionColumns =
                            searchEqualPartitionColumns(
                                    joinColumnSets, outerPartColumn, innerPartColumn, outerTableColumns)
                                    .orElse(Collections.emptySet());
                    if (equalPartitionColumns.isEmpty()) {
                        if (outerHasPartitionKey && ! innerIsPartitioned || innerHasPartitionKey && ! outerIsPartitioned) {
                            // The partitioned rel has equal key; the other rel is replicated --> SP
                            return true;
                        } else if (outerHasPartitionKey &&
                                outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue())) {
                            // Both relations are partitioned with keys, and they are equal --> SP
                            return true;
                        } else {
                            return false;
                        }
                    }
                    final Pair<Integer, Integer> cols = separateColumns(
                            equalPartitionColumns, outerPartColumn, innerPartColumn, outerTableColumns);
                    final int outerJoiningCol = cols.getFirst(), innerJoiningCol = cols.getSecond();
                    // Does not join on the partition column
                    if (outerPartColumn != null && outerPartColumn != outerJoiningCol ||
                            innerPartColumn != null && innerPartColumn != innerJoiningCol - outerTableColumns) {
                        return false;
                    }
                    // missing filters on table scans
                    final Pair<Map<Integer, RexLiteral>, Map<Integer, RexLiteral>> r =
                            fillColumnLiteralEqualPredicates(outer, inner, outerTableColumns,
                                    joinCondition, equalPartitionColumns);
                    final Map<Integer, RexLiteral> outerFilter = r.getFirst(), innerFilter = r.getSecond();
                    if (joinColumnSets.isEmpty() ||
                            joinColumnSets.stream().noneMatch(set ->
                                    (outerPartColumn == null || set.contains(outerPartColumn)) &&
                                            (innerPartColumn == null || set.contains(innerPartColumn + outerTableColumns)))) {
                        if (outerIsPartitioned && innerIsPartitioned) {
                            // Both relations are partitioned; but the join condition
                            // does **not** join on their partition columns.
                            // We can assert that VoltDB cannot handle such join.
                            throw new PlanningErrorException("SQL error while compiling query: " +
                                    "This query is not plannable.  " +
                                    "The planner cannot guarantee that all rows would be in a single partition.");
                        } else {
                            // only one relation is partitioned. Depending on whether the partitioned table has
                            // the equal value set, it is either SP (if the equal value is set and not null), or
                            // MP (but VoltDB can handle it).
                            return outerIsPartitioned && outerDist.getPartitionEqualValue() != null ||
                                    innerIsPartitioned && innerDist.getPartitionEqualValue() != null;
                        }
                    }
                    final boolean
                            isOuterPartitionedSP = outerIsPartitioned && outerDist.getPartitionEqualValue() != null,
                            isInnerPartitionedSP = innerIsPartitioned && innerDist.getPartitionEqualValue() != null;
                    if (isOuterPartitionedSP || isInnerPartitionedSP) {
                        // multiple join with either of:
                        // 1. a joining multi-partitioned SP node and a replicated table
                        // 2. both are joining multi-partitioned SP nodes
                        // then, the result is still SP
                        if (isOuterPartitionedSP != isInnerPartitionedSP && outerIsPartitioned == innerIsPartitioned) {
                            // propagate distribution equal-value to the other unset table scan, for the case that
                            // both outer/inner are partitioned, but only one has partition equal value set.
                            if (isOuterPartitionedSP) {
                                innerDist.setPartitionEqualValue(outerDist.getPartitionEqualValue());
                            } else {
                                outerDist.setPartitionEqualValue(innerDist.getPartitionEqualValue());
                            }
                        }
                        return true;
                    }
                    // separate condFilter into outerFilter and innerFilter
                    if (outerFilter.isEmpty() && innerFilter.isEmpty()) {
                        return false; // No equal-filter found
                    }
                    final boolean outerTableHasEqValue =
                            outerFilter.entrySet().stream().anyMatch(entry ->
                                    entry.getKey().equals(outerPartColumn));
                    final boolean innerTableHasEqValue =
                            innerFilter.entrySet().stream().anyMatch(entry ->
                                    entry.getKey().equals(innerPartColumn + outerTableColumns));
                    if (outerTableHasEqValue || innerTableHasEqValue) {
                        // Has an equal-filter on partitioned column
                        // NOTE: in case there are 2 equal predicated on each partitioned column each,
                        // we just take out the first one.
                        // TODO: in that case, checking that they are equal or not, and optimize away is a
                        // minor-usage case of optimization
                        (outerTableHasEqValue ? outerFilter.entrySet() : innerFilter.entrySet())
                                .stream().flatMap(entry -> {
                            final int columnIndex = entry.getKey();
                            if (outerPartColumn == columnIndex || innerPartColumn == columnIndex - outerTableColumns) {
                                return Stream.of(entry.getValue());
                            } else {
                                return Stream.empty();
                            }
                        }).findAny().ifPresent(value -> {
                            if (innerIsPartitioned) {
                                innerDist.setPartitionEqualValue(value);
                            }
                            if (outerIsPartitioned) {
                                outerDist.setPartitionEqualValue(value);
                            }
                        });
                    } else {    // no equal-filter on partition column
                        return false;
                    }
                } // else Both are replicated: SP
                return true;
            default: // Not inner-join type involving at least a partitioned table
                return !outerIsPartitioned && !innerIsPartitioned;
        }
    }

    /**
     * Trace the condition from a Calc node until we found that it is in the form of "column_p = literal",
     * where column_p is the partition column provided.
     * TODO: we need to make it more capable so that literals in conditions like
     * "column_s = column_p AND column_p = literal" could be extracted.
     *
     * @param cond Calc condition, usually of {\code RexLocalRef}.
     * @param src List of local/input references, or literals to search for
     * @param partitionCol partition column of the partition table
     * @return the literal if found, null otherwise.
     */
    private static RexLiteral matchedColumnLiteral(RexNode cond, List<RexNode> src, int partitionCol) {
        if (cond.isA(SqlKind.LOCAL_REF)) {
            return matchedColumnLiteral(src.get(((RexLocalRef) cond).getIndex()), src, partitionCol);
        } else if (cond.isA(SqlKind.EQUALS)) {
            final RexCall eq = (RexCall) cond;
            final RexNode left = eq.getOperands().get(0), right = eq.getOperands().get(1);
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            final RexNode derefLeft = src.get(((RexLocalRef) left).getIndex()),
                    derefRight = src.get(((RexLocalRef) right).getIndex());
            if (derefLeft.isA(SqlKind.INPUT_REF) && derefRight.isA(SqlKind.LITERAL) ||
                    derefRight.isA(SqlKind.INPUT_REF) && derefLeft.isA(SqlKind.LITERAL)) {
                final int colIndex =
                        ((RexInputRef) (derefLeft.isA(SqlKind.INPUT_REF) ? derefLeft : derefRight)).getIndex();
                if (colIndex == partitionCol) {
                    return (RexLiteral) (derefLeft.isA(SqlKind.LITERAL) ? derefLeft : derefRight);
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else if (cond.isA(SqlKind.AND)) {
            return ((RexCall) cond).getOperands().stream()
                    .flatMap(node -> {
                        final RexLiteral result = matchedColumnLiteral(node, src, partitionCol);
                        if (result == null) {
                            return Stream.empty();
                        } else {
                            return Stream.of(result);
                        }
                    }).findAny().orElse(null);
        } else if (cond.isA(SqlKind.NOT)) {
            return null;    // TODO
        } else {
            return null;
        }
    }

    static boolean isCalcScanSP(VoltLogicalTableScan scan, VoltLogicalCalc calc) {
        final RelDistribution dist = scan.getTable().getDistribution();
        switch (dist.getType()) {
            case SINGLETON:
                return true;
            case RANDOM_DISTRIBUTED:        // View
                return false;
            case HASH_DISTRIBUTED:
                if (dist.getPartitionEqualValue() != null) {    // equal value already present
                    return true;
                } else if (calc.getProgram().getCondition() == null) {
                    return false;
                } else {    // find equal value in calc node
                    final RexLiteral literal = matchedColumnLiteral(
                            calc.getProgram().getCondition(), calc.getProgram().getExprList(), dist.getKeys().get(0));
                    if (literal != null) {
                        dist.setPartitionEqualValue(literal);
                    }
                    return literal != null;
                }
            default:
                return false;
        }
    }
}
