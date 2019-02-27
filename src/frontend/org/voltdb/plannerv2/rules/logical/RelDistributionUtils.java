package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
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
    private static Stream<Pair<Integer, RexNode>> getColumnValuePairs(RexCall call) {
        if (call.isA(SqlKind.EQUALS)) {
            final RexNode left = uncast(call.getOperands().get(0)), right = uncast(call.getOperands().get(1));
            if (left instanceof RexInputRef && RexUtil.isLiteral(right) ||
                    right instanceof RexInputRef && RexUtil.isLiteral(left)) {
                final int col;
                final RexNode literal;
                if (right.isA(SqlKind.LITERAL)) {
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

    private static RexNode uncast(RexNode node) {
        if (node.isA(SqlKind.CAST)) {
            return ((RexCall) node).getOperands().get(0);
        } else {
            return node;
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
            final RexNode leftConj = uncast(joinCondition.getOperands().get(0)),
                    rightConj = uncast(joinCondition.getOperands().get(1));
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
            isJoinSP(join, join.getLeft(), join.getRight());
            return null;
        }
    }

    /**
     * Retrieve the partition column of a table scan node. For replicated table, returns null.
     * @param tbl table scan node
     * @return partition column
     */
    private static Integer getPartitionColumn(RelNode tbl) {
        final RelDistribution dist;
        if (tbl instanceof TableScan) {
            dist = tbl.getTable().getDistribution();
        } else {
            dist = tbl.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
        }
        if (dist.getKeys().isEmpty()) {
            return null;
        } else {
            return dist.getKeys().get(0);
        }
        /*
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
        } */
    }

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
    private static void addColumnIndexAndLiteral(
            Map<Integer, RexNode> map, RexNode node1, RexNode node2) {
        final RexNode literal;
        final Integer index;
        if (node1.isA(SqlKind.LITERAL)) {
            literal = node1;
            index = getColumnIndex(node2);
        } else if (node2.isA(SqlKind.LITERAL)) {
            index = getColumnIndex(node1);
            literal = node2;
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
    private static Map<Integer, RexNode> getEqualValuePredicate(RexLocalRef condRef, List<RexNode> exprs) {
        final RexNode condDeref = exprs.get(condRef.getIndex());
        if (condDeref.isA(SqlKind.EQUALS)) {
            final RexCall call = (RexCall) condDeref;
            RexNode left = uncast(call.getOperands().get(0)), right = uncast(call.getOperands().get(1));
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            left = exprs.get(((RexLocalRef) left).getIndex());
            right = exprs.get(((RexLocalRef) right).getIndex());
            final Map<Integer, RexNode> m = new HashMap<>();
            addColumnIndexAndLiteral(m, left, right);
            return m;
        } else if (condDeref.isA(SqlKind.AND)) {
            return ((RexCall) (condDeref)).getOperands().stream().flatMap(node -> {
                if (node instanceof RexLocalRef) {
                    final Map<Integer, RexNode> r = getEqualValuePredicate((RexLocalRef) node, exprs);
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
    static Map<Integer, RexNode> calcCondition(Calc calc) {
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

    static final class JoinState {
        final private boolean m_isSP;
        final private RexNode m_literal;
        public JoinState(boolean isSP, RexNode literal) {
            m_isSP = isSP;
            m_literal = literal;
        }
        public boolean isSP() {
            return m_isSP;
        }
        public RexNode getLiteral() {
            return m_literal;
        }
    }

    private static RexNode literalOr(RexNode left, RexNode right) {
        return left == null ? right : left;
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
        final Integer outerPartColumn = getPartitionColumn(outer), innerPartColumn = getPartitionColumn(inner);
        final boolean outerIsPartitioned = outerPartColumn != null,
                innerIsPartitioned = innerPartColumn != null,
                outerHasPartitionKey = outerDist.getPartitionEqualValue() != null,
                innerHasPartitionKey = innerDist.getPartitionEqualValue() != null;
        final RexNode srcLitera = literalOr(outerDist.getPartitionEqualValue(), innerDist.getPartitionEqualValue());
        switch (join.getJoinType()) {
            case INNER:
                if (outerIsPartitioned || innerIsPartitioned) {
                    if (join.getCondition().isA(SqlKind.LITERAL)) {
                        assert join.getCondition().isAlwaysFalse() || join.getCondition().isAlwaysTrue();
                        if (join.getCondition().isAlwaysFalse()) {        // TODO: could join condition ever be false?
                            return new JoinState(true, srcLitera);
                        } else if (outerIsPartitioned && innerIsPartitioned) {
                            return new JoinState(
                                    outerHasPartitionKey &&
                                            outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue()),
                                    srcLitera);
                        } else {
                            return new JoinState(outerHasPartitionKey || innerHasPartitionKey, srcLitera);
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
                            return new JoinState(true, srcLitera);
                        } else if (outerHasPartitionKey &&
                                outerDist.getPartitionEqualValue().equals(innerDist.getPartitionEqualValue())) {
                            // Both relations are partitioned with keys, and they are equal --> SP
                            return new JoinState(true, srcLitera);
                        } else {
                            return new JoinState(false, srcLitera);
                        }
                    }
                    final Pair<Integer, Integer> cols = separateColumns(
                            equalPartitionColumns, outerPartColumn, innerPartColumn, outerTableColumns);
                    final int outerJoiningCol = cols.getFirst(), innerJoiningCol = cols.getSecond();
                    // Does not join on the partition column
                    if (outerPartColumn != null && outerPartColumn != outerJoiningCol ||
                            innerPartColumn != null && innerPartColumn != innerJoiningCol - outerTableColumns) {
                        return new JoinState(false, srcLitera);
                    }
                    // missing filters on table scans
                    final Pair<Map<Integer, RexNode>, Map<Integer, RexNode>> r =
                            fillColumnLiteralEqualPredicates(outer, inner, outerTableColumns,
                                    joinCondition, equalPartitionColumns);
                    final Map<Integer, RexNode> outerFilter = r.getFirst(), innerFilter = r.getSecond();
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
                            return new JoinState(
                                    outerIsPartitioned && outerDist.getPartitionEqualValue() != null ||
                                            innerIsPartitioned && innerDist.getPartitionEqualValue() != null,
                                    srcLitera);
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
                        return new JoinState(true, srcLitera);
                    }
                    // separate condFilter into outerFilter and innerFilter
                    if (outerFilter.isEmpty() && innerFilter.isEmpty()) {
                        return new JoinState(false, null); // No equal-filter found
                    }
                    final boolean outerTableHasEqValue =
                            outerPartColumn != null &&
                                    outerFilter.entrySet().stream().anyMatch(entry ->
                                            entry.getKey().equals(outerPartColumn));
                    final boolean innerTableHasEqValue =
                            innerPartColumn != null &&
                                    innerFilter.entrySet().stream().anyMatch(entry ->
                                            entry.getKey().equals(innerPartColumn + outerTableColumns));
                    if (outerTableHasEqValue || innerTableHasEqValue) {
                        // Has an equal-filter on partitioned column
                        // NOTE: in case there are 2 equal predicated on each partitioned column each,
                        // we just take out the first one.
                        return new JoinState(true,
                                (outerTableHasEqValue ? outerFilter.entrySet() : innerFilter.entrySet())
                                        .stream().flatMap(entry -> {
                                    final int columnIndex = entry.getKey();
                                    if (outerPartColumn != null && outerPartColumn == columnIndex ||
                                            innerPartColumn != null && innerPartColumn == columnIndex - outerTableColumns) {
                                        return Stream.of(entry.getValue());
                                    } else {
                                        return Stream.empty();
                                    }
                                }).findAny().orElse(null));
                    } else {    // no equal-filter on partition column
                        return new JoinState(false, srcLitera);
                    }
                } else { // else Both are replicated: SP
                    return new JoinState(true, srcLitera);
                }
            default: // Not inner-join type involving at least a partitioned table
                return new JoinState(!outerIsPartitioned && !innerIsPartitioned, srcLitera);
        }
    }

    /**
     * Flattens a RexCall that likely refers to some local index(es) of a given list of references,
     * so that the flattend RexCall does not refer to any external local references (but could refer to
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
     * TODO: we need to make it more capable so that literals in conditions like
     * "column_s = column_p AND column_p = literal" could be extracted.
     *
     * @param cond Calc condition, usually of {\code RexLocalRef}.
     * @param src List of local/input references, or literals to search for
     * @param partitionCol partition column of the partition table
     * @return the literal if found, null otherwise.
     */
    private static RexNode matchedColumnLiteral(RexNode cond, List<RexNode> src, int partitionCol) {
        if (cond.isA(SqlKind.LOCAL_REF)) {
            return matchedColumnLiteral(src.get(((RexLocalRef) cond).getIndex()), src, partitionCol);
        } else if (cond.isA(SqlKind.EQUALS)) {
            final RexCall eq = (RexCall) cond;
            final RexNode left = eq.getOperands().get(0), right = eq.getOperands().get(1);
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            final RexNode derefLeft = flattenRexCall(src.get(((RexLocalRef) left).getIndex()), src),
                    derefRight = flattenRexCall(src.get(((RexLocalRef) right).getIndex()), src);
            final boolean isLeftLiteral = RexUtil.isLiteral(derefLeft),
                    isRightLiteral = RexUtil.isLiteral(derefRight);
            if (derefLeft.isA(SqlKind.INPUT_REF) && isRightLiteral ||
                    derefRight.isA(SqlKind.INPUT_REF) && isLeftLiteral) {
                final int colIndex =
                        ((RexInputRef) (derefLeft.isA(SqlKind.INPUT_REF) ? derefLeft : derefRight)).getIndex();
                if (colIndex == partitionCol) {
                    return derefLeft.isA(SqlKind.INPUT_REF) ? derefRight : derefLeft;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else if (cond.isA(SqlKind.AND)) {
            return ((RexCall) cond).getOperands().stream()
                    .flatMap(node -> {
                        final RexNode result = matchedColumnLiteral(node, src, partitionCol);
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

    static Pair<Boolean, RexNode> isCalcScanSP(VoltLogicalTableScan scan, VoltLogicalCalc calc) {
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
                    final RexNode literal = matchedColumnLiteral(
                            calc.getProgram().getCondition(), calc.getProgram().getExprList(), dist.getKeys().get(0));
                    if (literal != null) {
                        if (calcDist.getPartitionEqualValue() != null &&
                                ! calcDist.getPartitionEqualValue().equals(literal)) {
                            // Calc's distribution key had already been set to a different value
                            return Pair.of(false, calcDist.getPartitionEqualValue());
                        } else {
                            /*scan.getTraitSet().replace(dist);
                            calc.getTraitSet().replace(dist);
                            setPartitionEqualKey(scan, literal);
                            setPartitionEqualKey(calc, literal);*/
                            return Pair.of(true, literal);
                        }
                    } else {
                        return Pair.of(false, null);
                    }
                }
            case RANDOM_DISTRIBUTED:        // View
            default:
                return Pair.of(false, null);
        }
    }
}
