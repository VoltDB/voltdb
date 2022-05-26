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

package org.voltdb.planner.parseinfo;

import java.util.*;
import java.util.stream.Collectors;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.AccessPath;
import org.voltdb.planner.StmtEphemeralTableScan;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

/**
 * JoinNode class captures the hierarchical data model of a given SQl join.
 * Each node in the tree can be either a leaf representing a joined table (m_table != null)
 * or a node representing the actual join (m_leftNode!=0 and m_rightNode!=0)
 * filter expressions
 */
public abstract class JoinNode implements Cloneable {
    // Node id. Must be unique within a given tree
    protected int m_id;
    // Join expression associated with this node
    protected AbstractExpression m_joinExpr = null;
    // Additional filter expression (WHERE) associated with this node
    protected AbstractExpression m_whereExpr = null;

    // Buckets for children expression classification
    public final List<AbstractExpression> m_joinOuterList = new ArrayList<>();
    public final List<AbstractExpression> m_joinInnerList = new ArrayList<>();
    public final List<AbstractExpression> m_joinInnerOuterList = new ArrayList<>();
    public final List<AbstractExpression> m_whereOuterList = new ArrayList<>();
    public final List<AbstractExpression> m_whereInnerList = new ArrayList<>();
    public final List<AbstractExpression> m_whereInnerOuterList = new ArrayList<>();
    protected Set<String> m_tableAliases = new HashSet<>();

    // All possible access paths for this node
    public List<AccessPath> m_accessPaths = new ArrayList<>();
    // Access path under the evaluation
    public AccessPath m_currentAccessPath = null;

    /**
     * Construct an empty join node
     */
    protected JoinNode(int id) {
        m_id = id;
    }

    /**
     * Deep clone
     */
    @Override
    abstract public Object clone();

    public int getId() {
        return m_id;
    }

    public void setId(int id) {
        m_id = id;
    }

    @SuppressWarnings("static-method")
    public JoinNode getLeftNode() {
        return null;
    }

    @SuppressWarnings("static-method")
    public JoinNode getRightNode() {
        return null;
    }

    public AbstractExpression getJoinExpression() {
        return m_joinExpr;
    }

    public void setJoinExpression(AbstractExpression expr) {
        m_joinExpr = expr;
    }

    public AbstractExpression getWhereExpression() {
        return m_whereExpr;
    }

    public void setWhereExpression(AbstractExpression expr) {
        m_whereExpr = expr;
    }

    /// For debug purposes:
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        explain_recurse(sb, "");
        return sb.toString();
    }

    public void explain_recurse(StringBuilder sb, String indent) {
        // Node id. Must be unique within a given tree
        sb.append(indent).append("JOIN NODE id: ").append(m_id).append("\n");
        // Table
        sb.append(indent).append("table alias: ").append(getTableAlias()).append("\n");
        // Join expression associated with this node
        if (m_joinExpr != null) {
            sb.append(indent).append(m_joinExpr.explain("be explicit")).append("\n");
        }
        // Additional filter expression (WHERE) associated with this node
        if (m_whereExpr != null) {
            sb.append(indent).append(m_whereExpr.explain("be explicit")).append("\n");
        }
        // Buckets for children expression classification
        explain_filter_list(sb, indent, "join outer:", m_joinOuterList);
        explain_filter_list(sb, indent, "join inner:", m_joinInnerList);
        explain_filter_list(sb, indent, "join inner outer:", m_joinInnerOuterList);
        explain_filter_list(sb, indent, "where outer:", m_whereOuterList);
        explain_filter_list(sb, indent, "where inner:", m_whereInnerList);
        explain_filter_list(sb, indent, "where inner outer:", m_whereInnerOuterList);
    }

    protected static void explain_filter_list(StringBuilder sb, String indent, String label,
                                     Collection<AbstractExpression> filterListMember) {
        String prefix = label + "\n" + indent;
        for (AbstractExpression filter : filterListMember) {
            sb.append(prefix).append(filter.explain("be explicit")).append("\n");
            prefix = indent;
        }
    }

    /**
     * Summarize the WHERE clause expressions of type COMPARE_EQUAL for the entire statement,
     * that can be used to determine query statement partitioning.
     * This is tricky in the case of JOIN filters because "partition_column = constant"
     * does not exclude joined result rows from occurring on all partitions.
     * This generally still requires a multi-partition plan.
     * But a join filter of the form "outer.partition_column = inner.partition_column" DOES allow
     * a multi-partition join to be executed in parallel on each partition -- each tuple on the OUTER
     * side will either get properly matched OR get a null-padded tuple on its local partition and any
     * differently-valued inner side rows that may exist on other partitions have no bearing on this.
     * So ALL manner of where clause but ONLY inner-outer column=column JOIN clauses can
     * influence partitioning.
     */
    public Map<AbstractExpression, Set<AbstractExpression>> getAllEquivalenceFilters() {
        Map<AbstractExpression, Set<AbstractExpression>> equivalenceSet = new HashMap<>();
        Deque<JoinNode> joinNodes = new ArrayDeque<>();
        // Iterate over the join nodes to collect their join and where equivalence filter expressions
        joinNodes.add(this);
        while (! joinNodes.isEmpty()) {
            JoinNode joinNode = joinNodes.poll();
            joinNode.collectEquivalenceFilters(equivalenceSet, joinNodes);
        }
        return equivalenceSet;
    }

    protected void collectEquivalenceFilters(Map<AbstractExpression,
            Set<AbstractExpression>> equivalenceSet, Deque<JoinNode> joinNodes) {
        if ( ! m_whereInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerList, equivalenceSet);
        }
        // HSQL sometimes tags single-table filters in inner joins as join clauses
        // rather than where clauses? OR does analyzeJoinExpressions correct for this?
        // If so, these CAN contain constant equivalences that get used as the basis for equivalence
        // conditions that determine partitioning, so process them as where clauses.
        if ( ! m_joinInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_joinInnerList, equivalenceSet);
        }
    }

    /**
     * Collect all JOIN and WHERE expressions combined with AND for the entire tree.
     */
    public AbstractExpression getAllFilters() {
        Deque<JoinNode> joinNodes = new ArrayDeque<>();
        Deque<AbstractExpression> in = new ArrayDeque<>();
        Deque<AbstractExpression> out = new ArrayDeque<>();
        // Iterate over the join nodes to collect their join and where expressions
        joinNodes.add(this);
        while (!joinNodes.isEmpty()) {
            JoinNode joinNode = joinNodes.poll();
            if (joinNode.m_joinExpr != null) {
                in.add(joinNode.m_joinExpr);
            }
            if (joinNode.m_whereExpr != null) {
                in.add(joinNode.m_whereExpr);
            }
            joinNode.queueChildren(joinNodes);
        }

        // this chunk of code breaks the code into a list of expression that
        // all have to be true for the where clause to be true
        AbstractExpression inExpr;
        while ((inExpr = in.poll()) != null) {
            if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                in.add(inExpr.getLeft());
                in.add(inExpr.getRight());
            } else {
                out.add(inExpr);
            }
        }
        return ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, out);
    }

    protected abstract void queueChildren(Deque<JoinNode> joinNodes);

    /**
     * Get the WHERE expression for a single-table statement.
     */
    public AbstractExpression getSimpleFilterExpression() {
        if (m_whereExpr != null) {
            if (m_joinExpr != null) {
                return ExpressionUtil.combine(m_whereExpr, m_joinExpr);
            } else {
                return m_whereExpr;
            }
        } else {
            return m_joinExpr;
        }
    }

    /**
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    public List<JoinNode> generateLeafNodesJoinOrder() {
        List<JoinNode> leafNodes = new ArrayList<>();
        listNodesJoinOrderRecursive(leafNodes, false);
        return leafNodes;
    }

    /**
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    public Collection<String> generateTableJoinOrder() {
        return generateLeafNodesJoinOrder().stream()
                .map(JoinNode::getTableAlias)
                .collect(Collectors.toList());
    }

    public abstract String getTableAlias();

    /**
     * Returns nodes in the order they are joined in the tree by iterating the tree depth-first
     */
    public List<JoinNode> generateAllNodesJoinOrder() {
        ArrayList<JoinNode> nodes = new ArrayList<>();
        listNodesJoinOrderRecursive(nodes, true);
        return nodes;
    }

    protected void listNodesJoinOrderRecursive(List<JoinNode> nodes, boolean appendBranchNodes) {
        nodes.add(this);
    }

    /**
     * Returns true if one of the tree nodes has outer join
     */
    @SuppressWarnings("static-method")
    public boolean hasOuterJoin() {
        return false;
    }

    /**
     * Returns a list of immediate sub-queries which are part of this query.
     * @return List<AbstractParsedStmt> - list of sub-queries from this query
     */
    abstract public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans);

    /**
     * Split a join tree into one or more sub-trees. Each sub-tree has the same join type
     * for all join nodes. The root of the child tree in the parent tree is replaced with a 'dummy' node
     * which id is negated id of the child root node.
     * @param tree - The join tree
     * @return the list of sub-trees from the input tree
     */
    public List<JoinNode> extractSubTrees() {
        List<JoinNode> subTrees = new ArrayList<>();
        // Extract the first sub-tree starting at the root
        subTrees.add(this);

        List<JoinNode> leafNodes = new ArrayList<>();
        extractSubTree(leafNodes);
        // Continue with the leafs
        for (JoinNode leaf : leafNodes) {
            subTrees.addAll(leaf.extractSubTrees());
        }
        return subTrees;
    }

    /**
     * Starting from the root recurse to its children stopping at the first join node
     * of the different type and discontinue the tree at this point by replacing the join node with
     * the temporary node which id matches the join node id. This join node is the root of the next
     * sub-tree.
     * @param root - The root of the join tree
     * @param leafNodes - the list of the root nodes of the next sub-trees
     */
    protected void extractSubTree(List<JoinNode> leafNodes) { }

    /**
     * Reconstruct a join tree from the list of tables always appending the next node to the right.
     *
     * @param tableNodes the list of tables to build the tree from.
     * @param joinType the join type for all the joins
     * @return The reconstructed tree
     */
    public static JoinNode reconstructJoinTreeFromTableNodes(List<JoinNode> tableNodes, JoinType joinType) {
        JoinNode root = null;
        for (JoinNode leafNode : tableNodes) {
            JoinNode node = leafNode.cloneWithoutFilters();
            if (root == null) {
                root = node;
            } else {
                // We only care about the root node id to be able to reconnect the sub-trees
                // The intermediate node id can be anything. For the final root node its id
                // will be set later to the original tree's root id
                root = new BranchNode(-node.m_id, joinType, root, node);
            }
        }
        return root;
    }

    @SuppressWarnings("static-method")
    protected JoinNode cloneWithoutFilters() {
        assert(false);
        return null;
    }

    /**
     * Reconstruct a join tree from the list of sub-trees connecting the sub-trees in the order
     * they appear in the list. The list of sub-trees must be initially obtained by calling the extractSubTrees
     * method on the original tree.
     *
     * @param subTrees the list of sub trees.
     * @return The reconstructed tree
     */
    public static JoinNode reconstructJoinTreeFromSubTrees(List<JoinNode> subTrees) {
        if (subTrees == null || subTrees.isEmpty()) {
            return null;
        }
        // Reconstruct the tree. The first element is the first sub-tree and so on
        JoinNode joinNode = subTrees.get(0);
        for (int i = 1; i < subTrees.size(); ++i) {
            JoinNode nextNode = subTrees.get(i);
            boolean replaced = joinNode.replaceChild(nextNode);
            // There must be a node in the current tree to be replaced
            assert(replaced);
        }
        return joinNode;
    }

    protected boolean replaceChild(JoinNode node) {
        // can't replace self
        assert Math.abs(m_id) != Math.abs(node.m_id);
        return false;
    }

    /**
     * Analyze the m_joinTree structure to extract useful components.
     * @param stmt parsed statement
     */
    public void analyzeJoinExpressions(AbstractParsedStmt stmt) {
        m_joinInnerList.addAll(ExpressionUtil.uncombineAny(getJoinExpression()));
        m_whereInnerList.addAll(ExpressionUtil.uncombineAny(getWhereExpression()));
        m_tableAliases = new HashSet<>(stmt.m_joinTree.generateTableJoinOrder());
    }

    /**
     * Apply implied transitive constant filter to join expressions
     * outer.partkey = ? and outer.partkey = inner.partkey is equivalent to
     * outer.partkey = ? and inner.partkey = ?
     * @param innerTableExprs inner table expressions
     * @param outerTableExprs outer table expressions
     * @param innerOuterTableExprs inner-outer tables expressions
     */
    protected static void applyTransitiveEquivalence(List<AbstractExpression> outerTableExprs,
            List<AbstractExpression> innerTableExprs, List<AbstractExpression> innerOuterTableExprs) {
        final List<AbstractExpression>
                simplifiedOuterExprs = applyTransitiveEquivalence(innerTableExprs, innerOuterTableExprs),
                simplifiedInnerExprs = applyTransitiveEquivalence(outerTableExprs, innerOuterTableExprs);
        outerTableExprs.addAll(simplifiedOuterExprs);
        innerTableExprs.addAll(simplifiedInnerExprs);
    }

    private static List<AbstractExpression> applyTransitiveEquivalence(
            List<AbstractExpression> singleTableExprs, List<AbstractExpression> twoTableExprs) {
        List<AbstractExpression> simplifiedExprs = new ArrayList<>();
        Map<AbstractExpression, Set<AbstractExpression>> eqMap1 = new HashMap<>();
        ExpressionUtil.collectPartitioningFilters(singleTableExprs, eqMap1);

        for (AbstractExpression expr : twoTableExprs) {
            if (! expr.isColumnEquivalenceFilter()) {
                continue;
            }
            AbstractExpression leftExpr = expr.getLeft();
            AbstractExpression rightExpr = expr.getRight();
            assert(leftExpr instanceof TupleValueExpression && rightExpr instanceof TupleValueExpression);
            Set<AbstractExpression> eqSet1 = eqMap1.get(leftExpr);
            AbstractExpression singleExpr = leftExpr;
            if (eqSet1 == null) {
                eqSet1 = eqMap1.get(rightExpr);
                if (eqSet1 == null) {
                    continue;
                }
                singleExpr = rightExpr;
            }

            for (AbstractExpression eqExpr : eqSet1) {
                if (eqExpr instanceof ConstantValueExpression) {
                    if (singleExpr == leftExpr) {
                        expr.setLeft(eqExpr);
                    } else {
                        expr.setRight(eqExpr);
                    }
                    simplifiedExprs.add(expr);
                    // Having more than one const value for a single column doesn't make
                    // much sense, right?
                    break;
                }
            }

        }

         twoTableExprs.removeAll(simplifiedExprs);
         return simplifiedExprs;
    }

    /**
     * Split the input expression list into the three categories
     * 1. TVE expressions with outer tables only
     * 2. TVE expressions with inner tables only
     * 3. TVE expressions with inner and outer tables
     * The outer tables are the tables reachable from the outer node of the join
     * The inner tables are the tables reachable from the inner node of the join
     * @param exprList expression list to split
     * @param outerTables outer table
     * @param innerTables outer table
     * @param outerList expressions with outer table only
     * @param innerList expressions with inner table only
     * @param innerOuterList with inner and outer tables
     */
    protected static void classifyJoinExpressions(Collection<AbstractExpression> exprList,
            Collection<String> outerTables, Collection<String> innerTables,
            List<AbstractExpression> outerList, List<AbstractExpression> innerList,
            List<AbstractExpression> innerOuterList, List<AbstractExpression> noneList) {
        final Set<String> outerSet = new HashSet<>(outerTables);
        final Set<String> innerSet = new HashSet<>(innerTables);
        for (AbstractExpression expr : exprList) {
            final Set<String> tableAliasSet = getTablesForExpression(expr);
            if (tableAliasSet.isEmpty()) {
                noneList.add(expr);
            } else {
                final boolean
                        outer = tableAliasSet.stream().anyMatch(outerSet::contains),
                        inner = tableAliasSet.stream().anyMatch(innerSet::contains);
                if (! outer && ! inner) {
                    // alias not found in either outer/inner table. This happens in multi-join query.
                    // See also BranchNode#analyzeJoinExpression().
                    throw new SubPlanAssembler.SkipCurrentPlanException();
                } else if (outer && inner) {
                    innerOuterList.add(expr);
                } else if (outer) {
                    outerList.add(expr);
                } else {
                    innerList.add(expr);
                }
            }
        }
    }

    private static Set<String> getTablesForExpression(AbstractExpression expr) {
        return ExpressionUtil
                .getTupleValueExpressions(expr)
                .stream()
                .map(TupleValueExpression::getTableAlias)
                .collect(Collectors.toSet());
    }

    @SuppressWarnings("static-method")
    public StmtTableScan getTableScan() {
        return null;
    }

    private String m_contentDeterminismMessage = null;

    public String getContentDeterminismMessage() {
        return m_contentDeterminismMessage;
    }

    public void updateContentDeterminismMessage(String msg) {
        if (m_contentDeterminismMessage == null) {
            m_contentDeterminismMessage = msg;
        }
    }

    /**
     * Returns if all the join operations within this join tree are inner joins.
     * @return true or false.
     */
    public boolean allInnerJoins() {
        return true;
    }

    public void gatherJoinExpressions(List<AbstractExpression> checkExpressions) {
        if (m_joinExpr != null) {
            checkExpressions.add(m_joinExpr);
        }
        if (m_whereExpr != null) {
            checkExpressions.addAll(checkExpressions);
        }
    }

    abstract public boolean hasSubqueryScans();
}
