/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

/**
 * JoinNode class captures the hierarchical data model of a given SQl join.
 * Each node in the tree can be either a leaf representing a joined table (m_table != null)
 * or a node representing the actual join (m_leftNode!=0 and m_rightNode!=0)
 * filter expressions
 */
public class JoinNode implements Cloneable {

    // Left child
    public JoinNode m_leftNode = null;
    // Right child
    public JoinNode m_rightNode = null;
    // index into the query catalog cache for a table alias
    public int m_tableAliasIndex = StmtTableScan.NULL_ALIAS_INDEX;
    // Join type
    public JoinType m_joinType = JoinType.INNER;
    // Join expression associated with this node
    public AbstractExpression m_joinExpr = null;
    // Additional filter expression (WHERE) associated with this node
    public AbstractExpression m_whereExpr = null;
    // Node id. Must be unique within a given tree
    public int m_id = 0;

    // Buckets for children expression classification
    public final ArrayList<AbstractExpression> m_joinOuterList = new ArrayList<AbstractExpression>();
    public final ArrayList<AbstractExpression> m_joinInnerList = new ArrayList<AbstractExpression>();
    public final ArrayList<AbstractExpression> m_joinInnerOuterList = new ArrayList<AbstractExpression>();
    public final ArrayList<AbstractExpression> m_whereOuterList = new ArrayList<AbstractExpression>();
    public final ArrayList<AbstractExpression> m_whereInnerList = new ArrayList<AbstractExpression>();
    public final ArrayList<AbstractExpression> m_whereInnerOuterList = new ArrayList<AbstractExpression>();

    // All possible access paths for this node
    List<AccessPath> m_accessPaths = new ArrayList<AccessPath>();
    // Access path under the evaluation
    AccessPath m_currentAccessPath = null;

    /**
     * Construct a leaf node
     * @param id - node unique id
     * @param joinType - join type
     * @param table - join table index
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    JoinNode(int id, JoinType joinType, int tableAliasIdx, AbstractExpression joinExpr, AbstractExpression  whereExpr) {
        this(id, joinType);
        m_tableAliasIndex = tableAliasIdx;
        m_joinExpr = joinExpr;
        m_whereExpr = whereExpr;
    }

    /**
     * Construct a join node
     * @param id - node unique id
     * @param joinType - join type
     * @param leftNode - left node
     * @param rightNode - right node
     */
    JoinNode(int id, JoinType joinType, JoinNode leftNode, JoinNode rightNode) {
        this(id, joinType);
        m_leftNode = leftNode;
        m_rightNode = rightNode;
    }

    /**
     * Construct an empty join node
     */
    private JoinNode(int id, JoinType joinType) {
        m_id = id;
        m_joinType = joinType;
    }

    /**
     * Deep clone
     */
    @Override
    public Object clone() {
        JoinNode newNode = new JoinNode(m_id, m_joinType);
        if (m_joinExpr != null) {
            newNode.m_joinExpr = (AbstractExpression) m_joinExpr.clone();
        }
        if (m_whereExpr != null) {
            newNode.m_whereExpr = (AbstractExpression) m_whereExpr.clone();
        }
        if (m_tableAliasIndex == StmtTableScan.NULL_ALIAS_INDEX) {
            assert(m_leftNode != null && m_rightNode != null);
            newNode.m_leftNode = (JoinNode) m_leftNode.clone();
            newNode.m_rightNode = (JoinNode) m_rightNode.clone();
        } else {
            newNode.m_tableAliasIndex = m_tableAliasIndex;
        }
        return newNode;
    }

    /// For debug purposes:
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        explain_recurse(sb, "");
        return sb.toString();
    }

    public void explain_recurse(StringBuilder sb, String indent) {

        // Node id. Must be unique within a given tree
        sb.append(indent).append("JOIN NODE id: " + m_id).append("\n");

        // Table
        if (m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
            sb.append(indent).append("table alias index: ").append(m_tableAliasIndex).append("\n");
        }

        // Join expression associated with this node
        if (m_joinExpr != null) {
            sb.append(indent).append(m_joinExpr.explain("be explicit")).append("\n");
        }
        // Additional filter expression (WHERE) associated with this node
        if (m_whereExpr != null) {
            sb.append(indent).append(m_whereExpr.explain("be explicit")).append("\n");
        }

        // Join type
        if (m_tableAliasIndex == StmtTableScan.NULL_ALIAS_INDEX) {
            sb.append(indent).append("join type: ").append(m_joinType.name()).append("\n");
        }

        // Buckets for children expression classification
        explain_filter_list(sb, indent, "join outer:", m_joinOuterList);
        explain_filter_list(sb, indent, "join inner:", m_joinInnerList);
        explain_filter_list(sb, indent, "join inner outer:", m_joinInnerOuterList);
        explain_filter_list(sb, indent, "where outer:", m_whereOuterList);
        explain_filter_list(sb, indent, "where inner:", m_whereInnerList);
        explain_filter_list(sb, indent, "where inner outer:", m_whereInnerOuterList);

        String extraIndent = " ";

        if (m_leftNode != null) {
            m_leftNode.explain_recurse(sb, indent + extraIndent);
        }
        if (m_rightNode != null) {
            m_rightNode.explain_recurse(sb, indent + extraIndent);
        }
    }

    private void explain_filter_list(StringBuilder sb, String indent, String label,
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
    HashMap<AbstractExpression, Set<AbstractExpression> > getAllEquivalenceFilters()
    {
        HashMap<AbstractExpression, Set<AbstractExpression> > equivalenceSet =
                new HashMap<AbstractExpression, Set<AbstractExpression> >();
        ArrayDeque<JoinNode> joinNodes = new ArrayDeque<JoinNode>();
        // Iterate over the join nodes to collect their join and where equivalence filter expressions
        joinNodes.add(this);
        while ( ! joinNodes.isEmpty()) {
            JoinNode joinNode = joinNodes.poll();
            if ( ! joinNode.m_whereInnerList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(joinNode.m_whereInnerList,
                                                          equivalenceSet);
            }
            if (joinNode.m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
                assert joinNode.m_leftNode == null && joinNode.m_rightNode == null;
                // HSQL sometimes tags single-table filters in inner joins as join clauses
                // rather than where clauses? OR does analyzeJoinExpressions correct for this?
                // If so, these CAN contain constant equivalences that get used as the basis for equivalence
                // conditions that determine partitioning, so process them as where clauses.
                if ( ! joinNode.m_joinInnerList.isEmpty()) {
                    ExpressionUtil.collectPartitioningFilters(joinNode.m_joinInnerList,
                                                              equivalenceSet);
                }
                continue;
            }
            if ( ! joinNode.m_whereOuterList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(joinNode.m_whereOuterList,
                                                          equivalenceSet);
            }
            if ( ! joinNode.m_whereInnerOuterList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(joinNode.m_whereInnerOuterList,
                                                          equivalenceSet);
            }
            if ( ! joinNode.m_joinInnerOuterList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(joinNode.m_joinInnerOuterList,
                                                          equivalenceSet);
            }
            if (joinNode.m_joinType == JoinType.INNER) {
                // HSQL sometimes tags single-table filters in inner joins as join clauses
                // rather than where clauses? OR does analyzeJoinExpressions correct for this?
                // If so, these CAN contain constant equivalences that get used as the basis for equivalence
                // conditions that determine partitioning, so process them as where clauses.
                if ( ! joinNode.m_joinInnerList.isEmpty()) {
                    ExpressionUtil.collectPartitioningFilters(joinNode.m_joinInnerList,
                                                              equivalenceSet);
                }
                if ( ! joinNode.m_joinOuterList.isEmpty()) {
                    ExpressionUtil.collectPartitioningFilters(joinNode.m_joinOuterList,
                                                              equivalenceSet);
                }
            }
            if (joinNode.m_leftNode != null) {
                joinNodes.add(joinNode.m_leftNode);
            }
            if (joinNode.m_rightNode != null) {
                joinNodes.add(joinNode.m_rightNode);
            }
        }
        return equivalenceSet;
    }

    /**
     * Collect all JOIN and WHERE expressions combined with AND for the entire tree.
     */
    AbstractExpression getAllInnerJoinFilters() {
        assert(m_joinType == JoinType.INNER);
        ArrayDeque<JoinNode> joinNodes = new ArrayDeque<JoinNode>();
        ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
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
            if (joinNode.m_leftNode != null) {
                joinNodes.add(joinNode.m_leftNode);
            }
            if (joinNode.m_rightNode != null) {
                joinNodes.add(joinNode.m_rightNode);
            }
        }

        // this chunk of code breaks the code into a list of expression that
        // all have to be true for the where clause to be true
        AbstractExpression inExpr = null;
        while ((inExpr = in.poll()) != null) {
            if (inExpr.getExpressionType() == ExpressionType.CONJUNCTION_AND) {
                in.add(inExpr.getLeft());
                in.add(inExpr.getRight());
            }
            else {
                out.add(inExpr);
            }
        }
        return ExpressionUtil.combine(out);
    }

    /**
     * Get the WHERE expression for a single-table statement.
     */
    AbstractExpression getSimpleFilterExpression()
    {
        assert(m_leftNode == null);
        assert(m_rightNode == null);
        if (m_whereExpr != null) {
            if (m_joinExpr != null) {
                return ExpressionUtil.combine(m_whereExpr, m_joinExpr);
            }
            return m_whereExpr;
        }
        return m_joinExpr;
    }

    /**
     * Transform all RIGHT joins from the tree into the LEFT ones by swapping the nodes and their join types
     */
    void toLeftJoin() {
        assert((m_leftNode != null && m_rightNode != null) || (m_leftNode == null && m_rightNode == null));
        if (m_leftNode == null && m_rightNode == null) {
            // End of recursion
            return;
        }
        // recursive calls
        m_leftNode.toLeftJoin();
        m_rightNode.toLeftJoin();

        // Swap own children
        if (m_joinType == JoinType.RIGHT) {
            JoinNode node = m_rightNode;
            m_rightNode = m_leftNode;
            m_leftNode = node;
            m_joinType = JoinType.LEFT;
        }
    }

    /**
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    List<JoinNode> generateLeafNodesJoinOrder() {
        ArrayList<JoinNode> leafNodes = new ArrayList<JoinNode>();
        generateLeafNodesJoinOrderRecursive(this, leafNodes);
        return leafNodes;
    }

    private void generateLeafNodesJoinOrderRecursive(JoinNode node, ArrayList<JoinNode> leafNodes) {
        if (node == null) {
            return;
        }
        if (node.m_leftNode != null || node.m_rightNode != null) {
            assert(node.m_leftNode != null && node.m_rightNode != null);
            generateLeafNodesJoinOrderRecursive(node.m_leftNode, leafNodes);
            generateLeafNodesJoinOrderRecursive(node.m_rightNode, leafNodes);
        } else {
            assert(node.m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX);
            leafNodes.add(node);
        }
    }

    /**
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    List<Integer> generateTableJoinOrder() {
        List<JoinNode> leafNodes = generateLeafNodesJoinOrder();
        ArrayList<Integer> tables = new ArrayList<Integer>();
        for (JoinNode node : leafNodes) {
            tables.add(node.m_tableAliasIndex);
        }
        return tables;
    }

    /**
     * Returns nodes in the order they are joined in the tree by iterating the tree depth-first
     */
    List<JoinNode> generateAllNodesJoinOrder() {
        ArrayList<JoinNode> nodes = new ArrayList<JoinNode>();
        generateAllNodesJoinOrderRecursive(this, nodes);
        return nodes;
    }

    private void generateAllNodesJoinOrderRecursive(JoinNode node, ArrayList<JoinNode> nodes) {
        if (node == null) {
            return;
        }
        if (node.m_leftNode != null || node.m_rightNode != null) {
            assert(node.m_leftNode != null && node.m_rightNode != null);
            generateAllNodesJoinOrderRecursive(node.m_leftNode, nodes);
            generateAllNodesJoinOrderRecursive(node.m_rightNode, nodes);
        }
        nodes.add(node);
    }

    /**
     * Returns true if one of the tree nodes has outer join
     */
    boolean hasOuterJoin() {
        if (m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
            return false;
        }
        assert(m_leftNode != null && m_rightNode != null);
        return m_joinType != JoinType.INNER ||
                m_leftNode.hasOuterJoin() || m_rightNode.hasOuterJoin();
    }

    /**
     * Split a join tree into one or more sub-trees. Each sub-tree has the same join type
     * for all join nodes. The root of the child tree in the parent tree is replaced with a 'dummy' node
     * which id is negated id of the child root node.
     * @param tree - The join tree
     * @return the list of sub-trees from the input tree
     */
    public List<JoinNode> extractSubTrees() {
        List<JoinNode> subTrees = new ArrayList<JoinNode>();
        // Extract the first sub-tree starting at the root
        List<JoinNode> leafNodes = new ArrayList<JoinNode>();
        extractSubTree(leafNodes);
        subTrees.add(this);

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
    private void extractSubTree(List<JoinNode> leafNodes) {
        if (m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
            return;
        }
        JoinNode[] children = {m_leftNode, m_rightNode};
        for (JoinNode child : children) {

            // Leaf nodes don't have a significant join type,
            // test for them first and never attempt to start a new tree at a leaf.
            if (child.m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
                continue;
            }

            if (child.m_joinType == m_joinType) {
                // The join type for this node is the same as the root's one
                // Keep walking down the tree
                child.extractSubTree(leafNodes);
            } else {
                // The join type for this join differs from the root's one
                // Terminate the sub-tree
                leafNodes.add(child);
                // Replace the join node with the temporary node having the id negated
                // the tree at the later stage
                // We also need to generate a dummy alias index to preserve JoinNode invariant
                // that a node represent either a table (m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) or
                // a join of two nodes m_leftNode != null && m_rightNode != null
                int tempAliasIdx = StmtTableScan.NULL_ALIAS_INDEX - 1 - child.m_id;
                JoinNode tempNode = new JoinNode(
                        -child.m_id, child.m_joinType, tempAliasIdx, child.m_joinExpr, child.m_whereExpr);
                if (child == m_leftNode) {
                    m_leftNode = tempNode;
                } else {
                    m_rightNode = tempNode;
                }
            }
        }
    }

    /**
     * Reconstruct a join tree from the list of tables always appending the next node to the right.
     *
     * @param tableNodes the list of tables to build the tree from.
     * @return The reconstructed tree
     */
    public static JoinNode reconstructJoinTreeFromTableNodes(List<JoinNode> tableNodes) {
        JoinNode root = null;
        for (JoinNode leafNode : tableNodes) {
            assert(leafNode.m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX);
            JoinNode node = new JoinNode(leafNode.m_id, leafNode.m_joinType, leafNode.m_tableAliasIndex, null, null);
            if (root == null) {
                root = node;
            } else {
                // We only care about the root node id to be able to reconnect the sub-trees
                // The intermediate node id can be anything. For the final root node its id
                // will be set later to the original tree's root id
                root = new JoinNode(-node.m_id, JoinType.INNER, root, node);
            }
        }
        return root;
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
            boolean replaced = replaceChild(joinNode, nextNode);
            // There must be a node in the current tree to be replaced
            assert(replaced == true);
        }
        return joinNode;
    }

    private static boolean replaceChild(JoinNode root, JoinNode node) {
        // can't replace self
        assert (root != null && Math.abs(root.m_id) != Math.abs(node.m_id));
        if (root.m_tableAliasIndex != StmtTableScan.NULL_ALIAS_INDEX) {
            return false;
        } else if (Math.abs(root.m_leftNode.m_id) == Math.abs(node.m_id)) {
            root.m_leftNode  = node;
            return true;
        } else if (Math.abs(root.m_rightNode.m_id) == Math.abs(node.m_id)) {
            root.m_rightNode  = node;
            return true;
        } else if (replaceChild(root.m_leftNode, node) == true) {
            return true;
        } else if (replaceChild(root.m_rightNode, node) == true) {
            return true;
        }

        return false;
    }


}
