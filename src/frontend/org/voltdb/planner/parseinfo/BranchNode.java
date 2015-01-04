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

package org.voltdb.planner.parseinfo;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.types.JoinType;

public class BranchNode extends JoinNode {
    // Join type
    private JoinType m_joinType;
    // Left child
    private JoinNode m_leftNode = null;
    // Right child
    private JoinNode m_rightNode = null;
    // index into the query catalog cache for a table alias

    /**
     * Construct a join node
     * @param id - node unique id
     * @param joinType - join type
     * @param leftNode - left node
     * @param rightNode - right node
     */
    public BranchNode(int id, JoinType joinType, JoinNode leftNode, JoinNode rightNode) {
        super(id);
        m_joinType = joinType;
        m_leftNode = leftNode;
        m_rightNode = rightNode;
    }

    /**
     * Deep clone
     */
    @Override
    public Object clone() {
        assert(m_leftNode != null && m_rightNode != null);
        JoinNode leftNode = (JoinNode) m_leftNode.clone();
        JoinNode rightNode = (JoinNode) m_rightNode.clone();

        BranchNode newNode = new BranchNode(m_id, m_joinType, leftNode, rightNode);

        if (m_joinExpr != null) {
            newNode.m_joinExpr = (AbstractExpression) m_joinExpr.clone();
        }
        if (m_whereExpr != null) {
            newNode.m_whereExpr = (AbstractExpression) m_whereExpr.clone();
        }
        return newNode;
    }

    public void setJoinType(JoinType joinType) {
        m_joinType = joinType;
    }

    public JoinType getJoinType() {
        return m_joinType;
    }

    @Override
    public JoinNode getLeftNode() {
        assert(m_leftNode != null);
        return m_leftNode;
    }

    @Override
    public JoinNode getRightNode() {
        assert(m_rightNode != null);
        return m_rightNode;
    }

    @Override
    public void analyzeJoinExpressions(List<AbstractExpression> noneList) {
        getLeftNode().analyzeJoinExpressions(noneList);
        getRightNode().analyzeJoinExpressions(noneList);

        // At this moment all RIGHT joins are already converted to the LEFT ones
        assert (getJoinType() == JoinType.LEFT || getJoinType() == JoinType.INNER);

        ArrayList<AbstractExpression> joinList = new ArrayList<AbstractExpression>();
        ArrayList<AbstractExpression> whereList = new ArrayList<AbstractExpression>();

        // Collect node's own join and where expressions
        joinList.addAll(ExpressionUtil.uncombineAny(getJoinExpression()));
        whereList.addAll(ExpressionUtil.uncombineAny(getWhereExpression()));

        // Collect children expressions only if a child is a leaf. They are not classified yet
        JoinNode leftChild = getLeftNode();
        if ( ! (leftChild instanceof BranchNode)) {
            joinList.addAll(leftChild.m_joinInnerList);
            leftChild.m_joinInnerList.clear();
            whereList.addAll(leftChild.m_whereInnerList);
            leftChild.m_whereInnerList.clear();
        }
        JoinNode rightChild = getRightNode();
        if ( ! (rightChild instanceof BranchNode)) {
            joinList.addAll(rightChild.m_joinInnerList);
            rightChild.m_joinInnerList.clear();
            whereList.addAll(rightChild.m_whereInnerList);
            rightChild.m_whereInnerList.clear();
        }

        Collection<String> outerTables = leftChild.generateTableJoinOrder();
        Collection<String> innerTables = rightChild.generateTableJoinOrder();

        // Classify join expressions into the following categories:
        // 1. The OUTER-only join conditions. If any are false for a given outer tuple,
        // then NO inner tuples should match it (and it can automatically get null-padded by the join
        // without even considering the inner table). Testing the outer-only conditions
        // COULD be considered as an optimal first step to processing each outer tuple
        // 2. The INNER-only join conditions apply to the inner tuples (even prior to considering any outer tuple).
        // if true for a given inner tuple, the condition has no effect, if false,
        // it prevents the inner tuple from matching ANY outer tuple,
        // In case of multi-tables join, they could be pushed down to a child node if this node is a join itself
        // 3. The two-sided expressions that get evaluated on each combination of outer and inner tuple
        // and either accept or reject that particular combination.
        // 4. The TVE expressions where neither inner nor outer tables are involved. This is not possible
        // for the currently supported two table joins but could change if number of tables > 2
        classifyJoinExpressions(joinList, outerTables, innerTables,  m_joinOuterList,
                m_joinInnerList, m_joinInnerOuterList, noneList);

        // Apply implied transitive constant filter to join expressions
        // outer.partkey = ? and outer.partkey = inner.partkey is equivalent to
        // outer.partkey = ? and inner.partkey = ?
        applyTransitiveEquivalence(m_joinOuterList, m_joinInnerList, m_joinInnerOuterList);

        // Classify where expressions into the following categories:
        // 1. The OUTER-only filter conditions. If any are false for a given outer tuple,
        // nothing in the join processing of that outer tuple will get it past this filter,
        // so it makes sense to "push this filter down" to pre-qualify the outer tuples before they enter the join.
        // 2. The INNER-only join conditions. If these conditions reject NULL inner tuple it make sense to
        // move them "up" to the join conditions, otherwise they must remain post-join conditions
        // to preserve outer join semantic
        // 3. The two-sided expressions. Same as the inner only conditions.
        // 4. The TVE expressions where neither inner nor outer tables are involved. Same as for the join expressions
        classifyJoinExpressions(whereList, outerTables, innerTables,  m_whereOuterList,
                m_whereInnerList, m_whereInnerOuterList, noneList);

        // Apply implied transitive constant filter to where expressions
        applyTransitiveEquivalence(m_whereOuterList, m_whereInnerList, m_whereInnerOuterList);

        // In case of multi-table joins certain expressions could be pushed down to the children
        // to improve join performance.
        pushDownExpressions(noneList);
    }

    /**
     * Push down each WHERE expression on a given join node to the most specific child join
     * or table the expression applies to.
     *  1. The OUTER WHERE expressions can be pushed down to the outer (left) child for all joins
     *    (INNER and LEFT).
     *  2. The INNER WHERE expressions can be pushed down to the inner (right) child for the INNER joins.
     * @param joinNode JoinNode
     */
    protected void pushDownExpressions(List<AbstractExpression> noneList)
    {
        JoinNode outerNode = getLeftNode();
        if (outerNode instanceof BranchNode) {
            ((BranchNode)outerNode).pushDownExpressionsRecursively(m_whereOuterList, noneList);
        }
        JoinNode innerNode = getRightNode();
        if (innerNode instanceof BranchNode && getJoinType() == JoinType.INNER) {
            ((BranchNode)innerNode).pushDownExpressionsRecursively(m_whereInnerList, noneList);
        }
    }

    private void pushDownExpressionsRecursively(List<AbstractExpression> pushDownExprList,
            List<AbstractExpression> noneList)
    {
        // It is a join node. Classify pushed down expressions as inner, outer, or inner-outer
        // WHERE expressions.
        Collection<String> outerTables = getLeftNode().generateTableJoinOrder();
        Collection<String> innerTables = getRightNode().generateTableJoinOrder();
        classifyJoinExpressions(pushDownExprList, outerTables, innerTables,
                m_whereOuterList, m_whereInnerList, m_whereInnerOuterList, noneList);
        // Remove them from the original list
        pushDownExprList.clear();
        // Descend to the inner child
        pushDownExpressions(noneList);
    }

    @Override
    public void explain_recurse(StringBuilder sb, String indent) {

        // Node id. Must be unique within a given tree
        sb.append(indent).append("JOIN NODE id: " + m_id).append("\n");

        // Join expression associated with this node
        if (m_joinExpr != null) {
            sb.append(indent).append(m_joinExpr.explain("be explicit")).append("\n");
        }
        // Additional filter expression (WHERE) associated with this node
        if (m_whereExpr != null) {
            sb.append(indent).append(m_whereExpr.explain("be explicit")).append("\n");
        }

        // Join type
        sb.append(indent).append("join type: ").append(m_joinType.name()).append("\n");

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

    @Override
    protected void collectEquivalenceFilters(
            HashMap<AbstractExpression, Set<AbstractExpression>> equivalenceSet,
            ArrayDeque<JoinNode> joinNodes) {
        if ( ! m_whereInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerList,
                                                      equivalenceSet);
        }
        if ( ! m_whereOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereOuterList,
                                                      equivalenceSet);
        }
        if ( ! m_whereInnerOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerOuterList,
                                                      equivalenceSet);
        }
        if ( ! m_joinInnerOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_joinInnerOuterList,
                                                      equivalenceSet);
        }
        if (m_joinType == JoinType.INNER) {
            // HSQL sometimes tags single-table filters in inner joins as join clauses
            // rather than where clauses? OR does analyzeJoinExpressions correct for this?
            // If so, these CAN contain constant equivalences that get used as the basis for equivalence
            // conditions that determine partitioning, so process them as where clauses.
            if ( ! m_joinInnerList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(m_joinInnerList,
                                                          equivalenceSet);
            }
            if ( ! m_joinOuterList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(m_joinOuterList,
                                                          equivalenceSet);
            }
        }
        if (m_leftNode != null) {
            joinNodes.add(m_leftNode);
        }
        if (m_rightNode != null) {
            joinNodes.add(m_rightNode);
        }
    }

    /**
     * Transform all RIGHT joins from the tree into the LEFT ones by swapping the nodes and their join types
     */
    public void toLeftJoin() {
        assert((m_leftNode != null && m_rightNode != null) || (m_leftNode == null && m_rightNode == null));
        if (m_leftNode == null && m_rightNode == null) {
            // End of recursion
            return;
        }
        // recursive calls
        if (m_leftNode instanceof BranchNode) {
            ((BranchNode)m_leftNode).toLeftJoin();
        }
        if (m_rightNode instanceof BranchNode) {
            ((BranchNode)m_rightNode).toLeftJoin();
        }

        // Swap own children
        if (m_joinType == JoinType.RIGHT) {
            JoinNode node = m_rightNode;
            m_rightNode = m_leftNode;
            m_leftNode = node;
            m_joinType = JoinType.LEFT;
        }
    }

    /**
     * Starting from the root recurse to its children stopping at the first join node
     * of the different type and discontinue the tree at this point by replacing the join node with
     * the temporary node which id matches the join node id. This join node is the root of the next
     * sub-tree.
     * @param root - The root of the join tree
     * @param leafNodes - the list of the root nodes of the next sub-trees
     */
    @Override
    protected void extractSubTree(List<JoinNode> leafNodes) {
        JoinNode[] children = {m_leftNode, m_rightNode};
        for (JoinNode child : children) {

            // Leaf nodes don't have a significant join type,
            // test for them first and never attempt to start a new tree at a leaf.
            if ( ! (child instanceof BranchNode)) {
                continue;
            }

            if (((BranchNode)child).m_joinType == m_joinType) {
                // The join type for this node is the same as the root's one
                // Keep walking down the tree
                child.extractSubTree(leafNodes);
            } else {
                // The join type for this join differs from the root's one
                // Terminate the sub-tree
                leafNodes.add(child);
                // Replace the join node with the temporary node having the id negated
                JoinNode tempNode = new TableLeafNode(
                        -child.m_id, child.m_joinExpr, child.m_whereExpr, null);
                if (child == m_leftNode) {
                    m_leftNode = tempNode;
                } else {
                    m_rightNode = tempNode;
                }
            }
        }
    }

    /**
     * Returns true if one of the tree nodes has outer join
     */
    @Override
    public boolean hasOuterJoin() {
        assert(m_leftNode != null && m_rightNode != null);
        return m_joinType != JoinType.INNER ||
                m_leftNode.hasOuterJoin() || m_rightNode.hasOuterJoin();
    }

    /**
     * Returns a list of immediate sub-queries which are part of this query.
     * @return List<AbstractParsedStmt> - list of sub-queries from this query
     */
    @Override
    public void extractSubQueries(List<StmtSubqueryScan> subQueries) {
        if (m_leftNode != null) {
            m_leftNode.extractSubQueries(subQueries);
        }
        if (m_rightNode != null) {
            m_rightNode.extractSubQueries(subQueries);
        }
    }

    @Override
    protected void listNodesJoinOrderRecursive(ArrayList<JoinNode> nodes, boolean appendBranchNodes) {
        m_leftNode.listNodesJoinOrderRecursive(nodes, appendBranchNodes);
        m_rightNode.listNodesJoinOrderRecursive(nodes, appendBranchNodes);
        if (appendBranchNodes) {
            nodes.add(this);
        }
    }

    @Override
    protected void queueChildren(ArrayDeque<JoinNode> joinNodes) {
        joinNodes.add(m_leftNode);
        joinNodes.add(m_rightNode);
    }

    @Override
    protected boolean replaceChild(JoinNode node) {
        // can't replace self
        assert (Math.abs(m_id) != Math.abs(node.m_id));
        if (Math.abs(m_leftNode.m_id) == Math.abs(node.m_id)) {
            m_leftNode = node;
            return true;
        }
        if (Math.abs(m_rightNode.m_id) == Math.abs(node.m_id)) {
            m_rightNode = node;
            return true;
        }
        if (m_leftNode.replaceChild(node)) {
            return true;
        }
        if (m_rightNode.replaceChild(node)) {
            return true;
        }
        return false;
    }
}
