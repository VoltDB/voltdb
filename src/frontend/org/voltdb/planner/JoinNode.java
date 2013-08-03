/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.util.List;

import org.voltdb.catalog.Table;
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
    // Table
    public Table m_table = null;
    // Join type
    public JoinType m_joinType = JoinType.INNER;
    // Join expression associated with this node
    public AbstractExpression m_joinExpr = null;
    // Additional filter expression (WHERE) associated with this node
    public AbstractExpression m_whereExpr = null;
    // Node id. Must be unique within a given tree
    public int m_id = 0;

    // Buckets for children expression classification
    public ArrayList<AbstractExpression> m_joinOuterList = new ArrayList<AbstractExpression>();
    public ArrayList<AbstractExpression> m_joinInnerList = new ArrayList<AbstractExpression>();
    public ArrayList<AbstractExpression> m_joinInnerOuterList = new ArrayList<AbstractExpression>();
    public ArrayList<AbstractExpression> m_whereOuterList = new ArrayList<AbstractExpression>();
    public ArrayList<AbstractExpression> m_whereInnerList = new ArrayList<AbstractExpression>();
    public ArrayList<AbstractExpression> m_whereInnerOuterList = new ArrayList<AbstractExpression>();

    // All possible access paths for this node
    List<AccessPath> m_accessPaths = new ArrayList<AccessPath>();
    // Access path under the evaluation
    AccessPath m_currentAccessPath = null;

    /**
     * Construct a leaf node
     * @param id - node unique id
     * @param joinType - join type
     * @param table - join table
     * @param joinExpr - join expression
     * @param whereExpr - filter expression
     * @param id - node id
     */
    JoinNode(int id, JoinType joinType, Table table, AbstractExpression joinExpr, AbstractExpression  whereExpr) {
        this(id, joinType);
        m_table = table;
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
        if (m_table == null) {
            assert(m_leftNode != null && m_rightNode != null);
            newNode.m_leftNode = (JoinNode) m_leftNode.clone();
            newNode.m_rightNode = (JoinNode) m_rightNode.clone();
        } else {
            newNode.m_table = m_table;
        }
        return newNode;
    }

    /**
     * Collect all JOIN and WHERE expressions combined with AND for the entire tree.
     */
    Collection<AbstractExpression> getAllExpressions() {
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
        return out;
    }

    /**
     * Collect all JOIN and WHERE expressions for the entire statement and combine them into
     * a single expression joined by AND.
     */
    AbstractExpression getCombinedExpression() {
        // First, collect/decompose the original expressions into the subexpressions joined by AND
        Collection<AbstractExpression> exprList = getAllExpressions();
        // Eliminate dups
        return ExpressionUtil.eliminateDuplicates(exprList);
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
            assert(node.m_table != null);
            leafNodes.add(node);
        }
    }

    /**
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    List<Table> generateTableJoinOrder() {
        List<JoinNode> leafNodes = generateLeafNodesJoinOrder();
        ArrayList<Table> tables = new ArrayList<Table>();
        for (JoinNode node : leafNodes) {
            tables.add(node.m_table);
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
        if (m_table != null) {
            return false;
        }
        assert(m_leftNode != null && m_rightNode != null);
        return m_joinType != JoinType.INNER ||
                m_leftNode.hasOuterJoin() || m_rightNode.hasOuterJoin();
    }

}
