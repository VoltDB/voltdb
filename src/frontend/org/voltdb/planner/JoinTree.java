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
import java.util.HashMap;
import java.util.List;

import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

/**
 * JoinTree class captures the hierarchical data model of a given SQl join.
 *
 */
public class JoinTree {

    public static class TablePair {
        public Table t1;
        public Table t2;

        @Override
        public boolean equals(Object obj) {
            if ((obj instanceof TablePair) == false)
                return false;
            TablePair tp = (TablePair)obj;

            return (((t1 == tp.t1) && (t2 == tp.t2)) ||
                    ((t1 == tp.t2) && (t2 == tp.t1)));
        }

        @Override
        public int hashCode() {
            assert((t1.hashCode() ^ t2.hashCode()) == (t2.hashCode() ^ t1.hashCode()));

            return t1.hashCode() ^ t2.hashCode();
        }
    }
    /**
     * Join tree node. The node can be either a leaf representing a joined table (m_table != null)
     * or a node representing the actual join (m_leftNode!=0 and m_rightNode!=0)
     * filter expressions
     *
     */
    public static class JoinNode {

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
        // True if either all children are replicated or table is replicated. False otherwise
        boolean m_isReplicated = false;

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
         * @param table - join table
         * @param joinType - join type
         * @param joinExpr - join expression
         * @param whereExpr - filter expression
         */
        JoinNode(Table table, JoinType joinType, AbstractExpression joinExpr, AbstractExpression  whereExpr) {
            m_table = table;
            m_joinType = joinType;
            m_joinExpr = joinExpr;
            m_whereExpr = whereExpr;
        }

        /**
         * Construct an empty join node
         */
        JoinNode() {
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
            if (m_rightNode.m_joinType == JoinType.RIGHT) {
                assert(m_leftNode.m_joinType == JoinType.INNER);
                JoinNode node = m_rightNode;
                m_rightNode = m_leftNode;
                m_leftNode = node;
                m_rightNode.m_joinType = JoinType.LEFT;
                m_leftNode.m_joinType = JoinType.INNER;
            }
        }

        /**
         * Returns true if table belongs to the right sub-tree
         */
        boolean isInnerTable(String tableName) {
            return checkTable(tableName, true);
        }

        /**
         * Returns true if table belongs to the left sub-tree
         */
        boolean isOuterTable(String tableName) {
            return checkTable(tableName, false);
        }

        private boolean checkTable(String tableName, boolean checkRight) {
            if (m_table != null) {
                return false;
            }
            assert(m_rightNode != null && m_leftNode != null);
            List<Table> innerTables = (checkRight == true) ?
                    m_rightNode.generateTableJoinOrder() : m_leftNode.generateTableJoinOrder();
            for (Table table : innerTables) {
                if (table.getTypeName().equals(tableName)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns tables in the order they are joined in the tree by iterating the tree depth-first
         */
        List<Table> generateTableJoinOrder() {
            ArrayList<Table> tables = new ArrayList<Table>();
            generateTableJoinOrderRecursive(this, tables);
            return tables;
        }

        private void generateTableJoinOrderRecursive(JoinNode node, ArrayList<Table> tables) {
            if (node == null) {
                return;
            }
            if (node.m_leftNode != null || node.m_rightNode != null) {
                assert(node.m_leftNode != null && node.m_rightNode != null);
                generateTableJoinOrderRecursive(node.m_leftNode, tables);
                generateTableJoinOrderRecursive(node.m_rightNode, tables);
            } else {
                assert(node.m_table != null);
                tables.add(node.m_table);
            }
        }

        /**
         * Returns nodes in the order they are joined in the tree by iterating the tree depth-first
         */
        List<JoinNode> generateJoinOrder() {
            ArrayList<JoinNode> nodes = new ArrayList<JoinNode>();
            generateJoinOrderRecursive(this, nodes);
            return nodes;
        }

        private void generateJoinOrderRecursive(JoinNode node, ArrayList<JoinNode> nodes) {
            if (node == null) {
                return;
            }
            if (node.m_leftNode != null || node.m_rightNode != null) {
                assert(node.m_leftNode != null && node.m_rightNode != null);
                generateJoinOrderRecursive(node.m_leftNode, nodes);
                generateJoinOrderRecursive(node.m_rightNode, nodes);
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
            return m_leftNode.m_joinType != JoinType.INNER ||
                    m_rightNode.m_joinType != JoinType.INNER ||
                    m_leftNode.hasOuterJoin() || m_rightNode.hasOuterJoin();
        }

    }

    // The tree root
    public JoinNode m_root = null;
    // False if all joins in the tree are INNER
    public boolean m_hasOuterJoin = false;
    // For all inner joins only. Aggregated set of the single table expressions across the entire join
    public HashMap<Table, ArrayList<AbstractExpression>> m_tableFilterList = new HashMap<Table, ArrayList<AbstractExpression>>();
    // For all inner joins only. Aggregated set of the two table expressions across the entire join
    public HashMap<TablePair, ArrayList<AbstractExpression>> m_joinSelectionList = new HashMap<TablePair, ArrayList<AbstractExpression>>();
    // For all inner joins only. The conditions need to be analyzed only once for all possible join orders.
    public boolean m_wasAnalyzed = false;
    // For all inner joins only. Flat list of tables to represent a join order
    public Table[] m_joinOrder = null;

    /**
     * Construct an empty join node
     */
    JoinTree() {
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
     * Collect all JOIN and WHERE expressions combined with AND for the entire statement.
     */
    Collection<AbstractExpression> getAllExpressions() {
        ArrayDeque<JoinNode> joinNodes = new ArrayDeque<JoinNode>();
        ArrayDeque<AbstractExpression> in = new ArrayDeque<AbstractExpression>();
        ArrayDeque<AbstractExpression> out = new ArrayDeque<AbstractExpression>();
        // Iterate over the join nodes to collect their join and where expressions
        if (m_root != null) {
            joinNodes.add(m_root);
        }
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
     * Returns tables in the order they are joined in the tree by iterating the tree depth-first
     */
    List<Table> generateJoinOrder() {
        if (m_root != null) {
            return m_root.generateTableJoinOrder();
        }
        return new ArrayList<Table>();
    }

    /**
     * Sets isReplicated flag for all nodes in the tree
     */
    public void setReplicatedFlag() {
        if (m_root != null) {
            setReplicatedFlag(m_root);
        }
    }

    /**
     * Sets isReplicated flag for all nodes in the tree recursively
     */
    private void setReplicatedFlag(JoinNode joinNode) {
        if (joinNode.m_table != null) {
            joinNode.m_isReplicated = joinNode.m_table.getIsreplicated();
        } else {
            assert (joinNode.m_rightNode != null &  joinNode.m_leftNode != null);
            setReplicatedFlag(joinNode.m_leftNode);
            setReplicatedFlag(joinNode.m_rightNode);
            joinNode.m_isReplicated = joinNode.m_rightNode.m_isReplicated && joinNode.m_leftNode.m_isReplicated;
        }
    }

}
