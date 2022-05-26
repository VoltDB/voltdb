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
import java.util.function.Predicate;

import org.voltdb.expressions.*;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.StmtEphemeralTableScan;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.types.JoinType;

/**
 * A BranchNode is an interior node of a join tree.
 */
public class BranchNode extends JoinNode {
    // Join type
    private JoinType m_joinType;
    // Left child
    private JoinNode m_leftNode;
    // Right child
    private JoinNode m_rightNode;
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
        updateContentDeterminismMessage(leftNode.getContentDeterminismMessage());
        updateContentDeterminismMessage(rightNode.getContentDeterminismMessage());
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
            newNode.m_joinExpr = m_joinExpr.clone();
        }
        if (m_whereExpr != null) {
            newNode.m_whereExpr = m_whereExpr.clone();
        }
        return newNode;
    }

    public boolean hasChild(Predicate<JoinNode> pred) {
        return pred.test(getLeftNode()) || pred.test(getRightNode());
    }

    public boolean allChildren(Predicate<JoinNode> pred) {
        return pred.test(getLeftNode()) && pred.test(getRightNode());
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
    public String getTableAlias() {
        return null;
    }

    private static Set<TupleValueExpression> collectTVEs(AbstractExpression expr, Set<TupleValueExpression> acc) {
        if (expr != null) {
            if (expr instanceof TupleValueExpression) {
                acc.add((TupleValueExpression) expr);
            } else if (expr instanceof ComparisonExpression || expr instanceof ConjunctionExpression) {
                collectTVEs(expr.getLeft(), acc);
                collectTVEs(expr.getRight(), acc);
            } else if (expr instanceof FunctionExpression) {
                expr.getArgs().forEach(e -> collectTVEs(e, acc));
            }
        }
        return acc;
    }

    private static boolean validWhere(AbstractExpression where, Set<String> rels) {
        return collectTVEs(where, new HashSet<>())
                .stream()
                .allMatch(tve -> rels.contains(tve.getTableName()) || rels.contains(tve.getTableAlias()));
    }

    @Override
    public void analyzeJoinExpressions(AbstractParsedStmt stmt) {
        JoinNode leftChild = getLeftNode();
        JoinNode rightChild = getRightNode();
        leftChild.analyzeJoinExpressions(stmt);
        rightChild.analyzeJoinExpressions(stmt);

        // At this moment all RIGHT joins are already converted to the LEFT ones
        assert (getJoinType() != JoinType.RIGHT);

        // Collect node's own join and where expressions
        List<AbstractExpression> joinList = new ArrayList<>(ExpressionUtil.uncombineAny(getJoinExpression()));
        List<AbstractExpression> whereList = new ArrayList<>(ExpressionUtil.uncombineAny(getWhereExpression()));

        // Collect children expressions only if a child is a leaf. They are not classified yet
        if (! (leftChild instanceof BranchNode)) {
            joinList.addAll(leftChild.m_joinInnerList);
            leftChild.m_joinInnerList.clear();
            whereList.addAll(leftChild.m_whereInnerList);
            leftChild.m_whereInnerList.clear();
        }
        if (! (rightChild instanceof BranchNode)) {
            joinList.addAll(rightChild.m_joinInnerList);
            rightChild.m_joinInnerList.clear();
            whereList.addAll(rightChild.m_whereInnerList);
            rightChild.m_whereInnerList.clear();
        }

        final Collection<String> outerTables = leftChild.generateTableJoinOrder();
        final Collection<String> innerTables = rightChild.generateTableJoinOrder();
        if (! whereList.isEmpty()) {        // validate that all TVEs in WHERE clause have corresponding tables from outer or inner relations.
            final Set<String> rels = new HashSet<String>() {{
                    addAll(outerTables);
                    addAll(innerTables);
                }};
            if (! whereList.stream().allMatch(expr -> validWhere(expr, rels))) {
                throw new SubPlanAssembler.SkipCurrentPlanException();
            }
        }

        // Classify join expressions into the following categories:
        // 1. The OUTER-only join conditions. If any are false for a given outer tuple,
        // then NO inner tuples should match it (and it can automatically get null-padded by the join
        // without even considering the inner table). Testing the outer-only conditions
        // COULD be considered as an optimal first step to processing each outer tuple
        // 2. The INNER-only join conditions apply to the inner tuples (even prior to considering any outer tuple).
        // if true for a given inner tuple, the condition has no effect,
        // if false, it prevents the inner tuple from matching ANY outer tuple,
        // In case of multi-tables join, they could be pushed down to a child node if this node is a join itself
        // 3. The two-sided expressions that get evaluated on each combination of outer and inner tuple
        // and either accept or reject that particular combination.
        // 4. The TVE expressions where neither inner nor outer tables are involved. This is not possible
        // for the currently supported two table joins but could change if number of tables > 2.
        // When that occurs, the call throws SkipCurrentPlanException, signaling its caller to continue with
        // next possible plan.
        // Constant Value Expression may fall into this category.
        classifyJoinExpressions(joinList, outerTables, innerTables,  m_joinOuterList,
                m_joinInnerList, m_joinInnerOuterList, stmt.m_noTableSelectionList);

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
                m_whereInnerList, m_whereInnerOuterList, stmt.m_noTableSelectionList);

        // Apply implied transitive constant filter to where expressions
        applyTransitiveEquivalence(m_whereOuterList, m_whereInnerList, m_whereInnerOuterList);

        // In case of multi-table joins certain expressions could be pushed down to the children
        // to improve join performance.
        pushDownExpressions(stmt.m_noTableSelectionList);

        Iterator<AbstractExpression> iter = stmt.m_noTableSelectionList.iterator();
        while (iter.hasNext()) {
            AbstractExpression noneExpr = iter.next();
            // Allow only CVE(TRUE/FALSE) for now.
            // Though it does seem strange to be adding a constant TRUE or FALSE
            // to a list of conjunctions rather than replacing it with an empty
            // list or a single FALSE element.
            // TODO: there may be other use cases that can be handled the same way
            // as CVEs like predicates based on non-correlated subqueries or predicates
            // based on correlation parameters from parent queries. These would require
            // additional testing to be enabled here.
            // TODO: it seems like there are at least some cases that would perform
            // better with these predicates pushed down to the inner child node.
            if (noneExpr instanceof ConstantValueExpression) {
                m_whereInnerOuterList.add(noneExpr);
                iter.remove();
            }
        }
    }

    /**
     * Push down each WHERE expression on a given join node to the most specific child join
     * or table the expression applies to.
     *  1. The OUTER WHERE expressions can be pushed down to the outer (left) child for all joins
     *    (INNER and LEFT).
     *  2. The INNER WHERE expressions can be pushed down to the inner (right) child for the INNER joins.
     *  3. The WHERE expressions must be preserved for the FULL join type.
     * @param joinNode JoinNode
     */
    protected void pushDownExpressions(List<AbstractExpression> noneList) {
        JoinType joinType = getJoinType();
        if (joinType == JoinType.FULL) {
            return;
        }
        JoinNode outerNode = getLeftNode();
        if (outerNode instanceof BranchNode) {
            ((BranchNode)outerNode).pushDownExpressionsRecursively(m_whereOuterList, noneList);
        }
        JoinNode innerNode = getRightNode();
        if (innerNode instanceof BranchNode && joinType == JoinType.INNER) {
            ((BranchNode)innerNode).pushDownExpressionsRecursively(m_whereInnerList, noneList);
        }
    }

    private void pushDownExpressionsRecursively(List<AbstractExpression> pushDownExprList,
            List<AbstractExpression> noneList) {
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
        sb.append(indent).append("JOIN NODE id: ").append(m_id).append("\n");

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
            Map<AbstractExpression, Set<AbstractExpression>> equivalenceSet,
            Deque<JoinNode> joinNodes) {
        //* enable to debug */ System.out.println("DEBUG: Branch cEF in  " + this + " nodes:" + joinNodes.size() + " filters:" + equivalenceSet.size());
        if ( ! m_whereInnerList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerList, equivalenceSet);
        }
        if ( ! m_whereOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereOuterList, equivalenceSet);
        }
        if ( ! m_whereInnerOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_whereInnerOuterList, equivalenceSet);
        }
        if ( ! m_joinInnerOuterList.isEmpty()) {
            ExpressionUtil.collectPartitioningFilters(m_joinInnerOuterList, equivalenceSet);
        }
        // One-sided join criteria can not be used to infer single partitioining for a
        // non-inner query. In general, they do not prevent results from being generated
        // on the partitions that don't have partition-key-qualified rows.
        if (m_joinType == JoinType.INNER) {
            if (! m_joinInnerList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(m_joinInnerList, equivalenceSet);
            }
            if (! m_joinOuterList.isEmpty()) {
                ExpressionUtil.collectPartitioningFilters(m_joinOuterList, equivalenceSet);
            }
        }

        if (m_leftNode != null) {
            joinNodes.add(m_leftNode);
        }
        if (m_rightNode != null) {
            joinNodes.add(m_rightNode);
        }
        //* enable to debug */ System.out.println("DEBUG: Branch cEF out " + this + " nodes:" + joinNodes.size() + " filters:" + equivalenceSet.size());
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
        for (JoinNode child : new JoinNode[]{m_leftNode, m_rightNode}) {

            // Leaf nodes don't have a significant join type,
            // test for them first and never attempt to start a new tree at a leaf.
            if (child instanceof BranchNode) {
                if (((BranchNode) child).m_joinType == m_joinType) {
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
    }

    /**
     * Returns true if one of the tree nodes has outer join
     */
    @Override
    public boolean hasOuterJoin() {
        assert(m_leftNode != null && m_rightNode != null);
        return m_joinType != JoinType.INNER || m_leftNode.hasOuterJoin() || m_rightNode.hasOuterJoin();
    }

    /**
     * Returns a list of immediate sub-queries which are part of this query.
     * @return List<AbstractParsedStmt> - list of sub-queries from this query
     */
    @Override
    public void extractEphemeralTableQueries(List<StmtEphemeralTableScan> scans) {
        if (m_leftNode != null) {
            m_leftNode.extractEphemeralTableQueries(scans);
        }
        if (m_rightNode != null) {
            m_rightNode.extractEphemeralTableQueries(scans);
        }
    }

    @Override
    public boolean hasSubqueryScans() {
        if (m_leftNode != null && m_leftNode.hasSubqueryScans()) {
            return true;
        } else {
            return m_rightNode != null && m_rightNode.hasSubqueryScans();
        }
    }

    @Override
    protected void listNodesJoinOrderRecursive(List<JoinNode> nodes, boolean appendBranchNodes) {
        m_leftNode.listNodesJoinOrderRecursive(nodes, appendBranchNodes);
        m_rightNode.listNodesJoinOrderRecursive(nodes, appendBranchNodes);
        if (appendBranchNodes) {
            nodes.add(this);
        }
    }

    @Override
    protected void queueChildren(Deque<JoinNode> joinNodes) {
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
        } else if (Math.abs(m_rightNode.m_id) == Math.abs(node.m_id)) {
            m_rightNode = node;
            return true;
        } else if (m_leftNode.replaceChild(node)) {
            return true;
        } else {
            return m_rightNode.replaceChild(node);
        }
    }

    /**
     * Returns if all the join operations within this join tree are inner joins.
     * @return true or false.
     */
    @Override
    public boolean allInnerJoins() {
        return m_joinType == JoinType.INNER &&
               (m_leftNode == null || m_leftNode.allInnerJoins()) &&
               (m_rightNode == null || m_rightNode.allInnerJoins());
    }

    @Override
    public void gatherJoinExpressions(List<AbstractExpression> checkExpressions) {
        super.gatherJoinExpressions(checkExpressions);
        m_leftNode.gatherJoinExpressions(checkExpressions);
        m_rightNode.gatherJoinExpressions(checkExpressions);
    }

}
