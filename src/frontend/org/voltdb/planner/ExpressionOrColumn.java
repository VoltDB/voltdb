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
package org.voltdb.planner;

import java.util.Collections;
import java.util.List;

import com.google_voltpatches.common.base.Preconditions;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.types.SortDirectionType;
import org.voltdb.planner.parseinfo.StmtTableScan;

/*
 * Try to use the index scan's inherent ordering to implement some ORDER BY
 * or WINDOW FUNCTION clauses.
 *
 * These clauses can be statement level ORDER BY clauses or else
 * window function PARTITION BY and ORDER BY specifications.
 * For example, if a table has a tree index on columns "(A, B, C)", then
 * "ORDER BY A", "ORDER BY A, B" and "ORDER BY A, B, C" are considered a match
 * but NOT "ORDER BY A, C", "ORDER BY A, D", "ORDER BY A, B, C, D",
 * "ORDER BY B" or "ORDER BY B, A".
 *
 * Similarly, these window functions would match:
 * <ul>
 *   <li>"MIN(E) OVER (PARTITION BY A, B ORDER BY C)"</li>
 *   <li>"MIN(E) OVER (PARTITION BY A ORDER BY B)"</br>
 *       The order expressions don't need to use all the
 *       index expressions.
 *   </li>
 *   <li>"MIN(E) OVER (PARTITION BY B, A ORDER BY B)"</li>
 *       We don't care about the order of PARTITION BY expressions.
 * </ul>
 * These, however, would not match.
 * <ul>
 *   <li>"MIN(E) OVER (PARTITION BY A, C ORDER BY B)"</br>
 *       Index expressions must match all the PARTITION BY
 *       expressions before they match any ORDER BY
 *       expressions.  They can't match B until they match
 *       A and C.
 *   </li>
 *   <li>"MIN(E) OVER (PARTITION BY A, D ORDER BY B)"</br>
 *       All partition by expressions must appear in the
 *       index.
 *   </li>
 * </ul>
 *
 * We do this selection for a particular index by iterating over
 * the index's expressions or column references.  We try to match
 * the expression-or-column-reference to each candidate window function
 * or statement level order by expression sequence.  We keep a
 * scoreboard with one score for each candidate.  If a candidate
 * peters out because it doesn't match at all, we mark it dead.
 * If a candidate runs out of expressions, and the sort orders
 * are sensible, it is marked done and is usable.  We currently
 * support only one window function for a select statement.  But the
 * same set of matching logic applies to window functions and any statement
 * level order by clause, so generalizing to multiple window functions
 * is essentially cost free.
 *
 * TODO: In theory, we COULD leverage index ordering when the index covers only a prefix of
 * the ordering requirement list, such as the "ORDER BY A, B, C, D" case listed above.
 * But that still requires an ORDER BY plan node.
 * To gain any substantial advantage, the ORDER BY plan node would have to be smart enough
 * to apply just an incremental "minor" sort on "C" to subsets of the result "grouped by"
 * equal A and B values.  The ORDER BY plan node is not yet that smart.
 * So, for now, this case is handled by tagging the index output as not sorted,
 * leaving the ORDER BY to do the full job.
 *
 * There are some additional considerations that might disqualify a match.
 * A match also requires that all columns are ordered in the same direction.
 * For example, if a table has a tree index on columns "(A, B, C)", then
 * "ORDER BY A, B" or "ORDER BY A DESC, B DESC, C DESC" are considered a match
 * but not "ORDER BY A, B DESC" or "ORDER BY A DESC, B".
 *
 * TODO: Currently only ascending key index definitions are supported
 * -- the DESC keyword is not supported in the index creation DDL.
 * If that is ever enabled, the checks here may need to be generalized
 * to maintain the current level of support for only exact matching or
 * "exact reverse" matching of the ASC/DESC qualifiers on all columns,
 * but no other cases.
 *
 * Caveat: "Reverse scans", that is, support for descending ORDER BYs using ascending key
 * indexes only work when the index scan can start at the very end of the index
 * (to work backwards).
 * That means no equality conditions or upper bound conditions can be allowed that would
 * interfere with the start of the backward scan.
 * To minimize re-work, those query qualifications are not checked here.
 * It is easier to tentatively claim the reverse sort order of the index output, here,
 * and later invalidate that sortDirection upon detecting that the reverse scan
 * is not supportable.
 *
 * Some special cases are supported in addition to the simple match of all the ORDER BY "columns":
 *
 * It is possible for an ORDER BY "column" to have a parameterized form that neither strictly
 * equals nor contradicts an index key component.
 * For example, an index might be defined on a particular character of a column "(substr(A, 1, 1))".
 * This trivially matches "ORDER BY substr(A, 1, 1)".
 * It trivially refuses to match "ORDER BY substr(A, 2, 1)" or even "ORDER BY substr(A, 2, ?)"
 * The more interesting case is "ORDER BY substr(A, ?, 1)" where a match
 * must be predicated on the user specifying the correct value "1" for the parameter.
 * This is handled by allowing the optimization but by generating and returning a
 * "parameter binding" that describes its inherent usage restriction.
 * Such parameterized plans are used for ad hoc statements only; it is easy to validate
 * immediately that they have been passed the correct parameter value.
 * Compiled stored procedure statements need to be more specific about constants
 * used in index expressions, even if that means compiling a separate statement with a
 * constant hard-coded in the place of the parameter to purposely match the indexed expression.
 *
 * It is possible for an index key to contain extra components that do not match the
 * ORDER BY columns and yet do not interfere with the intended ordering for a particular query.
 * For example, an index on "(A, B, C, D)" would not generally be considered a match for
 * "ORDER BY A, D" but in the narrow context of a query that also includes a clause like
 * "WHERE B = ? AND C = 1", the ORDER BY clause is functionally equivalent to
 * "ORDER BY A, B, C, D" so it CAN be considered a match.
 * This case is supported in 2 phases, one here and a later one in
 * getRelevantAccessPathForIndex as follows:
 * As long as each ORDER BY column is eventually found in the index key components
 * (in its correct major-to-minor order and ASC/DESC direction),
 * the positions of any non-matching key components that had to be
 * skipped are simply collected in order into an array of "orderSpoilers".
 * The index ordering is provisionally considered valid.
 * Later, in the processing of getRelevantAccessPathForIndex,
 * failure to find an equality filter for one of these "orderSpoilers"
 * causes an override of the provisional sortDirection established here.
 *
 * In theory, a similar (arguably less probable) case could arise in which the ORDER BY columns
 * contain values that are constrained by the WHERE clause to be equal to constants or parameters
 * and the other ORDER BY columns match the index key components in the usual way.
 * Such a case will simply fail to match, here, possibly resulting in suboptimal plans that
 * make unnecessary use of ORDER BY plan nodes, and possibly even use sequential scan plan nodes.
 * The rationale for not complicating this code to handle that case is that the case should be
 * detected by a statement pre-processor that simplifies the ORDER BY clause prior to any
 * "scan planning".
 *
 *TODO: Another case not accounted for is an ORDER BY list that uses a combination of
 * columns/expressions from different tables -- the most common missed case would be
 * when the major ORDER BY columns are from an outer table (index scan) of a join (NLIJ)
 * and the minor columns from its inner table index scan.
 * This would have to be detected from a wider perspective than that of a single table/index.
 * For now, there is some wasted effort in the join case, as this sort order determination is
 * carefully done for each scan in a join, but the result for all of them is ignored because
 * they are never at the top of the plan tree -- the join is there.
 * In theory, if the left-most child scan of a join tree
 * is an index scan with a valid sort order,
 * that should be enough to avoid an explicit sort.
 * Also, if one or more left-most child scans in a join tree
 * are constrained so that they are known to produce a single row result
 * AND the next-left-most child scan is an index scan with a valid sort order,
 * the explicit sort can be skipped.
 * So, the effort to determine the sort direction of an index scan that participates in a join
 * is currently ALWAYS wasted, and in the future, would continue to be wasted effort for the
 * majority of index scans that do not fall into one of the narrow special cases just described.
 */
/**
 * An index can hold either an expression or a column reference.
 * This class tries to hide the difference between them for the
 * purposes of index selection.
 */
class ExpressionOrColumn {
    // Exactly one of these can be null.
    final AbstractExpression m_expr;
    private final ColumnRef          m_colRef;
    // If m_colRef is non-null, then m_tableScan must
    // be non-null and name the table scan.
    private final StmtTableScan      m_tableScan;
    // This is the expression or column position of this expression or
    // ColumnRef in the candidate index.
    final int                m_indexKeyComponentPosition;
    // This is the sort direction of a statement level
    // order by.  If there is no statement level order
    // by clause this will be SortDirectionType.INVALID.
    private final SortDirectionType m_sortDirection;

    // This cached value saves work on the assumption that it is only used to return
    // final "leaf node" bindingLists that are never updated "in place",
    // but just get their contents dumped into a summary List that was created
    // inline and NOT initialized here.
    final static List<AbstractExpression> s_reusableImmutableEmptyBinding = Collections.emptyList();

    ExpressionOrColumn(int indexEntryNumber, AbstractExpression expr, SortDirectionType sortDir) {
        this(indexEntryNumber, null, expr, sortDir, null);
    }

    ExpressionOrColumn(int aIndexEntryNumber, StmtTableScan tableScan, AbstractExpression expr,
            SortDirectionType sortDir, ColumnRef colRef) {
        // Exactly one of expr or colRef can be null.
        Preconditions.checkArgument((expr == null) == (colRef != null),
                "Exactly one of argument expr or colRef must be null.");
        Preconditions.checkArgument(colRef == null || tableScan != null,
                "When argument colRef is null, tableScan cannot be null.");
        m_expr = expr;
        m_colRef = colRef;
        m_indexKeyComponentPosition = aIndexEntryNumber;
        m_tableScan = tableScan;
        m_sortDirection = sortDir;
    }

    SortDirectionType sortDirection() {
        return m_sortDirection;
    }

    @Override
    public boolean equals(Object otherObj) {
        if ( ! ( otherObj instanceof ExpressionOrColumn) ) {
            return false;
        }
        ExpressionOrColumn other = (ExpressionOrColumn)otherObj;
        //
        // The function findBindingsForOneIndexedExpression is almost
        // like equality, but it matches parameters specially.  It's
        // exactly what we want here, but we have to make sure the
        // ExpressionOrColumn from the index is the second parameter.
        //
        // If both are from the same place, then we reduce this to
        // equals.  I'm not sure this is possible, but better safe
        // than sorry.
        //
        if ((other.m_indexKeyComponentPosition < 0 && m_indexKeyComponentPosition < 0)
                || (0 <= other.m_indexKeyComponentPosition && 0 <= m_indexKeyComponentPosition)) {
            // If they are both expressions, they must be equal.
            if ((other.m_expr != null) && (m_expr != null)) {
                return m_expr.equals(other.m_expr);
            } else if ((other.m_colRef != null) && (m_colRef != null)) {
                // If they are both column references they must be equal.
                return m_colRef.equals(other.m_colRef);
            } else {
                // If they are mixed, sort out which is the column
                // reference and which is the expression.
                AbstractExpression expr = (m_expr != null) ? m_expr : other.m_expr;
                ColumnRef cref = (m_colRef != null) ? m_colRef : other.m_colRef;
                StmtTableScan tscan = (m_tableScan != null) ? m_tableScan : other.m_tableScan;
                assert (expr != null && cref != null);
                // Use the same matcher that findBindingsForOneIndexedExpression
                // uses.
                return matchExpressionAndColumnRef(expr, cref, tscan);
            }
        }
        final ExpressionOrColumn fromStmt = (m_indexKeyComponentPosition < 0) ? this : other;
        final ExpressionOrColumn fromIndx = (m_indexKeyComponentPosition < 0) ? other : this;
        return findBindingsForOneIndexedExpression(fromStmt, fromIndx) != null;
    }

    static boolean matchExpressionAndColumnRef(
            AbstractExpression statementExpr, ColumnRef indexColRef, StmtTableScan tableScan) {
        if (statementExpr instanceof TupleValueExpression) {
            TupleValueExpression tve = (TupleValueExpression)statementExpr;
            return tve.getTableAlias().equals(tableScan.getTableAlias()) &&
                    tve.getColumnName().equals(indexColRef.getColumn().getTypeName());
        } else {
            return false;
        }
    }

    /**
     * Match the indexEntry, which is from an index, with
     * a statement expression or column.  The nextStatementEOC
     * must be an expression, not a column reference.
     *
     * @param nextStatementEOC The expression or column in the SQL statement.
     * @param indexEntry The expression or column in the index.
     * @return A list of bindings for this match.  Return null if
     *         there is no match.  If there are no bindings but the
     *         expressions match, return an empty, non-null list.
     */
    static List<AbstractExpression> findBindingsForOneIndexedExpression(
            ExpressionOrColumn nextStatementEOC, ExpressionOrColumn indexEntry) {
        Preconditions.checkNotNull(nextStatementEOC.m_expr);
        if (indexEntry.m_colRef != null) {
            // This is a column.  So try to match it
            // with the expression in nextStatementEOC.
            if (ExpressionOrColumn.matchExpressionAndColumnRef(
                    nextStatementEOC.m_expr, indexEntry.m_colRef, indexEntry.m_tableScan)) {
                return s_reusableImmutableEmptyBinding;
            } else {
                return null;
            }
        } else { // So, this index entry is an expression.
            List<AbstractExpression> moreBindings =
                    nextStatementEOC.m_expr.bindingToIndexedExpression(indexEntry.m_expr);
            if (moreBindings != null) {
                return moreBindings;
            } else {
                return null;
            }
        }
    }

}
