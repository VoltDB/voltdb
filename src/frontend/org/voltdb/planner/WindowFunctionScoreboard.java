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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.types.SortDirectionType;

/**
 * Objects of this class: keep track of all window functions and
 * order by statement expressions.  We run the index expressions
 * over this scoreboard to see if any of them match appropriately.
 * If so we pull out the window function or statement level order by
 * which match.
 */
public class WindowFunctionScoreboard {
    private final List<WindowFunctionScore> m_winFunctions;
    private final int m_numWinScores;
    private final int m_numOrderByScores;
    private final Set<Integer> m_orderSpoilers = new TreeSet<>();

    /**
     * These are some constants for using indexes for window functions.
     */
    public final static int NO_INDEX_USE = -2;
    final static int STATEMENT_LEVEL_ORDER_BY_INDEX = -1;

    public WindowFunctionScoreboard(AbstractParsedStmt stmt) {
        m_numWinScores = stmt.getWindowFunctionExpressionCount();
        m_numOrderByScores = stmt.hasOrderByColumns() ? 1 : 0;
        m_winFunctions = new ArrayList<>(m_numWinScores + m_numOrderByScores);
        for (int idx = 0; idx < m_numWinScores; ++idx) {
            // stmt has to be a ParsedSelectStmt if 0 < m_numWinScores.
            // So this cast will be ok.
            m_winFunctions.add(new WindowFunctionScore(
                    stmt.getWindowFunctionExpressions().get(idx), idx));
        }
        if (m_numOrderByScores > 0) {
            m_winFunctions.add(new WindowFunctionScore(stmt.orderByColumns()));
        }
    }

    public boolean isDone() {
        return m_winFunctions.stream().allMatch(WindowFunctionScore::isDone);
    }

    public void matchIndexEntry(ExpressionOrColumn indexEntry) {
        m_winFunctions.stream()
                .filter(score -> score.matchIndexEntry(indexEntry) == WindowFunctionScore.MatchResults.POSSIBLE_ORDER_SPOILER)
                .forEach(unused ->
                        // This can only happen with a statement level order by
                        // clause.  We don't return POSSIBLE_ORDER_SPOILER for
                        // window functions.
                        m_orderSpoilers.add(indexEntry.m_indexKeyComponentPosition)
                );
    }

    /**
     * Return the number of order spoilers.  Also, fill in
     * the AccessPath, the order spoilers list and the bindings.
     *
     * We prefer window functions to statement level
     * order by expressions.  If there is more than one
     * window functions, we prefer the one with the least
     * number of order spoilers.  In the case of ties we
     * return the first one in the list, which is essentially
     * random.  Perhaps we can do better.
     *
     * @param access portal to access/update table traits
     * @param orderSpoilers counts agreements between ORDER BY column and table index.
     * @return The number of order spoilers in the candidate we choose.
     */
    public int getResult(AccessPath access, int[] orderSpoilers) {
        WindowFunctionScore answer = null;
        // Fill in the failing return values as a fallback.
        access.bindings.clear();
        access.m_windowFunctionUsesIndex = NO_INDEX_USE;
        access.m_stmtOrderByIsCompatible = false;
        access.sortDirection = SortDirectionType.INVALID;
        int numOrderSpoilers = 0;

        // The statement level order by expressions
        // are always the last in the list.  So, if there
        // are no window functions which match this
        // index, and the order by expressions match,
        // then the last time through this list will pick
        // the order by expressions.  If a window function
        // matches, we will pick it up first.  We
        // can only match the statement level order by
        // statements if on score from a window function
        // matches.
        for (WindowFunctionScore score : m_winFunctions) {
            if (score.isDone()) {
                assert ! score.isDead();
                // Prefer window functions, and prefer longer matches, but we'll
                // take anything.
                if (answer == null || ! answer.isWindowFunction() ||
                        answer.getNumberMatches() < score.getNumberMatches()) {
                    answer = score;
                }
            }
        }
        if (answer != null) {
            assert(answer.sortDirection() != null);
            if (m_numOrderByScores > 0) {
                assert(m_numOrderByScores + m_numWinScores <= m_winFunctions.size());
                WindowFunctionScore orderByScore = m_winFunctions.get(m_numOrderByScores + m_numWinScores - 1);
                assert(orderByScore != null);
                // If the order by score is done and the
                // sort directions match then this
                // index may be usable for the statement level
                // order by as well as for any
                // window functions.
                access.m_stmtOrderByIsCompatible = orderByScore.sortDirection() == answer.sortDirection() &&
                        orderByScore.isDone();
            }
            if (answer.sortDirection() != SortDirectionType.INVALID) {

                // Mark how we are using this index.
                access.m_windowFunctionUsesIndex = answer.m_windowFunctionNumber;
                // If we have an index for the Statement Level
                // Order By clause but there is a window function
                // that can't use the index, then we can't use the
                // index at all. for ordering.  The window function
                // will invalidate the ordering for the statment level
                // order by clause.
                if ((access.m_windowFunctionUsesIndex == STATEMENT_LEVEL_ORDER_BY_INDEX)
                        && (0 < m_numWinScores)) {
                    access.m_stmtOrderByIsCompatible = false;
                    access.m_windowFunctionUsesIndex = NO_INDEX_USE;
                    access.sortDirection = SortDirectionType.INVALID;
                    return 0;
                }

                // Add the bindings.
                access.bindings.addAll(answer.m_bindings);

                // Mark the sort direction.
                if (access.m_windowFunctionUsesIndex == NO_INDEX_USE) {
                    access.sortDirection = SortDirectionType.INVALID;
                } else {
                    access.sortDirection = answer.sortDirection();
                }

                // Add the order spoilers if the index is
                // compatible with the statement level
                // order by clause.
                if (access.m_stmtOrderByIsCompatible) {
                    assert(m_orderSpoilers.size() <= orderSpoilers.length);
                    int idx = 0;
                    for (Integer spoiler : m_orderSpoilers) {
                        orderSpoilers[idx++] = spoiler;
                    }
                    // We will return this.
                    numOrderSpoilers = m_orderSpoilers.size();
                }
                access.m_finalExpressionOrder.addAll(answer.m_orderedMatchingExpressions);
                // else numOrderSpoilers is already zero.
            }
        }
        return numOrderSpoilers;
    }
}

