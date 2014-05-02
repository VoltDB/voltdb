/* Copyright (c) 2001-2009, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.lib.HsqlArrayList;
import org.hsqldb_voltpatches.lib.Iterator;
import org.hsqldb_voltpatches.lib.MultiValueHashMap;
import org.hsqldb_voltpatches.lib.OrderedHashSet;
import org.hsqldb_voltpatches.lib.OrderedIntHashSet;

/**
 * Determines how JOIN and WHERE expressions are used in query
 * processing and which indexes are used for table access.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class RangeVariableResolver {

    RangeVariable[] rangeVariables;
    Expression      conditions;
    OrderedHashSet  rangeVarSet = new OrderedHashSet();
    CompileContext  compileContext;

    //
    HsqlArrayList[] tempJoinExpressions;
    HsqlArrayList[] joinExpressions;
    HsqlArrayList[] whereExpressions;
    HsqlArrayList   queryExpressions = new HsqlArrayList();

    //
    Expression[] inExpressions;
    boolean[]    flags;

    //
    OrderedHashSet set = new OrderedHashSet();

    //
    OrderedIntHashSet colIndexSetEqual  = new OrderedIntHashSet();
    OrderedIntHashSet colIndexSetOther  = new OrderedIntHashSet();
    MultiValueHashMap map               = new MultiValueHashMap();
    int               inExpressionCount = 0;
    boolean           hasOuterJoin      = false;

    RangeVariableResolver(RangeVariable[] rangeVars, Expression conditions,
                          CompileContext compileContext) {

        this.rangeVariables = rangeVars;
        this.conditions     = conditions;
        this.compileContext = compileContext;

        for (int i = 0; i < rangeVars.length; i++) {
            RangeVariable range = rangeVars[i];

            rangeVarSet.add(range);

            if (range.isLeftJoin || range.isRightJoin) {
                hasOuterJoin = true;
            }
        }

        inExpressions       = new Expression[rangeVars.length];
        flags               = new boolean[rangeVars.length];
        tempJoinExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            tempJoinExpressions[i] = new HsqlArrayList();
        }

        joinExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            joinExpressions[i] = new HsqlArrayList();
        }

        whereExpressions = new HsqlArrayList[rangeVars.length];

        for (int i = 0; i < rangeVars.length; i++) {
            whereExpressions[i] = new HsqlArrayList();
        }
    }

    void processConditions() {

        decomposeCondition(conditions, queryExpressions);

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].nonIndexJoinCondition == null) {
                continue;
            }

            decomposeCondition(rangeVariables[i].nonIndexJoinCondition,
                               tempJoinExpressions[i]);

            rangeVariables[i].nonIndexJoinCondition = null;
        }

        conditions = null;

        assignToLists();
        expandConditions();
        assignToRangeVariables();
        processFullJoins();
    }

    /**
     * Divides AND conditions and assigns
     */
    static Expression decomposeCondition(Expression e,
                                         HsqlArrayList conditions) {

        if (e == null) {
            return Expression.EXPR_TRUE;
        }

        Expression arg1 = e.getLeftNode();
        Expression arg2 = e.getRightNode();
        int        type = e.getType();

        if (type == OpTypes.AND) {
            arg1 = decomposeCondition(arg1, conditions);
            arg2 = decomposeCondition(arg2, conditions);

            if (arg1 == Expression.EXPR_TRUE) {
                return arg2;
            }

            if (arg2 == Expression.EXPR_TRUE) {
                return arg1;
            }

            e.setLeftNode(arg1);
            e.setRightNode(arg2);

            return e;
        } else if (type == OpTypes.EQUAL) {
            if (arg1.getType() == OpTypes.ROW
                    && arg2.getType() == OpTypes.ROW) {
                for (int i = 0; i < arg1.nodes.length; i++) {
                    Expression part = new ExpressionLogical(arg1.nodes[i],
                        arg2.nodes[i]);

                    part.resolveTypes(null, null);
                    conditions.add(part);
                }

                return Expression.EXPR_TRUE;
            }
        }

        if (e != Expression.EXPR_TRUE) {
            conditions.add(e);
        }

        return Expression.EXPR_TRUE;
    }

    /**
     * Assigns the conditions to separate lists
     */
    void assignToLists() {

        int lastOuterIndex = -1;

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isLeftJoin
                    || rangeVariables[i].isRightJoin) {
                lastOuterIndex = i;
            }

            if (lastOuterIndex == i) {
                joinExpressions[i].addAll(tempJoinExpressions[i]);
            } else {
                for (int j = 0; j < tempJoinExpressions[i].size(); j++) {
                    assignToLists((Expression) tempJoinExpressions[i].get(j),
                                  joinExpressions, lastOuterIndex + 1);
                }
            }
        }

        for (int i = 0; i < queryExpressions.size(); i++) {
            assignToLists((Expression) queryExpressions.get(i),
                          whereExpressions, lastOuterIndex);
        }
    }

    /**
     * Assigns a single condition to the relevant list of conditions
     *
     * Parameter first indicates the first range variable to which condition
     * can be assigned
     */
    void assignToLists(Expression e, HsqlArrayList[] expressionLists,
                       int first) {

        set.clear();
        e.collectRangeVariables(rangeVariables, set);

        int index = rangeVarSet.getLargestIndex(set);

        // condition is independent of tables if no range variable is found
        if (index == -1) {
            index = 0;
        }

        // condition is assigned to first non-outer range variable
        if (index < first) {
            index = first;
        }

        expressionLists[index].add(e);
    }

    void expandConditions() {
        expandConditions(whereExpressions, false);
        expandConditions(joinExpressions, false);
    }

    void expandConditions(HsqlArrayList[] array, boolean isJoin) {

        for (int i = 0; i < rangeVariables.length; i++) {
            HsqlArrayList list = array[i];

            map.clear();
            set.clear();

            boolean hasChain = false;

            for (int j = 0; j < list.size(); j++) {
                Expression e = (Expression) list.get(j);

                if (!e.isColumnEqual
                        || e.getLeftNode().getRangeVariable()
                           == e.getRightNode().getRangeVariable()) {
                    continue;
                }

                if (e.getLeftNode().getRangeVariable() == rangeVariables[i]) {
                    map.put(e.getLeftNode().getColumn(), e.getRightNode());

                    if (!set.add(e.getLeftNode().getColumn())) {
                        hasChain = true;
                    }
                } else {
                    map.put(e.getRightNode().getColumn(), e.getLeftNode());

                    if (!set.add(e.getRightNode().getColumn())) {
                        hasChain = true;
                    }
                }
            }

            if (hasChain && !(hasOuterJoin && isJoin)) {
                Iterator keyIt = map.keySet().iterator();

                while (keyIt.hasNext()) {
                    Object   key = keyIt.next();
                    Iterator it  = map.get(key);

                    set.clear();

                    while (it.hasNext()) {
                        set.add(it.next());
                    }

                    while (set.size() > 1) {
                        Expression e1 = (Expression) set.remove(set.size()
                            - 1);

                        for (int j = 0; j < set.size(); j++) {
                            Expression e2 = (Expression) set.get(j);

                            closeJoinChain(array, e1, e2);
                        }
                    }
                }
            }
        }
    }

    void closeJoinChain(HsqlArrayList[] array, Expression e1, Expression e2) {

        int idx1  = rangeVarSet.getIndex(e1.getRangeVariable());
        int idx2  = rangeVarSet.getIndex(e2.getRangeVariable());
        int index = idx1 > idx2 ? idx1
                                : idx2;

        array[index].add(new ExpressionLogical(e1, e2));
    }

    /**
     * Assigns conditions to range variables and converts suitable IN conditions
     * to table lookup.
     */
    void assignToRangeVariables() {

        for (int i = 0; i < rangeVariables.length; i++) {
            boolean isOuter = rangeVariables[i].isLeftJoin
                              || rangeVariables[i].isRightJoin;

            if (isOuter) {
                assignToRangeVariable(rangeVariables[i], i,
                                      joinExpressions[i], true);
                assignToRangeVariable(rangeVariables[i], i,
                                      whereExpressions[i], false);
            } else {
                joinExpressions[i].addAll(whereExpressions[i]);
                assignToRangeVariable(rangeVariables[i], i,
                                      joinExpressions[i], true);
            }

            // A VoltDB extension to disable
            // Turn off some weird rewriting of in expressions based on index support for the query.
            // This makes it simpler to parse on the VoltDB side,
            // at the expense of HSQL performance.
            // Also fixed an apparent join/where confusion?
            if (inExpressions[i] != null) {
                if (!flags[i] && isOuter) {
                    rangeVariables[i].addJoinCondition(inExpressions[i]);
                } else {
                    rangeVariables[i].addWhereCondition(inExpressions[i]);
                }
            /* disable 7 lines ...
            if (rangeVariables[i].hasIndexCondition()
                    && inExpressions[i] != null) {
                if (!flags[i] && isOuter) {
                    rangeVariables[i].addWhereCondition(inExpressions[i]);
                } else {
                    rangeVariables[i].addJoinCondition(inExpressions[i]);
                }
            ... disabled 7 lines */
            // End of VoltDB extension

                inExpressions[i] = null;

                inExpressionCount--;
            }
        }

        if (inExpressionCount != 0) {
            // A VoltDB extension to disable
            // This will never be called because of the change made to the block above
            assert(false);
            // End of VoltDB extension
            setInConditionsAsTables();
        }
    }

    /**
     * Assigns a set of conditions to a range variable.
     */
    void assignToRangeVariable(RangeVariable rangeVar, int rangeVarIndex,
                               HsqlArrayList exprList, boolean isJoin) {

        if (exprList.isEmpty()) {
            return;
        }

        colIndexSetEqual.clear();
        colIndexSetOther.clear();

        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);

            if (rangeVar.hasIndexCondition()) {
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);

                continue;
            }

            if (e.getIndexableExpression(rangeVar) == null) {
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);

                continue;
            }

            // can use index
            int type = e.getType();

            switch (type) {

                default : {
                    int colIndex = e.getLeftNode().getColumnIndex();

                    colIndexSetOther.add(colIndex);

                    break;
                }
                case OpTypes.EQUAL :
                    if (e.exprSubType == OpTypes.ANY_QUANTIFIED) {
                        Index index = rangeVar.rangeTable.getIndexForColumn(
                            e.getLeftNode().nodes[0].getColumnIndex());

// code to disable IN optimisation
//                        index = null;
                        if (index != null
                                && inExpressions[rangeVarIndex] == null) {
                            inExpressions[rangeVarIndex] = e;

                            inExpressionCount++;
                        } else {
                            rangeVar.addCondition(e, isJoin);
                        }

                        exprList.set(j, null);

                        continue;
                    }

                // $FALL-THROUGH$
                case OpTypes.IS_NULL : {
                    int colIndex = e.getLeftNode().getColumnIndex();

                    colIndexSetEqual.add(colIndex);

                    break;
                }
                case OpTypes.NOT : {
                    int colIndex =
                        e.getLeftNode().getLeftNode().getColumnIndex();

                    colIndexSetOther.add(colIndex);

                    break;
                }
            }
        }

        boolean isEqual = true;
        Index   idx = rangeVar.rangeTable.getIndexForColumns(colIndexSetEqual);

        if (idx == null) {
            isEqual = false;
            idx     = rangeVar.rangeTable.getIndexForColumns(colIndexSetOther);
        }

        // different procedure for subquery tables
        if (idx == null && rangeVar.rangeTable.isSessionBased) {
            if (!colIndexSetEqual.isEmpty()) {
                int[] cols = colIndexSetEqual.toArray();

                idx = rangeVar.rangeTable.getIndexForColumns(cols);
            }

            if (idx == null && !colIndexSetOther.isEmpty()) {
                int[] cols = colIndexSetOther.toArray();

                idx = rangeVar.rangeTable.getIndexForColumns(cols);
            }
        }

        // no index found
        if (idx == null) {
            for (int j = 0, size = exprList.size(); j < size; j++) {
                Expression e = (Expression) exprList.get(j);

                if (e != null) {
                    rangeVar.addCondition(e, isJoin);
                }
            }

            return;
        }

        // index found
        int[] cols     = idx.getColumns();
        int   colCount = cols.length;

        if (isEqual && colCount > 1) {
            Expression[] firstRowExpressions = new Expression[cols.length];

            for (int j = 0; j < exprList.size(); j++) {
                Expression e = (Expression) exprList.get(j);

                if (e == null) {
                    continue;
                }

                int type = e.getType();

                if (type == OpTypes.EQUAL) {
                    int offset =
                        ArrayUtil.find(cols, e.getLeftNode().getColumnIndex());

                    if (offset != -1 && firstRowExpressions[offset] == null) {
                        firstRowExpressions[offset] = e;

                        exprList.set(j, null);

                        continue;
                    }
                }

                // not used in index lookup
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);
            }

            boolean hasNull = false;

            for (int i = 0; i < firstRowExpressions.length; i++) {
                Expression e = firstRowExpressions[i];

                if (e == null) {
                    if (colCount == cols.length) {
                        colCount = i;
                    }

                    hasNull = true;

                    continue;
                }

                if (hasNull) {
                    rangeVar.addCondition(e, isJoin);

                    firstRowExpressions[i] = null;
                }
            }

            rangeVar.addIndexCondition(firstRowExpressions, idx, colCount,
                                       isJoin);

            return;
        }

        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);

            if (e == null) {
                continue;
            }

            if (rangeVar.hasIndexCondition()) {
                rangeVar.addCondition(e, isJoin);
                exprList.set(j, null);

                continue;
            }

            boolean isIndexed = false;

            if (e.getType() == OpTypes.NOT
                    && cols[0]
                       == e.getLeftNode().getLeftNode().getColumnIndex()) {
                isIndexed = true;
            }

            if (cols[0] == e.getLeftNode().getColumnIndex()) {
                if (e.getRightNode() != null
                        && !e.getRightNode().isCorrelated()) {
                    isIndexed = true;
                }

                if (e.getType() == OpTypes.IS_NULL) {
                    isIndexed = true;
                }
            }

            if (isIndexed) {
                rangeVar.addIndexCondition(e, idx, isJoin);
            } else {
                rangeVar.addCondition(e, isJoin);
            }

            exprList.set(j, null);
        }
    }

    /**
     * Converts an IN conditions into a JOIN
     */
    void setInConditionsAsTables() {

        for (int i = rangeVariables.length - 1; i >= 0; i--) {
            RangeVariable rangeVar = rangeVariables[i];
            Expression    in       = inExpressions[i];

            if (in != null) {
                Index index = rangeVar.rangeTable.getIndexForColumn(
                    in.getLeftNode().nodes[0].getColumnIndex());
                RangeVariable newRangeVar =
                    new RangeVariable(in.getRightNode().subQuery.getTable(),
                                      null, null, null, compileContext);
                RangeVariable[] newList =
                    new RangeVariable[rangeVariables.length + 1];

                ArrayUtil.copyAdjustArray(rangeVariables, newList,
                                          newRangeVar, i, 1);

                rangeVariables = newList;

                // make two columns as arg
                ColumnSchema left = rangeVar.rangeTable.getColumn(
                    in.getLeftNode().nodes[0].getColumnIndex());
                ColumnSchema right = newRangeVar.rangeTable.getColumn(0);
                Expression e = new ExpressionLogical(rangeVar, left,
                                                     newRangeVar, right);

                rangeVar.addIndexCondition(e, index, flags[i]);
            }
        }
    }

    void processFullJoins() {

        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isRightJoin) {}
        }
    }
}
