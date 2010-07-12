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

import org.hsqldb_voltpatches.index.Index;
import org.hsqldb_voltpatches.lib.HsqlArrayList;

/*
 * Implementation of ORDER BY and LIMIT properties of query expressions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public final class SortAndSlice {

    final static SortAndSlice noSort = new SortAndSlice();
    public int[]              sortOrder;
    public boolean[]          sortDescending;
    public boolean[]          sortNullsLast;
    boolean                   sortUnion;
    HsqlArrayList             exprList = new HsqlArrayList();
    Expression                limitCondition;
    public Index              index;

    SortAndSlice() {}

    public boolean hasOrder() {
        return exprList.size() != 0;
    }

    public boolean hasLimit() {
        return limitCondition != null;
    }

    public int getOrderLength() {
        return exprList.size();
    }

    public void addOrderExpression(Expression e) {
        exprList.add(e);
    }

    public void addLimitCondition(Expression expression) {
        limitCondition = expression;
    }

    public void prepare(QuerySpecification select) {

        int orderByColumnCount = exprList.size();

        if (orderByColumnCount == 0) {
            return;
        }

        sortOrder      = new int[orderByColumnCount];
        sortDescending = new boolean[orderByColumnCount];
        sortNullsLast  = new boolean[orderByColumnCount];

        for (int i = 0; i < orderByColumnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);

            if (sort.getLeftNode().queryTableColumnIndex == -1) {
                sortOrder[i] = select.indexStartOrderBy + i;
            } else {
                sortOrder[i] = sort.getLeftNode().queryTableColumnIndex;
            }

            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
        }
    }

    public int getLimitStart(Session session) {

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getLeftNode().getValue(session);

            if (limit != null) {
                return limit.intValue();
            }
        }

        return 0;
    }

    public int getLimitCount(Session session, int rowCount) {

        int limitCount = 0;

        if (limitCondition != null) {
            Integer limit =
                (Integer) limitCondition.getRightNode().getValue(session);

            if (limit != null) {
                limitCount = limit.intValue();
            }
        }

        if (rowCount != 0 && (limitCount == 0 || rowCount < limitCount)) {
            limitCount = rowCount;
        }

        return limitCount;
    }

    public void setIndex(TableBase table) {

        try {
            index = table.createAndAddIndexStructure(null, sortOrder,
                    sortDescending, sortNullsLast, false, false, false);
        } catch (Throwable t) {
            throw Error.runtimeError(ErrorCode.U_S0500, "SortAndSlice");
        }
    }
}
