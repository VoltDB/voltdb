/* Copyright (c) 2001-2011, The HSQL Development Group
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

import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.HsqlList;
import org.hsqldb_voltpatches.types.Type;

/**
 * database object component access
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since 2.0.0
 */
public class ExpressionAccessor extends Expression {

    ExpressionAccessor(Expression left, Expression right) {

        super(OpTypes.ARRAY_ACCESS);

        nodes = new Expression[] {
            left, right
        };
    }

    public ColumnSchema getColumn() {
        return nodes[LEFT].getColumn();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeGroup rangeGroup, int rangeCount,
            RangeGroup[] rangeGroups, HsqlList unresolvedSet, boolean acceptsSequences) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeGroup, rangeCount, rangeGroups, unresolvedSet, acceptsSequences);
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (!nodes[LEFT].dataType.isArrayType()) {
            throw Error.error(ErrorCode.X_42563);
        }

        dataType = nodes[LEFT].dataType.collectionBaseType();

        if (nodes[RIGHT].opType == OpTypes.DYNAMIC_PARAM) {
            nodes[RIGHT].dataType = Type.SQL_INTEGER;
        }
    }

    public Object getValue(Session session) {

        Object[] array = (Object[]) nodes[LEFT].getValue(session);

        if (array == null) {
            return null;
        }

        Number index = (Number) nodes[RIGHT].getValue(session);

        if (index == null) {
            return null;
        }

        if (index.intValue() < 1 || index.intValue() > array.length) {
            throw Error.error(ErrorCode.X_2202E);
        }

        return array[index.intValue() - 1];
    }

    /**
     * Assignment result
     */
    public Object[] getUpdatedArray(Session session, Object[] array,
                                    Object value, boolean copy) {

        if (array == null) {
            throw Error.error(ErrorCode.X_2200E);
        }

        Number index = (Number) nodes[RIGHT].getValue(session);

        if (index == null) {
            throw Error.error(ErrorCode.X_2202E);
        }

        int i = index.intValue() - 1;

        if (i < 0) {
            throw Error.error(ErrorCode.X_2202E);
        }

        if (i >= nodes[LEFT].dataType.arrayLimitCardinality()) {
            throw Error.error(ErrorCode.X_2202E);
        }

        Object[] newArray = array;

        if (i >= array.length) {
            newArray = new Object[i + 1];

            System.arraycopy(array, 0, newArray, 0, array.length);
        } else if (copy) {
            newArray = new Object[array.length];

            System.arraycopy(array, 0, newArray, 0, array.length);
        }

        newArray[i] = value;

        return newArray;
    }

    public String getSQL() {

        StringBuffer sb   = new StringBuffer(64);
        String       left = getContextSQL(nodes[LEFT]);

        sb.append(left).append('[');
        sb.append(nodes[RIGHT].getSQL()).append(']');

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("ARRAY ACCESS");

        if (getLeftNode() != null) {
            sb.append(" array=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        if (getRightNode() != null) {
            sb.append(" array_index=[");
            sb.append(nodes[RIGHT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }
}
