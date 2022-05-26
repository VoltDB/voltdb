/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.hsqldb_voltpatches;

import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;

/**
 * Reusable object for processing STARTS WITH queries.
 *
 */

class StartsWith {
    private final static BinaryData maxByteValue =
        new BinaryData(new byte[]{ -128 }, false);
    private char[]   cStartsWith;
    private int      iLen;
    private boolean  isNull;
    private boolean  isRightNull;
    boolean          hasCollation;
    boolean          isVariable      = true;
    Type             dataType;

    StartsWith() {}

    void setParams(boolean collation) {
        hasCollation = collation;
    }

    private Object getStartsWith() {

        if (iLen == 0) {
            return "";
        }

        StringBuffer              sb = new StringBuffer();

        int i = 0;

        for (; i < iLen; i++) {
            sb.append(cStartsWith[i]);
        }

        if (i == 0) {
            return null;
        }

        return sb.toString();
    }

    Boolean compare(Session session, Object o) {

        if (o == null) {
            return null;
        }

        if (isNull) {
            return null;
        }

        return compareAt(o, 0, 0, getLength(session, o, "")) ? Boolean.TRUE
                                                             : Boolean.FALSE;
    }

    char getChar(Object o, int i) {
        return ((String) o).charAt(i);
    }

    int getLength(SessionInterface session, Object o, String s) {
        return ((String) o).length();
    }

    private boolean compareAt(Object o, int i, int j, int jLen) {

        for (; i < iLen; i++) {
            if ((j >= jLen) || (cStartsWith[i] != getChar(o, j++))) {
                return false;
            }
        }

        return true;
    }

    void setPattern(Session session, Object pattern, Expression[] nodes) {

        isNull = pattern == null;

        if (isNull) {
            /* ENG-14266, solve 'col STARTS WITH CAST(NULL AS VARCHAR)' Null Pointer problem
             * In this particular case, the right expression has been turned into VALUE type whose valueData is null.
             * EE can handle this case.
             * isRightNull set to be true if it is this case.
             */
            isRightNull = (nodes[Expression.LEFT] instanceof ExpressionColumn) &&
                          (nodes[Expression.RIGHT] instanceof ExpressionOp) &&
                          (nodes[Expression.RIGHT].getType() == OpTypes.VALUE) &&
                          (nodes[Expression.RIGHT].getValue(session) == null);
            return;
        }

        iLen           = 0;

        int l = getLength(session, pattern, "");

        cStartsWith        = new char[l];

        for (int i = 0; i < l; i++) {
            char c = getChar(pattern, i);
            cStartsWith[iLen++] = c;
        }
    }

    boolean isEquivalentToUnknownPredicate() {
        return isNull && !isRightNull;
    }

    // Specific check for 'col STARTS WITH CAST(NULL AS VARCHAR)' case
    boolean isEquivalentToCastNullPredicate() {
        return isRightNull;
    }

    boolean isEquivalentToNotNullPredicate() {

        if (isVariable || isNull) {
            return false;
        }

        return true;
    }

    boolean isEquivalentToCharPredicate() {
        return !isVariable;
    }

    Object getRangeLow() {
        return getStartsWith();
    }

    Object getRangeHigh(Session session) {

        Object o = getStartsWith();

        if (o == null) {
            return null;
        }

        return dataType.concat(session, o, "\uffff");
    }

    public String describe(Session session) {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString()).append("[\n");
        sb.append("isNull=").append(isNull).append('\n');

        sb.append("iLen=").append(iLen).append('\n');
        sb.append("cStartsWith=");
        sb.append(StringUtil.arrayToString(cStartsWith));
        sb.append(']');

        return sb.toString();
    }
}
