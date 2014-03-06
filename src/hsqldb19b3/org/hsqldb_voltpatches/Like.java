/* Copyright (c) 1995-2000, The Hypersonic SQL Group.
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
 * Neither the name of the Hypersonic SQL Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE HYPERSONIC SQL GROUP,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * on behalf of the Hypersonic SQL Group.
 *
 *
 * For work added by the HSQL Development Group:
 *
 * Copyright (c) 2001-2009, The HSQL Development Group
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

import org.hsqldb_voltpatches.lib.StringUtil;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.lib.HsqlByteArrayOutputStream;

/**
 * Reusable object for processing LIKE queries.
 *
 * Enhanced in successive versions of HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @version 1.9.0
 * @since Hypersonic SQL
 */

// boucherb@users 20030930 - patch 1.7.2 - optimize into joins if possible
// fredt@users 20031006 - patch 1.7.2 - reuse Like objects for all rows
// fredt@users 1.9.0 - LIKE for binary strings
class Like {

    private final static BinaryData maxByteValue =
        new BinaryData(new byte[]{ -128 }, false);
    private char[]   cLike;
    private int[]    wildCardType;
    private int      iLen;
    private boolean  isIgnoreCase;
    private int      iFirstWildCard;
    private boolean  isNull;
    int              escapeChar;
    boolean          hasCollation;
    static final int UNDERSCORE_CHAR = 1;
    static final int PERCENT_CHAR    = 2;
    boolean          isVariable      = true;
    boolean          isBinary        = false;
    Type             dataType;

    Like() {}

    void setParams(boolean collation) {
        hasCollation = collation;
    }

    void setIgnoreCase(boolean flag) {
        isIgnoreCase = flag;
    }

    private Object getStartsWith() {

        if (iLen == 0) {
            return isBinary ? BinaryData.zeroLengthBinary
                            : "";
        }

        StringBuffer              sb = null;
        HsqlByteArrayOutputStream os = null;

        if (isBinary) {
            os = new HsqlByteArrayOutputStream();
        } else {
            sb = new StringBuffer();
        }

        int i = 0;

        for (; i < iLen && wildCardType[i] == 0; i++) {
            if (isBinary) {
                os.writeByte(cLike[i]);
            } else {
                sb.append(cLike[i]);
            }
        }

        if (i == 0) {
            return null;
        }

        return isBinary ? new BinaryData(os.toByteArray(), false)
                        : sb.toString();
    }

    Boolean compare(Session session, Object o) {

        if (o == null) {
            return null;
        }

        if (isNull) {
            return null;
        }

        if (isIgnoreCase) {
            o = ((CharacterType) dataType).upper(session, o);
        }

        return compareAt(o, 0, 0, getLength(session, o, "")) ? Boolean.TRUE
                                                             : Boolean.FALSE;
    }

    char getChar(Object o, int i) {

        char c;

        if (isBinary) {
            c = (char) ((BinaryData) o).getBytes()[i];
        } else {
            c = ((String) o).charAt(i);
        }

        return c;
    }

    int getLength(SessionInterface session, Object o, String s) {

        int l;

        if (isBinary) {
            l = (int) ((BinaryData) o).length(session);
        } else {
            l = ((String) o).length();
        }

        return l;
    }

    private boolean compareAt(Object o, int i, int j, int jLen) {

        for (; i < iLen; i++) {
            switch (wildCardType[i]) {

                case 0 :                  // general character
                    if ((j >= jLen) || (cLike[i] != getChar(o, j++))) {
                        return false;
                    }
                    break;

                case UNDERSCORE_CHAR :    // underscore: do not test this character
                    if (j++ >= jLen) {
                        return false;
                    }
                    break;

                case PERCENT_CHAR :       // percent: none or any character(s)
                    if (++i >= iLen) {
                        return true;
                    }

                    while (j < jLen) {
                        if ((cLike[i] == getChar(o, j))
                                && compareAt(o, i, j, jLen)) {
                            return true;
                        }

                        j++;
                    }

                    return false;
            }
        }

        if (j != jLen) {
            return false;
        }

        return true;
    }

    void setPattern(Session session, Object pattern, Object escape,
                    boolean hasEscape) {

        isNull = pattern == null;

        if (!hasEscape) {
            escapeChar = -1;
        } else {
            if (escape == null) {
                isNull = true;

                return;
            } else {
                int length = getLength(session, escape, "");

                if (length != 1) {
                    if (isBinary) {
                        throw Error.error(ErrorCode.X_2200D);
                    } else {
                        throw Error.error(ErrorCode.X_22019);
                    }
                }

                escapeChar = getChar(escape, 0);
            }
        }

        if (isNull) {
            return;
        }

        if (isIgnoreCase) {
            pattern = (String) ((CharacterType) dataType).upper(null, pattern);
        }

        iLen           = 0;
        iFirstWildCard = -1;

        int l = getLength(session, pattern, "");

        cLike        = new char[l];
        wildCardType = new int[l];

        boolean bEscaping = false,
                bPercent  = false;

        for (int i = 0; i < l; i++) {
            char c = getChar(pattern, i);

            if (!bEscaping) {
                if (escapeChar == c) {
                    bEscaping = true;

                    continue;
                } else if (c == '_') {
                    wildCardType[iLen] = UNDERSCORE_CHAR;

                    if (iFirstWildCard == -1) {
                        iFirstWildCard = iLen;
                    }
                } else if (c == '%') {
                    if (bPercent) {
                        continue;
                    }

                    bPercent           = true;
                    wildCardType[iLen] = PERCENT_CHAR;

                    if (iFirstWildCard == -1) {
                        iFirstWildCard = iLen;
                    }
                } else {
                    bPercent = false;
                }
            } else {
                if (c == escapeChar || c == '_' || c == '%') {
                    bPercent  = false;
                    bEscaping = false;
                } else {
                    throw Error.error(ErrorCode.X_22025);
                }
            }

            cLike[iLen++] = c;
        }

        if (bEscaping) {
            throw Error.error(ErrorCode.X_22025);
        }

        for (int i = 0; i < iLen - 1; i++) {
            if ((wildCardType[i] == PERCENT_CHAR)
                    && (wildCardType[i + 1] == UNDERSCORE_CHAR)) {
                wildCardType[i]     = UNDERSCORE_CHAR;
                wildCardType[i + 1] = PERCENT_CHAR;
            }
        }
    }

    boolean hasWildcards() {
        return iFirstWildCard != -1;
    }

    boolean isEquivalentToUnknownPredicate() {
        return isNull;
    }

    boolean isEquivalentToEqualsPredicate() {
        return !isVariable && iFirstWildCard == -1;
    }

    boolean isEquivalentToNotNullPredicate() {

        if (isVariable || isNull || !hasWildcards()) {
            return false;
        }

        for (int i = 0; i < wildCardType.length; i++) {
            if (wildCardType[i] != PERCENT_CHAR) {
                return false;
            }
        }

        return true;
    }

    boolean isEquivalentToBetweenPredicate() {

        return !isVariable && iFirstWildCard > 0
               && iFirstWildCard == wildCardType.length - 1
               && cLike[iFirstWildCard] == '%';
    }

    boolean isEquivalentToBetweenPredicateAugmentedWithLike() {
        return !isVariable && iFirstWildCard > 0
               && cLike[iFirstWildCard] == '%';
    }

    Object getRangeLow() {
        return getStartsWith();
    }

    Object getRangeHigh(Session session) {

        Object o = getStartsWith();

        if (o == null) {
            return null;
        }

        if (isBinary) {
            return new BinaryData(session, (BinaryData) o, maxByteValue);
        } else {
            return dataType.concat(session, o, "\uffff");
        }
    }

    public String describe(Session session) {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString()).append("[\n");
        sb.append("escapeChar=").append(escapeChar).append('\n');
        sb.append("isNull=").append(isNull).append('\n');

//        sb.append("optimised=").append(optimised).append('\n');
        sb.append("isIgnoreCase=").append(isIgnoreCase).append('\n');
        sb.append("iLen=").append(iLen).append('\n');
        sb.append("iFirstWildCard=").append(iFirstWildCard).append('\n');
        sb.append("cLike=");
        sb.append(StringUtil.arrayToString(cLike));
        sb.append('\n');
        sb.append("wildCardType=");
        sb.append(StringUtil.arrayToString(wildCardType));
        sb.append(']');

        return sb.toString();
    }
}
