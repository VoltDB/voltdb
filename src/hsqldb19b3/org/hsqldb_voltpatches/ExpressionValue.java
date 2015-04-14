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

import org.hsqldb_voltpatches.types.Type;
// A VoltDB extension to allow X'..' as numeric literals
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BinaryData;

import java.math.BigInteger;
// End VoltDB extension
/**
 * Implementation of value access operations.
 *
 * @author Campbell Boucher-Burnett (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionValue extends Expression {

    /**
     * Creates a VALUE expression
     */
    ExpressionValue(Object o, Type datatype) {

        super(OpTypes.VALUE);

        nodes     = Expression.emptyExpressionArray;
        dataType  = datatype;
        valueData = o;
    }

    public String getSQL() {

        switch (opType) {

            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }

                return dataType.convertToSQLString(valueData);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        switch (opType) {

            case OpTypes.VALUE :
                sb.append("VALUE = ").append(valueData);
                sb.append(", TYPE = ").append(dataType.getNameString());

                return sb.toString();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    public Object getValue(Session session) {
        return valueData;
    }
    // A VoltDB extension to allow X'..' as numeric literals
    /**
     * Given a ExpressionValue that is a VARBINARY constant,
     * convert it to a BIGINT constant.  Returns true for a
     * successful conversion and false otherwise.
     *
     * We assume that the VARBINARY constant is representing a
     * 64-bit two's complement integer:
     * - a constant with no digits returns false (no conversion)
     * - a constant with more than 16 digits returns false (too many digits)
     * - a constant that is shorter than 16 digits is implicitly zero-extended
     *   (i.e., constants with less than 16 digits are always positive)
     *
     * These are the VoltDB classes that handle hex literal constants:
     *   voltdb.ParameterConverter
     *   voltdb.expressions.ConstantValueExpression
     *
     * @param parent      Reference of parent expression
     * @param childIndex  Index of this node in parent
     * @return true for a successful conversion and false otherwise.
     */
    boolean mutateToBigintType(Expression parent, int childIndex) {
        if (valueData == null) {
            return false;
        }

        byte[] data = ((BinaryData)valueData).getBytes();
        if (data == null || data.length <= 0 || data.length > 16) {
            return false;
        }

        byte[] dataWithLeadingZeros = new byte[] {0, 0, 0, 0, 0, 0, 0, 0};
        int lenDiff = 8 - data.length;
        for (int j = lenDiff; j < 8; ++j) {
            dataWithLeadingZeros[j] = data[j - lenDiff];
        }

        BigInteger bi = new BigInteger(dataWithLeadingZeros);
        parent.nodes[childIndex] = new ExpressionValue(bi.longValue(), Type.SQL_BIGINT);

        return true;
    }
    // End VoltDB extension
}
