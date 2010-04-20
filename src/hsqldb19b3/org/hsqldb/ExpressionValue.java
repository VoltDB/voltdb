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


package org.hsqldb;

import org.hsqldb.HSQLInterface.HSQLParseException;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;

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

    /*************** VOLTDB *********************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @param indent A string of whitespace to be prepended to every line
     * in the resulting XML.
     * @return XML, correctly indented, representing this object.
     */
    String voltGetXML(Session session, String indent) throws HSQLParseException
    {
        StringBuffer sb = new StringBuffer();

        //
        // We want to keep track of which expressions are the same in the XML output
        //
        String include = "id=\"" + this.getUniqueId() + "\" ";

        // LEAF TYPES
        if (getType() == OpTypes.VALUE) {
            sb.append(indent).append("<value ").append(include);
            sb.append("type=\"").append(Types.getTypeName(dataType.typeCode)).append("\" ");

            if (isParam) {
                sb.append("isparam=\"true\" ");
            } else {
                String value = "NULL";
                if (valueData != null)
                {
                    if (valueData instanceof TimestampData)
                    {
                        // When we get the default from the DDL,
                        // it gets jammed into a TimestampData object.  If we
                        // don't do this, we get a Java class/reference
                        // string in the output schema for the DDL.
                        // EL HACKO: I'm just adding in the timezone seconds
                        // at the moment, hope this is right --izzy
                        TimestampData time = (TimestampData) valueData;
                        value =
                            Long.toString(Math.round((time.getSeconds() +
                                                      time.getZone()) * 1e6) +
                                                     time.getNanos() / 1000);
                    }
                    else
                    {
                        value = valueData.toString();
                    }
                }
                sb.append("value=\"").append(value).append("\" ");
            }

            sb.append("/>");
        }
        else if (getType() == OpTypes.COLUMN) {
            // XXX Should we throw HSQLParseException here?
            assert(false);
        }
        else if (getType() == OpTypes.ASTERISK) {
            sb.append(indent).append("<asterisk/>");
        }

        // catch unexpected types
        else {
            // XXX Should we throw HSQLParseException here instead?
            System.err.println("UNSUPPORTED EXPR TYPE: " + String.valueOf(getType()));
            sb.append("unknown");
        }

        return sb.toString();
    }
}
