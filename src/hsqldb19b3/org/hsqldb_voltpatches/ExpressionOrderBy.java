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

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;

/**
 * Implementation of ORDER BY operations
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class ExpressionOrderBy extends Expression {

    private boolean isDescending;
    private boolean isNullsLast;

    ExpressionOrderBy(Expression e) {

        super(OpTypes.ORDER_BY);

        nodes       = new Expression[UNARY];
        nodes[LEFT] = e;
    }

    /**
     * Set an ORDER BY column expression DESC
     */
    void setDescending() {
        isDescending = true;
    }

    /**
     * Is an ORDER BY column expression DESC
     */
    boolean isDescending() {
        return isDescending;
    }

    /**
     * Set an ORDER BY column NULL ordering
     */
    void setNullsLast() {
        isNullsLast = true;
    }

    /**
     * Is an ORDER BY column NULL ordering
     */
    boolean isNullsLast() {
        return isNullsLast;
    }

    @Override
    public Object getValue(Session session) {
        return nodes[LEFT].getValue(session);
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        nodes[LEFT].resolveTypes(session, parent);

        if (nodes[LEFT].isParam) {
            throw Error.error(ErrorCode.X_42567);
        }

        dataType = nodes[LEFT].dataType;
    }

    @Override
    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ORDER).append(' ').append(Tokens.T_BY).append(' ');

        if (nodes[LEFT].alias != null) {
            sb.append(nodes[LEFT].alias.name);
        } else {
            sb.append(nodes[LEFT].getSQL());
        }

        if (isDescending) {
            sb.append(' ').append(Tokens.T_DESC);
        }

        return sb.toString();
    }

    @Override
    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(Tokens.T_ORDER).append(' ').append(Tokens.T_BY);
        sb.append(' ');

        if (isDescending) {
            sb.append(Tokens.T_DESC).append(' ');
        }

        return sb.toString();
    }


    /*************** VOLTDB *********************/

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    @Override
    VoltXMLElement voltGetXML(Session session) throws HSQLParseException
    {
        VoltXMLElement exp = new VoltXMLElement("orderby");
        // We want to keep track of which expressions are the same in the XML output
        exp.attributes.put("id", getUniqueId(session));
        if ((this.alias != null) && (getAlias().length() > 0)) {
            exp.attributes.put("alias", getAlias());
        }
        exp.attributes.put("desc", isDescending ? "true" : "false");

        for (Expression expr : nodes) {
            VoltXMLElement vxmle = expr.voltGetXML(session);
            exp.children.add(vxmle);
            assert(vxmle != null);
        }

        return exp;
    }
}
