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

import java.util.HashMap;
import java.util.Map;

import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.types.Type;



/**
 * Implementation of calls to VoltDB functions that may have no SQL standard equivalent.<p>
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FunctionForVoltDB extends FunctionSQL {

    static class FunctionId {
        final private String m_name;
        final private int m_id;
        final private Type m_type;
        final private short[] m_paramList;

        public String getName() {
            return m_name;
        }

        public int getId() {
            return m_id;
        }

        public Type getDataType() {
            return m_type;
        }

        public short[] getParamList() {
            return m_paramList;
        }

        private FunctionId(String name, Type type, int id, short[] paramTypes) { m_name = name; m_type = type; m_id = id; m_paramList = paramTypes; }

        private static final FunctionId[] instances = {
            // These ID numbers need to be unique values for FunctionSQL.functType.
            // For now assume that 1001-2000 are reserved for us.
            // A possible alternative MIGHT be to reserve a single FunctionSQL.functType value and let
            // HSQL pretend that it just has one generic function -- or maybe one per signature? or return type?
            // It depends how smart/useful we can get HSQL to be for us.
            new FunctionId("sql_error", null, 1001, new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),
        };

        private static Map<String, FunctionId> by_LC_name = new HashMap<String, FunctionId>();

        static {
            for (FunctionId fn : instances) {
                by_LC_name.put(fn.m_name, fn);
            }
        }

        static FunctionId fn_by_name(String anyCase) {
            String upCase = anyCase.toLowerCase();
            return by_LC_name.get(upCase);
        }

    }

    private FunctionId m_def;

    public static FunctionSQL newVoltDBFunction(String token, int tokenType) {
        FunctionId def = FunctionId.fn_by_name(token);
        if (def == null) {
            return null;
        }
        FunctionSQL function = new FunctionForVoltDB(def);
        return function;
    }

    public FunctionForVoltDB(FunctionId fn) {
        super();
        m_def     = fn;
        funcType  = m_def.getId();
        name      = m_def.getName();
        parseList = m_def.getParamList();
    }

    @Override
    public void setArguments(Expression[] nodes) {
        //TODO; type-check arguments here? Possibly determine alternative result type for overloads? Possibly insert default values?
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        super.setArguments(nodes);
    }

    @Override
    public Expression getFunctionExpression() {
        //TODO; Here's where any complex aliases can get deconstructed into more explicit expression trees.
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        return super.getFunctionExpression();
    }

    @Override
    Object getValue(Session session, Object[] data) {
        //TODO; Here's where you implement the function for HSQL backends so it can be used for regression testing -- GOOD LUCK!

        switch (funcType) {
        default :
            throw Error.runtimeError(ErrorCode.U_S0500, "This FunctionForVoltDB is not implemented in HSQL backends -- or in HSQL constant-folding.");
        }
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        dataType = m_def.getDataType();
        if (dataType == null && nodes.length > 0) {
            Expression like_child = nodes[0];
            if (like_child != null) {
                dataType = like_child.dataType;
            }
        }
    }

    @Override
    public String getSQL() {

        switch (funcType) {
            default :
                return super.getSQL();
        }
    }

    @Override
    VoltXMLElement voltGetXML(Session session) throws HSQLParseException {
        VoltXMLElement exp = super.voltGetXML(session);
        exp.attributes.put("volt_alias", m_def.getName());
        return exp;
    }
}
