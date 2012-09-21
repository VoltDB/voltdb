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
        final private int m_typeParameter;
        final private Type[] m_paramTypes;
        final private short[] m_paramParseList;

        public String getName() {
            return m_name;
        }

        public int getId() {
            return m_id;
        }

        public Type getDataType() {
            return m_type;
        }

        public Type[] getParamTypes() {
            return m_paramTypes;
        }

        public short[] getParamParseList() {
            return m_paramParseList;
        }

        private FunctionId(String name, Type type, int id, int typeParameter, Type[] paramTypes, short[] paramParseList) {
            m_name = name;
            m_type = type;
            m_id = id;
            m_typeParameter = typeParameter;
            m_paramTypes = paramTypes;
            m_paramParseList = paramParseList;
        }

        // These ID numbers need to be unique values for FunctionSQL.functType.
        // Assume that 1-19999 are reserved for existing HSQL functions.
        // That leaves new VoltDB-specific functions free to use values in the 20000s.
        private static final int FUNC_VOLT_SQL_ERROR = 20000;
        private static final int FUNC_VOLT_DECODE = 20001;

        private static final FunctionId[] instances = {
            new FunctionId("sql_error", null, FUNC_VOLT_SQL_ERROR, 0, new Type[] { null, Type.SQL_VARCHAR }, new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("decode", null, FUNC_VOLT_DECODE, 2, new Type[] { null, null },

                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_REPEAT, 2, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),
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

        public int getTypeParameter() {
            return m_typeParameter;
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
        parseList = m_def.getParamParseList();
        parameterArg = m_def.getTypeParameter();
    }

    @Override
    public void setArguments(Expression[] nodes) {
        //TODO; Here's where we might re-order arguments or insert implied values for functions that were implemented as aliases for other functions.
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
        //TODO; Here's where we might substitute wholesale some other HSQL Expression for a function expression that is really just an alias.
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
        //TODO; Here's where we implement the function for HSQL backends so it can be used for regression testing -- GOOD LUCK!
        /*
        switch (m_def.getId()) {
        default :
            break;
        }
        */
        throw Error.runtimeError(ErrorCode.U_S0500, "This FunctionForVoltDB is not implemented in HSQL backends -- or in HSQL constant-folding.");
    }

    @Override
    public void resolveTypes(Session session, Expression parent) {

        Type[] paramTypes = m_def.getParamTypes();

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
                if (i >= paramTypes.length) {
                 // TODO support type checking for variadic functions
                    continue;
                }
                if (paramTypes[i] == null) {
                    continue; // accept all argument types
                }
                if (paramTypes[i].canConvertFrom(nodes[i].dataType)) {
                    continue; // accept compatible argument types
                }
                throw Error.error(ErrorCode.X_42565); // incompatible data type
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
