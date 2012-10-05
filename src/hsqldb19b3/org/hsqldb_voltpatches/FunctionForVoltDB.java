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
        private static final int FUNC_VOLT_FIELD = 20002;
        private static final FunctionId[] instances = {

            new FunctionId("sql_error", null, FUNC_VOLT_SQL_ERROR, 0, new Type[] { null, Type.SQL_VARCHAR }, new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("decode", null, FUNC_VOLT_DECODE, 2, new Type[] { null, null },

                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_REPEAT, 2, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionId("field", Type.SQL_VARCHAR, FUNC_VOLT_FIELD, -1,  new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR },

                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET}),
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
            }
        }

        switch(funcType) {
        /*
         * The types to the FIELD functions parameters are VARCHAR
         */
        case FunctionId.FUNC_VOLT_FIELD:
            if (nodes[0].dataType == null && nodes[0].isParam) {
                nodes[0].dataType = Type.SQL_VARCHAR;
            }
            if (nodes[1].dataType == null && nodes[1].isParam) {
                nodes[1].dataType = Type.SQL_VARCHAR;
            }
            break;

            /*
             * Infer parameter types to make the types of the 1st, 2nd, and (if not the last) 4th, 6th, etc.
             * arguments to DECODE as consistent as possible,
             * and the types of the 3rd, 5th, 7th, etc. and LAST arguments as consistent as possible.
             * Punt to inferring VARCHAR if the other arguments give no clue or are inconsistent
             * -- the VoltDB EE complains about NULL-typed parameters but is somewhat forgiving about
             * mixed argument types.
             */
        case FunctionId.FUNC_VOLT_DECODE:
            // Track whether parameter type hinting is needed for either key or value arguments.
            // For simplicity(?), parameters are not tracked explicitly (by position)
            // or even by category (key vs. value). So, if any parameter hinting is required at all,
            // all arguments get re-checked.
            boolean needParamType = false;

            // Start by trying to infer the inputs' common type from the first argument.
            Type inputTypeInferred = nodes[0].dataType; // Take the first hint.
            // If starting the args with a param, there's already work to be done, below.
            needParamType = (inputTypeInferred == null) && nodes[0].isParam;

            // Similarly, try to infer the common results' type from the last/default argument
            // (present only if there are an even number of arguments).
            Type resultTypeInferred = null;
            if ((nodes.length % 2 == 0)) {
                resultTypeInferred = nodes[nodes.length-1].dataType; // Take the hint.
                // A param here means (more) work to do, below.
                needParamType |= (resultTypeInferred == null) && nodes[nodes.length-1].isParam;
            }

            // With the first and the optional last/"default" argument out of the way,
            // the arguments alternate between candidate key inputs and candidate value results.
            for (int keyIndex = 1; keyIndex < nodes.length-1; keyIndex+=2) {

                // These (odd-numbered) arguments represent additional candidate input keys
                // that may hint at the input type or may require hinting from the other input keys.
                Type argType = nodes[keyIndex].dataType;
                if (argType == null) {
                    // A param here means (more) work to do, below.
                    needParamType |= nodes[keyIndex].isParam;
                }
                else if (inputTypeInferred == null) {
                    inputTypeInferred = argType; // Take the first input type hint.
                } else if (inputTypeInferred != argType) {
                    inputTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints, falling back to string type.
                }

                int valueIndex = keyIndex+1;
                // These (even numbered) arguments represent candidate result values
                // that may hint at the result type or require hinting from the other result values.
                argType = nodes[valueIndex].dataType;
                if (argType == null) {
                    // A param here means (more) work to do, below.
                    needParamType |= nodes[valueIndex].isParam;
                }
                else if (resultTypeInferred == null) {
                    resultTypeInferred = argType; // Take the first result type hint.
                } else if (resultTypeInferred != argType) {
                    resultTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints.
                }
            }

            // With any luck, there are no parameter "?" arguments to worry about.
            if ( ! needParamType) {
                break;
            }

            // No luck, try to infer the parameters' types.
            // Punt to guessing VARCHAR for lack of better information.
            if (inputTypeInferred == null) {
                inputTypeInferred = Type.SQL_VARCHAR;
            }
            if (resultTypeInferred == null) {
                resultTypeInferred = Type.SQL_VARCHAR;
            }

            // If needed, start by tagging the first argument as an input.
            if (nodes[0].isParam && (nodes[0].dataType == null)) {
                nodes[0].dataType = inputTypeInferred;
            }

            // Similarly, tag any last/default argument as a result.
            if ((nodes.length % 2 == 0)) {
                if (nodes[nodes.length-1].isParam && (nodes[nodes.length-1].dataType == null)) {
                    nodes[nodes.length-1].dataType = resultTypeInferred;
                }
            }

            // With the first and the optional last/"default" argument out of the way,
            // the arguments alternate between candidate key inputs and candidate value results.
            for (int keyIndex = 1; keyIndex < nodes.length-1; keyIndex+=2) {

                // These (odd-numbered) arguments represent additional candidate input keys
                // that may require hinting from the other input keys.
                if (nodes[keyIndex].isParam && (nodes[keyIndex].dataType == null)) {
                    nodes[keyIndex].dataType = inputTypeInferred;
                }

                int valueIndex = keyIndex+1;
                // These (even numbered) arguments represent candidate result values
                // that may require hinting from the other result values.
                if (nodes[valueIndex].isParam && (nodes[valueIndex].dataType == null)) {
                    nodes[valueIndex].dataType = resultTypeInferred;
                }
            }
            break;

        default:
            break;
        }

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (i >= paramTypes.length) {
                 // TODO support type checking for variadic functions
                    break;
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
            if (parameterArg < 0 || parameterArg >= nodes.length) {
                throw Error.error(ErrorCode.X_42565); // incompatible data type (so says the error -- we're missing one, actually)
            }
            Expression like_child = nodes[parameterArg];
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
