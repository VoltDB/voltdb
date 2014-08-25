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

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

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
        private static final int FUNC_VOLT_SQL_ERROR     = 20000;
        private static final int FUNC_VOLT_DECODE        = 20001;
        private static final int FUNC_VOLT_FIELD         = 20002;
        private static final int FUNC_VOLT_ARRAY_ELEMENT = 20003;
        private static final int FUNC_VOLT_ARRAY_LENGTH  = 20004;

        static final int FUNC_VOLT_SINCE_EPOCH               = 20005;
        static final int FUNC_VOLT_SINCE_EPOCH_SECOND        = 20006;
        static final int FUNC_VOLT_SINCE_EPOCH_MILLISECOND   = 20007;
        static final int FUNC_VOLT_SINCE_EPOCH_MICROSECOND   = 20008;

        static final int FUNC_VOLT_TO_TIMESTAMP              = 20009;
        static final int FUNC_VOLT_TO_TIMESTAMP_SECOND       = 20010;
        static final int FUNC_VOLT_TO_TIMESTAMP_MILLISECOND  = 20011;
        static final int FUNC_VOLT_TO_TIMESTAMP_MICROSECOND  = 20012;

        static final int FUNC_VOLT_TRUNCATE_TIMESTAMP     = 20013;
        static final int FUNC_VOLT_TRUNCATE_YEAR          = 20014;
        static final int FUNC_VOLT_TRUNCATE_QUARTER       = 20015;
        static final int FUNC_VOLT_TRUNCATE_MONTH         = 20016;
        static final int FUNC_VOLT_TRUNCATE_DAY           = 20017;
        static final int FUNC_VOLT_TRUNCATE_HOUR          = 20018;
        static final int FUNC_VOLT_TRUNCATE_MINUTE        = 20019;
        static final int FUNC_VOLT_TRUNCATE_SECOND        = 20020;
        static final int FUNC_VOLT_TRUNCATE_MILLISECOND   = 20021;
        static final int FUNC_VOLT_TRUNCATE_MICROSECOND   = 20022;

        static final int FUNC_VOLT_FROM_UNIXTIME          = 20023;

        static final int FUNC_VOLT_SET_FIELD              = 20024;

        static final int FUNC_VOLT_FORMAT_CURRENCY        = 20025;

        static final int FUNC_CONCAT                      = 124;

        private static final FunctionId[] instances = {

            new FunctionId("sql_error", null, FUNC_VOLT_SQL_ERROR, 0,
                    new Type[] { null, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("decode", null, FUNC_VOLT_DECODE, 2,
                    new Type[] { null, null },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.X_REPEAT, 2, Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionId("field", Type.SQL_VARCHAR, FUNC_VOLT_FIELD, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET}),

            new FunctionId("set_field", Type.SQL_VARCHAR, FUNC_VOLT_SET_FIELD, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR, Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET }),

            new FunctionId("array_element", Type.SQL_VARCHAR, FUNC_VOLT_ARRAY_ELEMENT, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_INTEGER },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION,
                                  Tokens.COMMA, Tokens.QUESTION,
                                  Tokens.CLOSEBRACKET}),

            new FunctionId("array_length", Type.SQL_INTEGER, FUNC_VOLT_ARRAY_LENGTH, -1,
                    new Type[] { Type.SQL_VARCHAR },
                    new short[] { Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.CLOSEBRACKET}),

            new FunctionId("since_epoch", Type.SQL_BIGINT, FUNC_VOLT_SINCE_EPOCH, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_TIMESTAMP },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 5,
                    Tokens.SECOND, Tokens.MILLIS, Tokens.MICROS,
                    Tokens.MILLISECOND, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("to_timestamp", Type.SQL_TIMESTAMP, FUNC_VOLT_TO_TIMESTAMP, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_BIGINT },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 5,
                    Tokens.SECOND, Tokens.MILLIS, Tokens.MICROS,
                    Tokens.MILLISECOND, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("truncate", Type.SQL_TIMESTAMP, FUNC_VOLT_TRUNCATE_TIMESTAMP, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_TIMESTAMP },
                    new short[] {  Tokens.OPENBRACKET, Tokens.X_KEYSET, 11,
                    Tokens.YEAR, Tokens.QUARTER, Tokens.MONTH, Tokens.DAY, Tokens.HOUR,
                    Tokens.MINUTE, Tokens.SECOND, Tokens.MILLIS, Tokens.MILLISECOND,
                    Tokens.MICROS, Tokens.MICROSECOND,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("from_unixtime", Type.SQL_TIMESTAMP, FUNC_VOLT_FROM_UNIXTIME, -1,
                    new Type[] { Type.SQL_BIGINT },
                    new short[] {  Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("format_currency", Type.SQL_VARCHAR, FUNC_VOLT_FORMAT_CURRENCY, -1,
                    new Type[] { Type.SQL_DECIMAL, Type.SQL_INTEGER},
                    new short[] {  Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
            		Tokens.QUESTION, Tokens.CLOSEBRACKET }),

            new FunctionId("concat", Type.SQL_VARCHAR, FUNC_CONCAT, -1,
                    new Type[] { Type.SQL_VARCHAR, Type.SQL_VARCHAR },
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

    private final FunctionId m_def;

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
        case FunctionId.FUNC_CONCAT:
            for (int ii = 0; ii < nodes.length; ii++) {
                if (nodes[ii].dataType == null && nodes[ii].isParam) {
                    nodes[ii].dataType = Type.SQL_VARCHAR;
                }
            }
            break;
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
            Type inputTypeInferred = null;
            Type resultTypeInferred = null;

            for (int ii = 0; ii < nodes.length; ii++) {
                Type argType = nodes[ii].dataType;
                if (argType == null) {
                    // A param here means work to do, below.
                    if (nodes[ii].isParam || nodes[ii].valueData == null) {
                        needParamType = true;
                    }
                    continue;
                }
                // Except for the first and the optional last/"default" argument,
                // the arguments alternate between candidate inputs and candidate results.
                if ((((ii % 2) == 0) || ii == nodes.length-1) && (ii != 0)) {
                    // These arguments represent candidate result values
                    // that may hint at the result type or require hinting from the other result values.
                    if (resultTypeInferred == null) {
                        resultTypeInferred = argType; // Take the first result type hint.
                    } else if (resultTypeInferred.typeComparisonGroup != argType.typeComparisonGroup) {
                        resultTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints.
                    }
                } else {
                    // These arguments represent candidate input keys
                    // that may hint at the input type or may require hinting from the other input keys.
                    if (inputTypeInferred == null) {
                        inputTypeInferred = argType; // Take the first input type hint.
                    } else if (inputTypeInferred.typeComparisonGroup != argType.typeComparisonGroup) {
                        inputTypeInferred = Type.SQL_VARCHAR; // Discard contradictory hints, falling back to string type.
                    }
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

            for (int ii = 0; ii < nodes.length; ii++) {
                Type argType = nodes[ii].dataType;
                if ((argType != null) || ! (nodes[ii].isParam || nodes[ii].valueData == null)) {
                    continue;
                }
                // This is the same test as above for determining that the argument
                // is a candidate result vs. a candidate input.
                if ((((ii % 2) == 0) || ii == nodes.length-1) && (ii != 0)) {
                    nodes[ii].dataType = resultTypeInferred;
                } else {
                    nodes[ii].dataType = inputTypeInferred;
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
                if (nodes[i].dataType == null) {
                    // assert that the ambiguous argument (e.g. '?' parameter) has the required type
                    nodes[i].dataType = paramTypes[i];
                    continue;
                }
                else if (paramTypes[i].canConvertFrom(nodes[i].dataType)) {
                    // Add support to pass in a JDBC time string constant
                    if (paramTypes[i].isDateTimeType() && nodes[i].dataType.isCharacterType()) {
                        String datetimestring = (String) nodes[i].valueData;
                        if (datetimestring != null) {
                            datetimestring = datetimestring.trim();
                            try {
                                Timestamp.valueOf(datetimestring);
                            }
                            catch (Exception e) {
                                throw Error.error(ErrorCode.X_42561);
                            }
                            nodes[i].dataType = paramTypes[i];
                        }
                    } else if (paramTypes[i].isNumberType() && !nodes[i].dataType.isNumberType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }
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

        StringBuffer sb = new StringBuffer();

        switch (funcType) {
            case FunctionId.FUNC_VOLT_DECODE: {
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(nodes[0].getSQL());
                for (int ii = 1; ii < nodes.length; ii++) {
                    sb.append(Tokens.T_COMMA).append(nodes[ii].getSQL());
                }
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_FIELD:
            case FunctionId.FUNC_VOLT_ARRAY_ELEMENT: {
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(nodes[0].getSQL()).append(Tokens.T_COMMA).append(nodes[1].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_ARRAY_LENGTH: {
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(nodes[0].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_SINCE_EPOCH: {
                int timeUnit = ((Number) nodes[0].valueData).intValue();
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(Tokens.getKeyword(timeUnit)).append(Tokens.T_COMMA).append(nodes[1].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_TO_TIMESTAMP: {
                int timeUnit = ((Number) nodes[0].valueData).intValue();
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(Tokens.getKeyword(timeUnit)).append(Tokens.T_COMMA).append(nodes[1].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_TRUNCATE_TIMESTAMP: {
                int timeUnit = ((Number) nodes[0].valueData).intValue();
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(Tokens.getKeyword(timeUnit)).append(Tokens.T_COMMA).append(nodes[1].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_FROM_UNIXTIME: {
                sb.append(name).append(Tokens.T_OPENBRACKET).append(nodes[0].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FunctionId.FUNC_VOLT_SET_FIELD: {
                sb.append(name).append(Tokens.T_OPENBRACKET);
                sb.append(nodes[0].getSQL());
                sb.append(Tokens.T_COMMA).append(nodes[1].getSQL());
                sb.append(Tokens.T_COMMA).append(nodes[2].getSQL());
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            default :
                return super.getSQL();
        }
    }

    // This function will be removed with a new attribute is added XML indicating Function Unit
    public static boolean isUnitFunction(int functionType) {

        switch (functionType) {
            case FunctionId.FUNC_VOLT_SINCE_EPOCH_SECOND:
            case FunctionId.FUNC_VOLT_SINCE_EPOCH_MILLISECOND:
            case FunctionId.FUNC_VOLT_SINCE_EPOCH_MICROSECOND:
            case FunctionId.FUNC_VOLT_TO_TIMESTAMP_SECOND:
            case FunctionId.FUNC_VOLT_TO_TIMESTAMP_MILLISECOND:
            case FunctionId.FUNC_VOLT_TO_TIMESTAMP_MICROSECOND:
            case FunctionId.FUNC_VOLT_TRUNCATE_YEAR:
            case FunctionId.FUNC_VOLT_TRUNCATE_QUARTER:
            case FunctionId.FUNC_VOLT_TRUNCATE_MONTH:
            case FunctionId.FUNC_VOLT_TRUNCATE_DAY:
            case FunctionId.FUNC_VOLT_TRUNCATE_HOUR:
            case FunctionId.FUNC_VOLT_TRUNCATE_MINUTE:
            case FunctionId.FUNC_VOLT_TRUNCATE_SECOND:
            case FunctionId.FUNC_VOLT_TRUNCATE_MILLISECOND:
            case FunctionId.FUNC_VOLT_TRUNCATE_MICROSECOND:
                return true;

            default :
                return false;
        }
    }

}
