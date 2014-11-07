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

import org.hsqldb_voltpatches.ParserDQL.CompileContext;
import org.hsqldb_voltpatches.lib.IntValueHashMap;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.BinaryData;
import org.hsqldb_voltpatches.types.BinaryType;
import org.hsqldb_voltpatches.types.BlobData;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of SQL standard function calls
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FunctionSQL extends Expression {

    private final static int   FUNC_POSITION_CHAR                    = 1;     // numeric
    private final static int   FUNC_POSITION_BINARY                  = 2;
    private final static int   FUNC_OCCURENCES_REGEX                 = 3;
    private final static int   FUNC_POSITION_REGEX                   = 4;
    protected final static int FUNC_EXTRACT                          = 5;
    protected final static int FUNC_BIT_LENGTH                       = 6;
    protected final static int FUNC_CHAR_LENGTH                      = 7;
    protected final static int FUNC_OCTET_LENGTH                     = 8;
    private final static int   FUNC_CARDINALITY                      = 9;
    private final static int   FUNC_ABS                              = 10;
    private final static int   FUNC_MOD                              = 11;
    protected final static int FUNC_LN                               = 12;
    private final static int   FUNC_EXP                              = 13;
    private final static int   FUNC_POWER                            = 14;
    private final static int   FUNC_SQRT                             = 15;
    private final static int   FUNC_FLOOR                            = 16;
    private final static int   FUNC_CEILING                          = 17;
    private final static int   FUNC_WIDTH_BUCKET                     = 20;
    protected final static int FUNC_SUBSTRING_CHAR                   = 21;    // string
    private final static int   FUNC_SUBSTRING_REG_EXPR               = 22;
    private final static int   FUNC_SUBSTRING_REGEX                  = 23;
    protected final static int FUNC_FOLD_LOWER                       = 24;
    protected final static int FUNC_FOLD_UPPER                       = 25;
    private final static int   FUNC_TRANSCODING                      = 26;
    private final static int   FUNC_TRANSLITERATION                  = 27;
    private final static int   FUNC_REGEX_TRANSLITERATION            = 28;
    protected final static int FUNC_TRIM_CHAR                        = 29;
    final static int           FUNC_OVERLAY_CHAR                     = 30;
    private final static int   FUNC_CHAR_NORMALIZE                   = 31;
    private final static int   FUNC_SUBSTRING_BINARY                 = 32;
    private final static int   FUNC_TRIM_BINARY                      = 33;
    private final static int   FUNC_OVERLAY_BINARY                   = 40;
    protected final static int FUNC_CURRENT_DATE                     = 41;    // datetime
    protected final static int FUNC_CURRENT_TIME                     = 42;
    protected final static int FUNC_CURRENT_TIMESTAMP                = 43;
    protected final static int FUNC_LOCALTIME                        = 44;
    private final static int   FUNC_LOCALTIMESTAMP                   = 50;
    private final static int   FUNC_CURRENT_CATALOG                  = 51;    // general
    private final static int   FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP  = 52;
    private final static int   FUNC_CURRENT_PATH                     = 53;
    private final static int   FUNC_CURRENT_ROLE                     = 54;
    private final static int   FUNC_CURRENT_SCHEMA                   = 55;
    private final static int   FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE = 56;
    private final static int   FUNC_CURRENT_USER                     = 57;
    private final static int   FUNC_SESSION_USER                     = 58;
    private final static int   FUNC_SYSTEM_USER                      = 59;
    protected final static int FUNC_USER                             = 60;
    private final static int   FUNC_VALUE                            = 61;

    //
    static final short[] noParamList              = new short[]{};
    static final short[] emptyParamList           = new short[] {
        Tokens.OPENBRACKET, Tokens.CLOSEBRACKET
    };
    static final short[] optionalNoParamList      = new short[] {
        Tokens.X_OPTION, 2, Tokens.OPENBRACKET, Tokens.CLOSEBRACKET
    };
    static final short[] singleParamList          = new short[] {
        Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.CLOSEBRACKET
    };
    static final short[] optionalIntegerParamList = new short[] {
        Tokens.X_OPTION, 3, Tokens.OPENBRACKET, Tokens.X_POS_INTEGER,
        Tokens.CLOSEBRACKET
    };
    static final short[] doubleParamList = new short[] {
        Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
        Tokens.CLOSEBRACKET
    };
    static final short[] tripleParamList = new short[] {
        Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
        Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
    };
    static final short[] quadParamList = new short[] {
        Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
        Tokens.COMMA, Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
        Tokens.CLOSEBRACKET
    };

    //
    static IntValueHashMap valueFuncMap   = new IntValueHashMap();
    static IntValueHashMap regularFuncMap = new IntValueHashMap();

    static {
        regularFuncMap.put(Tokens.T_POSITION, FUNC_POSITION_CHAR);
        /*
        regularFuncMap.put(Token.T_OCCURENCES_REGEX, FUNC_OCCURENCES_REGEX);
        */
        regularFuncMap.put(Tokens.T_POSITION_REGEX, FUNC_POSITION_REGEX);
        regularFuncMap.put(Tokens.T_EXTRACT, FUNC_EXTRACT);
        regularFuncMap.put(Tokens.T_BIT_LENGTH, FUNC_BIT_LENGTH);
        regularFuncMap.put(Tokens.T_CHAR_LENGTH, FUNC_CHAR_LENGTH);
        regularFuncMap.put(Tokens.T_CHARACTER_LENGTH, FUNC_CHAR_LENGTH);
        regularFuncMap.put(Tokens.T_OCTET_LENGTH, FUNC_OCTET_LENGTH);
        /*
        regularFuncMap.put(Token.T_CARDINALITY, FUNC_CARDINALITY);
        */
        regularFuncMap.put(Tokens.T_ABS, FUNC_ABS);
        regularFuncMap.put(Tokens.T_MOD, FUNC_MOD);
        regularFuncMap.put(Tokens.T_LN, FUNC_LN);
        regularFuncMap.put(Tokens.T_EXP, FUNC_EXP);
        regularFuncMap.put(Tokens.T_POWER, FUNC_POWER);
        regularFuncMap.put(Tokens.T_SQRT, FUNC_SQRT);
        regularFuncMap.put(Tokens.T_FLOOR, FUNC_FLOOR);
        regularFuncMap.put(Tokens.T_CEILING, FUNC_CEILING);
        regularFuncMap.put(Tokens.T_CEIL, FUNC_CEILING);
        regularFuncMap.put(Tokens.T_WIDTH_BUCKET, FUNC_WIDTH_BUCKET);
        regularFuncMap.put(Tokens.T_SUBSTRING, FUNC_SUBSTRING_CHAR);
        /*
        regularFuncMap.put(Token.T_SUBSTRING_REG_EXPR,
                           FUNC_SUBSTRING_REG_EXPR);
        */
        regularFuncMap.put(Tokens.T_SUBSTRING_REGEX, FUNC_SUBSTRING_REGEX);
        regularFuncMap.put(Tokens.T_LOWER, FUNC_FOLD_LOWER);
        regularFuncMap.put(Tokens.T_UPPER, FUNC_FOLD_UPPER);
        /*
        regularFuncMap.put(Token.T_TRANSCODING, FUNC_TRANSCODING);
        regularFuncMap.put(Token.T_TRANSLITERATION, FUNC_TRANSLITERATION);
        regularFuncMap.put(Token.T_TRASLATION,
                           FUNC_REGEX_TRANSLITERATION);
        */
        regularFuncMap.put(Tokens.T_TRIM, FUNC_TRIM_CHAR);
        regularFuncMap.put(Tokens.T_OVERLAY, FUNC_OVERLAY_CHAR);
        /*
        regularFuncMap.put(Token.T_NORMALIZE, FUNC_CHAR_NORMALIZE);
        */
        regularFuncMap.put(Tokens.T_TRIM, FUNC_TRIM_BINARY);
    }

    static {
        valueFuncMap.put(Tokens.T_CURRENT_DATE, FUNC_CURRENT_DATE);
        valueFuncMap.put(Tokens.T_CURRENT_TIME, FUNC_CURRENT_TIME);
        valueFuncMap.put(Tokens.T_CURRENT_TIMESTAMP, FUNC_CURRENT_TIMESTAMP);
        valueFuncMap.put(Tokens.T_LOCALTIME, FUNC_LOCALTIME);
        valueFuncMap.put(Tokens.T_LOCALTIMESTAMP, FUNC_LOCALTIMESTAMP);
        valueFuncMap.put(Tokens.T_CURRENT_CATALOG, FUNC_CURRENT_CATALOG);
        /*
        valueFuncMap.put(Token.T_CURRENT_DEFAULT_TRANSFORM_GROUP,
                FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP);
        */
        valueFuncMap.put(Tokens.T_CURRENT_PATH, FUNC_CURRENT_PATH);
        valueFuncMap.put(Tokens.T_CURRENT_ROLE, FUNC_CURRENT_ROLE);
        valueFuncMap.put(Tokens.T_CURRENT_SCHEMA, FUNC_CURRENT_SCHEMA);
        /*
        valueFuncMap.put(Token.T_CURRENT_TRANSFORM_GROUP_FOR_TYPE,
                FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE);
        */
        valueFuncMap.put(Tokens.T_CURRENT_USER, FUNC_CURRENT_USER);
        valueFuncMap.put(Tokens.T_SESSION_USER, FUNC_SESSION_USER);
        valueFuncMap.put(Tokens.T_SYSTEM_USER, FUNC_SYSTEM_USER);
        valueFuncMap.put(Tokens.T_USER, FUNC_USER);
        valueFuncMap.put(Tokens.T_VALUE, FUNC_VALUE);
    }

    //
    int     funcType;
    String  name;
    short[] parseList;
    short[] parseListAlt;
    boolean isValueFunction;
    // A VoltDB extension to control SQL functions,
    // their types and whether they are implemented in VoltDB.
    protected int parameterArg = -1;
    protected String voltDisabled;
    private static String DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR = "SQL Function";
    // End of VoltDB extension

    public static FunctionSQL newSQLFunction(String token,
            CompileContext context) {

        int id = regularFuncMap.get(token, -1);

        if (id == -1) {
            id = valueFuncMap.get(token, -1);
        }

        if (id == -1) {
            return null;
        }

        FunctionSQL function = new FunctionSQL(id);

        if (id == FUNC_VALUE) {
            if (context.currentDomain == null) {
                return null;
            }

            function.dataType = context.currentDomain;
        }

        return function;
    }

    public static boolean isFunction(String token) {
        return isRegularFunction(token) || isValueFunction(token);
    }

    public static boolean isRegularFunction(String token) {
        return regularFuncMap.containsKey(token);
    }

    public static boolean isValueFunction(String token) {
        return valueFuncMap.containsKey(token);
    }

    protected FunctionSQL() {

        super(OpTypes.SQL_FUNCTION);

        nodes = Expression.emptyExpressionArray;
    }

    protected FunctionSQL(int id) {

        this();

        this.funcType = id;

        switch (id) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY :
                name      = Tokens.T_POSITION;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.IN,
                    Tokens.QUESTION, Tokens.X_OPTION, 5, Tokens.USING,
                    Tokens.X_KEYSET, 2, Tokens.CHARACTERS, Tokens.OCTETS,
                    Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_EXTRACT :
                name      = Tokens.T_EXTRACT;
                parseList = new short[] {
                    // A VoltDB extension to support more selectors
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 18, Tokens.YEAR,
                    /* disable 1 line ...
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 16, Tokens.YEAR,
                    ... disabled 1 line */
                    // End of VoltDB extension
                    Tokens.MONTH, Tokens.DAY, Tokens.HOUR, Tokens.MINUTE,
                    Tokens.SECOND, Tokens.DAY_OF_WEEK, Tokens.WEEK_OF_YEAR,
                    Tokens.QUARTER, Tokens.DAY_OF_YEAR, Tokens.DAY_OF_MONTH,
                    Tokens.DAY_NAME, Tokens.MONTH_NAME,
                    Tokens.SECONDS_MIDNIGHT, Tokens.TIMEZONE_HOUR,
                    // A VoltDB extension to support WEEK, WEEKDAY
                    Tokens.WEEKDAY, Tokens.WEEK,
                    // End of VoltDB extension
                    Tokens.TIMEZONE_MINUTE, Tokens.FROM, Tokens.QUESTION,
                    Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_CHAR_LENGTH :
                name      = Tokens.T_CHAR_LENGTH;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 5,
                    Tokens.USING, Tokens.X_KEYSET, 2, Tokens.CHARACTERS,
                    Tokens.OCTETS, Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_BIT_LENGTH :
                name      = Tokens.T_BIT_LENGTH;
                parseList = singleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_OCTET_LENGTH :
                name      = Tokens.T_OCTET_LENGTH;
                parseList = singleParamList;
                break;

            case FUNC_CARDINALITY :
                parseList = singleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_ABS :
                name      = Tokens.T_ABS;
                parseList = singleParamList;
                break;

            case FUNC_MOD :
                name      = Tokens.T_MOD;
                parseList = singleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_LN :
                name      = Tokens.T_LN;
                parseList = singleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_EXP :
                name      = Tokens.T_EXP;
                parseList = singleParamList;
                break;

            case FUNC_POWER :
                name      = Tokens.T_POWER;
                parseList = doubleParamList;
                break;

            case FUNC_SQRT :
                name      = Tokens.T_SQRT;
                parseList = singleParamList;
                break;

            case FUNC_FLOOR :
                name      = Tokens.T_FLOOR;
                parseList = singleParamList;
                break;

            case FUNC_CEILING :
                name      = Tokens.T_CEILING;
                parseList = singleParamList;
                break;

            case FUNC_WIDTH_BUCKET :
                name      = Tokens.T_WIDTH_BUCKET;
                parseList = quadParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            // A VoltDB extension to customize the SQL function set support
            case FUNC_SUBSTRING_BINARY :
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // $FALL-THROUGH$
            case FUNC_SUBSTRING_CHAR :
            /* disable 2 lines ...
            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY :
            ... disabled 2 lines */
            // End of VoltDB extension
                name      = Tokens.T_SUBSTRING;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.FROM,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.FOR,
                    Tokens.QUESTION, Tokens.X_OPTION, 5, Tokens.USING,
                    Tokens.X_KEYSET, 2, Tokens.CHARACTERS, Tokens.OCTETS,
                    Tokens.CLOSEBRACKET
                };
                parseListAlt = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
                    // A VoltDB extension to make the third parameter optional
                    /* disable 1 line ...
                    Tokens.QUESTION, Tokens.COMMA, Tokens.QUESTION,
                    ... disabled 1 line */
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA, Tokens.QUESTION,
                    // End of VoltDB extension
                    Tokens.CLOSEBRACKET
                };
                break;

            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                name      = Tokens.T_LOWER;
                parseList = singleParamList;
                break;

            case FUNC_FOLD_UPPER :
                name      = Tokens.T_UPPER;
                parseList = singleParamList;
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                name      = Tokens.T_TRIM;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_OPTION, 11,    //
                    Tokens.X_OPTION, 5,                         //
                    Tokens.X_KEYSET, 3, Tokens.LEADING, Tokens.TRAILING,
                    Tokens.BOTH,                                //
                    Tokens.X_OPTION, 1, Tokens.QUESTION,        //
                    Tokens.FROM, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                break;

            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            // A VoltDB extension to customize the SQL function set support
            case FUNC_OVERLAY_BINARY :
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // $FALL-THROUGH$
            case FUNC_OVERLAY_CHAR :
            /* disable 2 lines ...
            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY :
            ... disabled 2 lines */
            // End of VoltDB extension
                name      = Tokens.T_OVERLAY;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.PLACING,
                    Tokens.QUESTION, Tokens.FROM, Tokens.QUESTION,
                    Tokens.X_OPTION, 2, Tokens.FOR, Tokens.QUESTION,
                    Tokens.X_OPTION, 2, Tokens.USING, Tokens.CHARACTERS,
                    Tokens.CLOSEBRACKET
                };
                break;

            case FUNC_CURRENT_CATALOG :
                name            = Tokens.T_CURRENT_CATALOG;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            /*
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
                break;
            case FUNC_CURRENT_PATH :
                break;
            */
            case FUNC_CURRENT_ROLE :
                name            = Tokens.T_CURRENT_ROLE;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_CURRENT_SCHEMA :
                name            = Tokens.T_CURRENT_SCHEMA;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            /*
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
                break;
            */
            case FUNC_CURRENT_USER :
                name            = Tokens.T_CURRENT_USER;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_SESSION_USER :
                name            = Tokens.T_SESSION_USER;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_SYSTEM_USER :
                name            = Tokens.T_SYSTEM_USER;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_USER :
                name            = Tokens.T_USER;
                parseList       = optionalNoParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_VALUE :
                name            = Tokens.T_VALUE;
                parseList       = noParamList;
                isValueFunction = false;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_CURRENT_DATE :
                name            = Tokens.T_CURRENT_DATE;
                parseList       = noParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_CURRENT_TIME :
                name            = Tokens.T_CURRENT_TIME;
                parseList       = optionalIntegerParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_CURRENT_TIMESTAMP :
                name            = Tokens.T_CURRENT_TIMESTAMP;
                parseList       = optionalIntegerParamList;
                isValueFunction = true;
                break;

            case FUNC_LOCALTIME :
                name            = Tokens.T_LOCALTIME;
                parseList       = optionalIntegerParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_LOCALTIMESTAMP :
                name            = Tokens.T_LOCALTIMESTAMP;
                parseList       = optionalIntegerParamList;
                isValueFunction = true;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONSQL_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionSQL");
        }
    }

    public void setArguments(Expression[] newNodes) {
        this.nodes = newNodes;
    }

    public Expression getFunctionExpression() {
        return this;
    }

    /**
     * Evaluates and returns this Function in the context of the session.<p>
     */
    public Object getValue(Session session) {

        Object[] data = new Object[nodes.length];

        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];

            if (e != null) {
                data[i] = e.getValue(session, e.dataType);
            }
        }

        return getValue(session, data);
    }

    Object getValue(Session session, Object[] data) {

        switch (funcType) {

            case FUNC_POSITION_CHAR : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                long result =
                    ((CharacterType) nodes[1].dataType).position(
                        session, data[1], data[0], nodes[0].dataType, 0) + 1;

                if (nodes[2] != null
                        && ((Number) nodes[2].valueData).intValue()
                           == Tokens.OCTETS) {
                    result *= 2;
                }

                return ValuePool.getLong(result);
            }
            case FUNC_POSITION_BINARY : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                long result =
                    ((BinaryType) nodes[1].dataType).position(
                        session, (BlobData) data[1], (BlobData) data[0],
                        nodes[0].dataType, 0) + 1;

                if (nodes[2] != null
                        && ((Number) nodes[2].valueData).intValue()
                           == Tokens.OCTETS) {
                    result *= 2;
                }

                return ValuePool.getLong(result);
            }
            /*
            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
            */
            case FUNC_EXTRACT : {
                if (data[1] == null) {
                    return null;
                }

                int part = ((Number) nodes[0].valueData).intValue();

                part = DTIType.getFieldNameTypeForToken(part);

                switch (part) {

                    case Types.SQL_INTERVAL_SECOND : {
                        return ((DTIType) nodes[1].dataType).getSecondPart(
                            data[1]);
                    }
                    case DTIType.MONTH_NAME :
                    case DTIType.DAY_NAME : {
                        return ((DateTimeType) nodes[1].dataType)
                            .getPartString(session, data[1], part);
                    }
                    default : {
                        int value =
                            ((DTIType) nodes[1].dataType).getPart(session,
                                data[1], part);

                        return ValuePool.getInt(value);
                    }
                }
            }
            case FUNC_CHAR_LENGTH : {
                if (data[0] == null) {
                    return null;
                }

                long result = ((CharacterType) nodes[0].dataType).size(session,
                    data[0]);

                return ValuePool.getLong(result);
            }
            case FUNC_BIT_LENGTH : {
                if (data[0] == null) {
                    return null;
                }

                long result;

                if (nodes[0].dataType.isBinaryType()) {
                    result = ((BlobData) data[0]).bitLength(session);
                } else {
                    result =
                        16 * ((CharacterType) nodes[0].dataType).size(session,
                            data[0]);
                }

                return ValuePool.getLong(result);
            }
            case FUNC_OCTET_LENGTH : {
                if (data[0] == null) {
                    return null;
                }

                long result;

                if (nodes[0].dataType.isBinaryType()) {
                    result = ((BlobData) data[0]).length(session);
                } else {
                    result =
                        2 * ((CharacterType) nodes[0].dataType).size(session,
                            data[0]);
                }

                return ValuePool.getLong(result);
            }
            /*
            case FUNC_CARDINALITY :
            */
            case FUNC_ABS : {
                if (data[0] == null) {
                    return null;
                }

                return dataType.absolute(data[0]);
            }
            case FUNC_MOD : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                // non-integral arguments are accepted with conversion

                /** @todo - check if widening has an effect */
                Object value =
                    ((NumberType) nodes[0].dataType).divide(nodes[0],
                        nodes[1]);

                value = ((NumberType) nodes[0].dataType).subtract(nodes[0],
                        value, nodes[1].dataType);

                // result type is the same as argList[1]
                return ((NumberType) dataType).convertToTypeLimits(session,
                        value);
            }
            case FUNC_LN : {
                if (data[0] == null) {
                    return null;
                }

                double d = ((Number) data[0]).doubleValue();

                if (d <= 0) {
                    throw Error.error(ErrorCode.X_2201E);
                }

                d = Math.log(d);

                return ValuePool.getDouble(Double.doubleToLongBits(d));
            }
            case FUNC_EXP : {
                if (data[0] == null) {
                    return null;
                }

                double val = Math.exp(((Number) data[0]).doubleValue());
                // A VoltDB extension to tweak compliance with standard sql error handling
                if (Double.isNaN(val) || Double.isInfinite(val)) {
                    throw Error.error(ErrorCode.X_2201F);
                }
                // End of VoltDB extension

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_POWER : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                double base     = ((Number) data[0]).doubleValue();
                double exponent = ((Number) data[1]).doubleValue();
                double val;

                // A VoltDB extension to tweak compliance with standard sql error handling
                //VOLTDB's HSQL_BACKEND doesn't object to negative exponents -- why should it?
                /* disable 3 lines ...
                if (exponent < 0) {
                    throw Error.error(ErrorCode.X_2201F);
                }
                ... disabled 2 lines */
                // End of VoltDB extension

                if (base == 0) {
                    if (exponent < 0) {
                        throw Error.error(ErrorCode.X_2201F);
                    } else if (exponent == 0) {
                        val = 1;
                    } else {
                        val = 0;
                    }
                } else {
                    val = Math.pow(base, exponent);
                    // A VoltDB extension to tweak compliance with standard sql error handling
                    if (Double.isNaN(val) || Double.isInfinite(val)) {
                        throw Error.error(ErrorCode.X_2201F);
                    }
                    // End of VoltDB extension
                }

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_SQRT : {
                if (data[0] == null) {
                    return null;
                }

                double val = Math.sqrt(((Number) data[0]).doubleValue());
                // A VoltDB extension to tweak compliance with standard sql error handling
                if (Double.isNaN(val) || Double.isInfinite(val)) {
                    throw Error.error(ErrorCode.X_2201F);
                }
                // End of VoltDB extension

                return ValuePool.getDouble(Double.doubleToLongBits(val));
            }
            case FUNC_FLOOR : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) dataType).floor(data[0]);
            }
            case FUNC_CEILING : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) dataType).ceiling(data[0]);
            }
            case FUNC_WIDTH_BUCKET : {
                return null;
            }
            case FUNC_SUBSTRING_CHAR : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[1],
                                                      nodes[1].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                // A VoltDB extension to make the third parameter optional and ignored vs. broken and disabled
                /* disable 1 line ...
                if (nodes[2] != null) {
                ... disabled 1 line */
                if (nodes.length > 2 && nodes[2] != null) {
                // End of VoltDB extension
                    if (data[2] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                          nodes[2].dataType);
                    length = ((Number) value).longValue();
                }

                // A VoltDB extension to make the fourth parameter optional and ignored vs. broken and disabled
                /* disable 9 lines ...
                if (nodes[3] != null
                        && ((Number) nodes[2].valueData).intValue()
                           == Tokens.OCTETS) {

                    // not clear what the rules on USING OCTECTS are with UTF
                }

                return ((CharacterType) dataType).substring(session, data[0],
                        offset, length, nodes[2] != null, false);
                ... disabled 9 lines */

                return ((CharacterType) dataType).substring(session, data[0],
                		offset, length, (nodes.length > 2 && nodes[2] != null), false);
                // End of VoltDB extension
            }
            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                if (data[0] == null) {
                    return null;
                }

                return ((CharacterType) dataType).lower(session, data[0]);

            case FUNC_FOLD_UPPER :
                if (data[0] == null) {
                    return null;
                }

                return ((CharacterType) dataType).upper(session, data[0]);

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                boolean leading  = false;
                boolean trailing = false;

                switch (((Number) nodes[0].valueData).intValue()) {

                    case Tokens.BOTH :
                        leading = trailing = true;
                        break;

                    case Tokens.LEADING :
                        leading = true;
                        break;

                    case Tokens.TRAILING :
                        trailing = true;
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionSQL");
                }

                String string = (String) data[1];

                if (string.length() != 1) {
                    throw Error.error(ErrorCode.X_22027);
                }

                int character = string.charAt(0);

                return ((CharacterType) dataType).trim(session, data[2],
                                                       character, leading,
                                                       trailing);
            }
            case FUNC_OVERLAY_CHAR : {
                if (data[0] == null || data[1] == null || data[2] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                      nodes[2].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (nodes[3] != null) {
                    if (data[3] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[3],
                                                          nodes[3].dataType);
                    length = ((Number) value).longValue();
                }

                return ((CharacterType) dataType).overlay(null, data[0],
                        data[1], offset, length, nodes[3] != null);
            }
            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_SUBSTRING_BINARY : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[1],
                                                      nodes[1].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (nodes[2] != null) {
                    if (data[2] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                          nodes[2].dataType);
                    length = ((Number) value).intValue();
                }

                return ((BinaryType) dataType).substring(session,
                        (BlobData) data[0], offset, length, nodes[2] != null);
            }
            case FUNC_TRIM_BINARY : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                boolean leading  = false;
                boolean trailing = false;
                int     spec     = ((Number) nodes[0].valueData).intValue();

                switch (spec) {

                    case Tokens.BOTH :
                        leading = trailing = true;
                        break;

                    case Tokens.LEADING :
                        leading = true;
                        break;

                    case Tokens.TRAILING :
                        trailing = true;
                        break;

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionSQL");
                }

                BlobData string = (BlobData) data[1];

                if (string.length(session) != 1) {
                    throw Error.error(ErrorCode.X_22027);
                }

                byte[] bytes = string.getBytes();

                return ((BinaryType) dataType).trim(session,
                                                    (BlobData) data[3],
                                                    bytes[0], leading,
                                                    trailing);
            }
            case FUNC_OVERLAY_BINARY : {
                if (data[0] == null || data[1] == null || data[2] == null) {
                    return null;
                }

                Object value;

                value = Type.SQL_BIGINT.convertToType(session, data[2],
                                                      nodes[2].dataType);

                long offset = ((Number) value).longValue() - 1;
                long length = 0;

                if (nodes[3] != null) {
                    if (data[3] == null) {
                        return null;
                    }

                    value = Type.SQL_BIGINT.convertToType(session, data[3],
                                                          nodes[3].dataType);
                    length = ((Number) value).longValue();
                }

                return ((BinaryType) dataType).overlay(session,
                                                       (BlobData) data[0],
                                                       (BlobData) data[1],
                                                       offset, length,
                                                       nodes[3] != null);
            }
            case FUNC_CURRENT_CATALOG :
                return session.database.getCatalogName().name;

            /*
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            */
            case FUNC_CURRENT_ROLE :
                return null;

            case FUNC_CURRENT_SCHEMA :
                return session.currentSchema.name;

            /*
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            */
            case FUNC_CURRENT_USER :
                return session.getGrantee().getNameString();

            case FUNC_SESSION_USER :
                return session.getGrantee().getNameString();

            case FUNC_SYSTEM_USER :
                return session.getGrantee().getNameString();

            case FUNC_USER :
                return session.getGrantee().getNameString();

            case FUNC_VALUE :
                return session.sessionData.currentValue;

            case FUNC_CURRENT_DATE :
                return session.getCurrentDate();

            case FUNC_CURRENT_TIME :
                return dataType.convertToTypeLimits(
                    session, session.getCurrentTime(true));

            case FUNC_CURRENT_TIMESTAMP :
                return dataType.convertToTypeLimits(
                    session, session.getCurrentTimestamp(true));

            case FUNC_LOCALTIME :
                return dataType.convertToTypeLimits(
                    session, session.getCurrentTime(false));

            case FUNC_LOCALTIMESTAMP :
                return dataType.convertToTypeLimits(
                    session, session.getCurrentTimestamp(false));

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionSQL");
        }
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (funcType) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY : {
                if (nodes[0].dataType == null) {
                    if (nodes[1].dataType == null) {
                        throw Error.error(ErrorCode.X_42567);
                    }

                    if (nodes[1].dataType.typeCode == Types.SQL_CLOB
                            || nodes[1].dataType.isBinaryType()) {
                        nodes[0].dataType = nodes[1].dataType;
                    } else {
                        nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }
                }

                if (nodes[1].dataType == null) {
                    if (nodes[0].dataType.typeCode == Types.SQL_CLOB
                            || nodes[0].dataType.isBinaryType()) {
                        nodes[1].dataType = nodes[0].dataType;
                    } else {
                        nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }
                }

                if (nodes[0].dataType.isCharacterType()
                        && nodes[1].dataType.isCharacterType()) {
                    funcType = FUNC_POSITION_CHAR;
                } else if (nodes[0].dataType.isBinaryType()
                           && nodes[1].dataType.isBinaryType()) {
                    if (nodes[0].dataType.isBitType()
                            || nodes[1].dataType.isBitType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }

                    funcType = FUNC_POSITION_BINARY;
                } else {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            /*
            case FUNC_OCCURENCES_REGEX :
            case FUNC_POSITION_REGEX :
            */
            case FUNC_EXTRACT : {
                if (nodes[1].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[1].dataType.isDateTimeType()
                        && !nodes[1].dataType.isIntervalType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                int     part = ((Number) nodes[0].valueData).intValue();
                DTIType type = (DTIType) nodes[1].dataType;

                part     = DTIType.getFieldNameTypeForToken(part);
                dataType = type.getExtractType(part);

                break;
            }
            case FUNC_BIT_LENGTH : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_BIT_VARYING_MAX_LENGTH;
                }

                if (!nodes[0].dataType.isCharacterType()
                        && !nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_CHAR_LENGTH :
                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

            // $FALL-THROUGH$
            case FUNC_OCTET_LENGTH : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                }

                if (!nodes[0].dataType.isCharacterType()
                        && !nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_CARDINALITY : {
                dataType = Type.SQL_BIGINT;

                break;
            }
            case FUNC_MOD : {
                if (nodes[0].dataType == null) {
                    nodes[1].dataType = nodes[0].dataType;
                }

                if (nodes[1].dataType == null) {
                    nodes[0].dataType = nodes[1].dataType;
                }

                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isNumberType()
                        || !nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                nodes[0].dataType =
                    ((NumberType) nodes[0].dataType).getIntegralType();
                nodes[1].dataType =
                    ((NumberType) nodes[1].dataType).getIntegralType();
                dataType = nodes[1].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 1;
                // End of VoltDB extension

                break;
            }
            case FUNC_POWER : {
                if (nodes[0].dataType == null) {
                    // A VoltDB extension to customize the SQL function set support
                    // VoltDB swapped out this odd propagation of nulls.
                    // VoltDB simply gives missing types the benefit of the doubt.
                    nodes[0].dataType = Type.SQL_DOUBLE;
                    // For VoltDB, the retest for null below is now redundant.
                    /* disable 1 line ...
                    nodes[1].dataType = nodes[0].dataType;
                    ... disabled 1 line */
                    // End of VoltDB extension
                }

                if (nodes[1].dataType == null) {
                    // VoltDB swapped out this odd propagation of nulls.
                    // ORIGINAL HSQL CODE: nodes[0].dataType = nodes[1].dataType;
                    // VoltDB simply gives missing types the benefit of the doubt.
                    nodes[1].dataType = Type.SQL_DOUBLE;
                    // A VoltDB extension to customize the SQL function set support
                    // VoltDB swapped out this odd propagation of nulls.
                    /* disable 1 line ...
                    nodes[0].dataType = nodes[1].dataType;
                    ... disabled 1 line */
                    // End of VoltDB extension
                }

                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isNumberType()
                        || !nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                nodes[0].dataType = Type.SQL_DOUBLE;
                nodes[1].dataType = Type.SQL_DOUBLE;
                dataType          = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_LN :
            case FUNC_EXP :
            case FUNC_SQRT : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                nodes[0].dataType = Type.SQL_DOUBLE;
                dataType          = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_ABS :
                if (nodes[0].dataType != null
                        && nodes[0].dataType.isIntervalType()) {
                    dataType = nodes[0].dataType;
                    // A VoltDB extension to customize the SQL function set support
                    parameterArg = 0;
                    // End of VoltDB extension

                    break;
                }

            // $FALL-THROUGH$
            case FUNC_FLOOR :
            case FUNC_CEILING : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = nodes[0].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 0;
                // End of VoltDB extension

                break;
            }
            case FUNC_WIDTH_BUCKET : {
                if (nodes[0].dataType == null || nodes[1].dataType == null
                        || nodes[2].dataType == null
                        || nodes[3].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isNumberType()
                        || !nodes[1].dataType.isNumberType()
                        || !nodes[2].dataType.isNumberType()
                        || !nodes[3].dataType.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = nodes[3].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 3;
                // End of VoltDB extension

                break;
            }
            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY : {
                if (nodes[0].dataType == null) {

                    // in 20.6 parameter not allowed as type cannot be determined as binary or char
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                }

                if (!nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                // A VoltDB extension to make the third parameter optional
                /* disable 1 line ...
                if (nodes[2] != null) {
                ... disabled 1 line */
                if (nodes.length > 2 && nodes[2] != null) {
                // End of VoltDB extension
                    if (nodes[2].dataType == null) {
                        nodes[2].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                    }

                    if (!nodes[2].dataType.isNumberType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }

                    nodes[2].dataType =
                        ((NumberType) nodes[2].dataType).getIntegralType();
                }

                dataType = nodes[0].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 0;
                // End of VoltDB extension

                if (dataType.isCharacterType()) {
                    funcType = FUNC_SUBSTRING_CHAR;

                    if (dataType.typeCode == Types.SQL_CHAR) {
                        dataType =
                            CharacterType.getCharacterType(Types.SQL_VARCHAR,
                                                           dataType.precision);
                    }
                } else if (dataType.isBinaryType()) {
                    funcType = FUNC_SUBSTRING_BINARY;
                } else {
                    throw Error.error(ErrorCode.X_42565);
                }

                if (nodes.length > 3 && nodes[3] != null) {

                    // always boolean constant if defined
                }

                break;
            }
            /*
            case FUNCTION_SUBSTRING_REG_EXPR :
                break;
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
            case FUNC_FOLD_UPPER :
                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                dataType = nodes[0].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 0;
                // End of VoltDB extension

                if (!dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42565);
                }
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                if (nodes[0] == null) {
                    nodes[0] =
                        new ExpressionValue(ValuePool.getInt(Tokens.BOTH),
                                            Type.SQL_INTEGER);
                }

                if (nodes[2].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                dataType = nodes[2].dataType;
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 2;
                // End of VoltDB extension

                if (dataType.isCharacterType()) {
                    funcType = FUNC_TRIM_CHAR;

                    if (dataType.typeCode == Types.SQL_CHAR) {
                        dataType =
                            CharacterType.getCharacterType(Types.SQL_VARCHAR,
                                                           dataType.precision);
                    }

                    if (nodes[1] == null) {
                        nodes[1] = new ExpressionValue(" ", Type.SQL_CHAR);
                    }
                    // A VoltDB extension to customize the SQL function set support
                    else if (nodes[1].dataType != Type.SQL_CHAR) {
                        nodes[1].dataType = Type.SQL_CHAR;
                    }
                    // End of VoltDB extension
                } else if (dataType.isBinaryType()) {
                    funcType = FUNC_TRIM_BINARY;

                    if (nodes[1] == null) {
                        nodes[1] = new ExpressionValue(
                            new BinaryData(new byte[]{ 0 }, false),
                            Type.SQL_BINARY);
                    }
                } else {
                    throw Error.error(ErrorCode.X_42565);
                }
                break;

            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY : {
                if (nodes[0].dataType == null) {
                    if (nodes[1].dataType == null) {
                        throw Error.error(ErrorCode.X_42567);
                    }

                    if (nodes[1].dataType.typeCode == Types.SQL_CLOB
                            || nodes[1].dataType.isBinaryType()) {
                        nodes[0].dataType = nodes[1].dataType;
                    } else {
                        nodes[0].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }
                }

                if (nodes[1].dataType == null) {
                    if (nodes[0].dataType.typeCode == Types.SQL_CLOB
                            || nodes[0].dataType.isBinaryType()) {
                        nodes[1].dataType = nodes[0].dataType;
                    } else {
                        nodes[1].dataType = Type.SQL_VARCHAR_DEFAULT;
                    }
                }

                if (nodes[0].dataType.isCharacterType()
                        && nodes[1].dataType.isCharacterType()) {
                    funcType = FUNC_OVERLAY_CHAR;

                    if (nodes[0].dataType.typeCode == Types.SQL_CLOB
                            || nodes[1].dataType.typeCode == Types.SQL_CLOB) {
                        dataType = CharacterType.getCharacterType(
                            Types.SQL_CLOB,
                            nodes[0].dataType.precision
                            + nodes[1].dataType.precision);
                    } else {
                        dataType = CharacterType.getCharacterType(
                            Types.SQL_VARCHAR,
                            nodes[0].dataType.precision
                            + nodes[1].dataType.precision);
                    }
                } else if (nodes[0].dataType.isBinaryType()
                           && nodes[1].dataType.isBinaryType()) {
                    funcType = FUNC_OVERLAY_BINARY;

                    if (nodes[0].dataType.typeCode == Types.SQL_BLOB
                            || nodes[1].dataType.typeCode == Types.SQL_BLOB) {
                        dataType = BinaryType.getBinaryType(
                            Types.SQL_BLOB,
                            nodes[0].dataType.precision
                            + nodes[1].dataType.precision);
                    } else {
                        dataType = BinaryType.getBinaryType(
                            Types.SQL_VARBINARY,
                            nodes[0].dataType.precision
                            + nodes[1].dataType.precision);
                    }
                } else {
                    throw Error.error(ErrorCode.X_42565);
                }
                // A VoltDB extension to customize the SQL function set support
                parameterArg = 0;
                // End of VoltDB extension

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                }

                if (!nodes[2].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                nodes[2].dataType =
                    ((NumberType) nodes[2].dataType).getIntegralType();

                if (nodes[3] != null) {
                    if (nodes[3].dataType == null) {
                        nodes[3].dataType = NumberType.SQL_NUMERIC_DEFAULT_INT;
                    }

                    if (!nodes[3].dataType.isNumberType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }

                    nodes[3].dataType =
                        ((NumberType) nodes[3].dataType).getIntegralType();
                }

                break;
            }
            /*
            case FUNCTION_CHAR_NORMALIZE :
                break;
            */
            case FUNC_CURRENT_CATALOG :
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            case FUNC_CURRENT_ROLE :
            case FUNC_CURRENT_SCHEMA :
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case FUNC_CURRENT_USER :
            case FUNC_SESSION_USER :
            case FUNC_SYSTEM_USER :
            case FUNC_USER :
                dataType = SqlInvariants.SQL_IDENTIFIER;
                break;

            case FUNC_VALUE :
                break;

            case FUNC_CURRENT_DATE :
                dataType = CharacterType.SQL_DATE;
                break;

            case FUNC_CURRENT_TIME : {
                int precision = DateTimeType.defaultTimeFractionPrecision;

                if (nodes[0] != null) {
                    precision = ((Integer) nodes[0].valueData).intValue();
                }

                dataType =
                    DateTimeType.getDateTimeType(Types.SQL_TIME_WITH_TIME_ZONE,
                                                 precision);

                break;
            }
            case FUNC_CURRENT_TIMESTAMP : {
                // A VoltDB extension to disable TIME ZONE support
                dataType = Type.SQL_TIMESTAMP;
                /* disable 8 lines ...
                int precision = DateTimeType.defaultTimestampFractionPrecision;

                if (nodes[0] != null) {
                    precision = ((Integer) nodes[0].valueData).intValue();
                }

                dataType = DateTimeType.getDateTimeType(
                    Types.SQL_TIMESTAMP_WITH_TIME_ZONE, precision);
                ... disabled 1 line */
                // End of VoltDB extension

                break;
            }
            case FUNC_LOCALTIME : {
                int precision = DateTimeType.defaultTimeFractionPrecision;

                if (nodes.length > 0 && nodes[0] != null) {
                    precision = ((Integer) nodes[0].valueData).intValue();
                }

                dataType = DateTimeType.getDateTimeType(Types.SQL_TIME,
                        precision);

                break;
            }
            case FUNC_LOCALTIMESTAMP : {
                int precision = DateTimeType.defaultTimestampFractionPrecision;

                if (nodes[0] != null) {
                    precision = ((Integer) nodes[0].valueData).intValue();
                }

                dataType = DateTimeType.getDateTimeType(Types.SQL_TIMESTAMP,
                        precision);

                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionSQL");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (funcType) {

            case FUNC_POSITION_CHAR :
            case FUNC_POSITION_BINARY : {
                sb.append(Tokens.T_POSITION).append('(')                 //
                    .append(nodes[0].getSQL()).append(' ')               //
                    .append(Tokens.T_IN).append(' ')                     //
                    .append(nodes[1].getSQL());

                if (nodes[2] != null
                        && Boolean.TRUE.equals(nodes[2].valueData)) {
                    sb.append(' ').append(Tokens.T_USING).append(' ').append(
                        Tokens.T_OCTETS);
                }

                sb.append(')');

                break;
            }
            case FUNC_OCCURENCES_REGEX :
                break;

            case FUNC_POSITION_REGEX :
                break;

            case FUNC_EXTRACT : {
                int type = ((Integer) nodes[0].valueData).intValue();

                type = DTIType.getFieldNameTypeForToken(type);

                String token = DTIType.getFieldNameTokenForType(type);

                sb.append(Tokens.T_EXTRACT).append('(').append(token)    //
                    .append(' ').append(Tokens.T_FROM).append(' ')       //
                    .append(nodes[1].getSQL()).append(')');

                break;
            }
            case FUNC_CHAR_LENGTH : {
                sb.append(Tokens.T_CHAR_LENGTH).append('(')              //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_BIT_LENGTH : {
                sb.append(Tokens.T_BIT_LENGTH).append('(')               //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_OCTET_LENGTH : {
                sb.append(Tokens.T_OCTET_LENGTH).append('(')             //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            /*
            case FUNC_CARDINALITY :{
                buf.append(Token.T_CARDINALITY).append('(').append(
                    argList[0].getSQL()).append(')');

                break;
            }
            */
            case FUNC_ABS : {
                sb.append(Tokens.T_ABS).append('(')                      //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_MOD : {
                sb.append(Tokens.T_MOD).append('(')                      //
                    .append(nodes[0].getSQL()).append(',')               //
                    .append(nodes[1].getSQL()).append(')');

                break;
            }
            case FUNC_LN : {
                sb.append(Tokens.T_LN).append('(')                       //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_EXP : {
                sb.append(Tokens.T_EXP).append('(')                      //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_POWER : {
                sb.append(Tokens.T_POWER).append('(')                    //
                    .append(nodes[0].getSQL()).append(',')               //
                    .append(nodes[1].getSQL()).append(')');

                break;
            }
            case FUNC_SQRT : {
                sb.append(Tokens.T_SQRT).append('(')                     //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_FLOOR : {
                sb.append(Tokens.T_FLOOR).append('(')                    //
                    .append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_CEILING : {
                sb.append(Tokens.T_CEILING).append('(').                 //
                    append(nodes[0].getSQL()).append(')');

                break;
            }
            case FUNC_WIDTH_BUCKET : {
                sb.append(Tokens.T_WIDTH_BUCKET).append('(')             //
                    .append(nodes[0].getSQL()).append(',')               //
                    .append(nodes[1].getSQL()).append(',')               //
                    .append(nodes[2].getSQL()).append(',')               //
                    .append(nodes[3].getSQL()).append(')');

                break;
            }
            case FUNC_SUBSTRING_CHAR :
            case FUNC_SUBSTRING_BINARY :
                sb.append(Tokens.T_SUBSTRING).append('(')                //
                    .append(nodes[0].getSQL()).append(' ')               //
                    .append(Tokens.T_FROM).append(' ')                   //
                    .append(nodes[1].getSQL());

                if (nodes[2] != null) {
                    sb.append(' ').append(Tokens.T_FOR).append(' ')      //
                        .append(nodes[2].getSQL());
                }

                if (nodes.length > 3 && nodes[3] != null) {
                    if (Boolean.TRUE.equals(nodes[3].valueData)) {
                        sb.append(' ').append(Tokens.T_USING).append(
                            ' ').append(Tokens.T_OCTETS);
                    }
                }

                sb.append(')');
                break;

            /*
            case FUNCTION_SUBSTRING_REGEX :
                break;
            */
            case FUNC_FOLD_LOWER :
                sb.append(Tokens.T_LOWER).append('(').append(
                    nodes[0].getSQL()).append(')');
                break;

            case FUNC_FOLD_UPPER :
                sb.append(Tokens.T_UPPER).append('(').append(
                    nodes[0].getSQL()).append(')');
                break;

            /*
            case FUNCTION_TRANSCODING :
                break;
            case FUNCTION_TRANSLITERATION :
                break;
            case FUNCTION_REGEX_TRANSLITERATION :
                break;
             */
            case FUNC_OVERLAY_CHAR :
            case FUNC_OVERLAY_BINARY :
                sb.append(Tokens.T_OVERLAY).append('(')                  //
                    .append(nodes[0].getSQL()).append(' ')               //
                    .append(Tokens.T_PLACING).append(' ')                //
                    .append(nodes[1].getSQL()).append(' ')               //
                    .append(Tokens.T_FROM).append(' ')                   //
                    .append(nodes[2].getSQL());

                if (nodes[3] != null) {
                    sb.append(' ').append(Tokens.T_FOR).append(' ').append(
                        nodes[3].getSQL());
                }

                if (nodes[4] != null) {
                    if (Boolean.TRUE.equals(nodes[4].valueData)) {
                        sb.append(' ').append(Tokens.T_USING).append(
                            ' ').append(Tokens.T_OCTETS);
                    }
                }

                sb.append(')');
                break;

            /*
            case FUNCTION_NORMALIZE :
                break;
            */
            case FUNC_TRIM_CHAR :
            case FUNC_TRIM_BINARY :
                String spec = null;

                switch (((Number) nodes[0].valueData).intValue()) {

                    case Tokens.BOTH :
                        spec = Tokens.T_BOTH;
                        break;

                    case Tokens.LEADING :
                        spec = Tokens.T_LEADING;
                        break;

                    case Tokens.TRAILING :
                        spec = Tokens.T_TRAILING;
                        break;
                }

                sb.append(Tokens.T_TRIM).append('(')                     //
                    .append(spec).append(' ')                            //
                    .append(nodes[1].getSQL()).append(' ')               //
                    .append(Tokens.T_FROM).append(' ')                   //
                    .append(nodes[2].getSQL()).append(')');
                break;

            case FUNC_CURRENT_CATALOG :
            case FUNC_CURRENT_DEFAULT_TRANSFORM_GROUP :
            case FUNC_CURRENT_PATH :
            case FUNC_CURRENT_ROLE :
            case FUNC_CURRENT_SCHEMA :
            case FUNC_CURRENT_TRANSFORM_GROUP_FOR_TYPE :
            case FUNC_CURRENT_USER :
            case FUNC_SESSION_USER :
            case FUNC_SYSTEM_USER :
            case FUNC_USER :
            case FUNC_CURRENT_DATE :
            case FUNC_VALUE :
                return name;

            case FUNC_LOCALTIME :
            case FUNC_CURRENT_TIME : {
                int precision = DateTimeType.defaultTimeFractionPrecision;

                if (nodes[0] != null) {
                    precision = ((Number) nodes[0].valueData).intValue();
                }

                if (precision == DateTimeType.defaultTimeFractionPrecision) {
                    return name;
                }

                sb.append(name).append(Tokens.T_OPENBRACKET).append(precision);
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            case FUNC_LOCALTIMESTAMP :
            case FUNC_CURRENT_TIMESTAMP : {
                int precision = DateTimeType.defaultTimestampFractionPrecision;

                if (nodes[0] != null) {
                    precision = ((Number) nodes[0].valueData).intValue();
                }

                if (precision
                        == DateTimeType.defaultTimestampFractionPrecision) {
                    return name;
                }

                sb.append(name).append(Tokens.T_OPENBRACKET).append(precision);
                sb.append(Tokens.T_CLOSEBRACKET);

                return sb.toString();
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionSQL");
        }

        return sb.toString();
    }

    public boolean equals(Object other) {

        if (other instanceof FunctionSQL
                && funcType == ((FunctionSQL) other).funcType) {
            return super.equals(other);
        }

        return false;
    }

    public int hashCode() {
        return opType + funcType;
    }

    /**
     * Returns a String representation of this object. <p>
     */
    public String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("FUNCTION ").append("=[\n");
        sb.append(name).append("(");

        for (int i = 0; i < nodes.length; i++) {
            sb.append("[").append(nodes[i].describe(session)).append("]");
        }

        sb.append(") returns ").append(dataType.getNameString());
        sb.append("]\n");

        return sb.toString();
    }

    public boolean isValueFunction() {
        return isValueFunction;
    }

    /************************* Volt DB Extensions *************************/

    // FunctionCustom adds a few values to the range of FUNC_ constants above that should probaby be
    // kept unique. types.DTIType and Types add a few values to the range used by VoltDB for
    // implementing EXTRACT variants. These are based on other ranges of constants that
    // DO overlap with these FUNC_ constant, so they are dynamically adjusted with the
    // following fixed offset when used as function ids.
    private final static int   SQL_EXTRACT_VOLT_FUNC_OFFSET = 1000;

    // Assume that 10000-19999 are available for VoltDB-specific use
    // in specialized implementations of existing HSQL functions.
    private final static int   FUNC_VOLT_SUBSTRING_CHAR_FROM = 10000;

    /**
     * VoltDB added method to get a non-catalog-dependent
     * representation of this HSQLDB object.
     * @param session The current Session object may be needed to resolve
     * some names.
     * @return XML, correctly indented, representing this object.
     * @throws HSQLParseException
     */
    VoltXMLElement voltAnnotateFunctionXML(VoltXMLElement exp)
    {
        // XXX Should this throw HSQLParseException instead?
        assert(getType() == OpTypes.SQL_FUNCTION);

        exp.attributes.put("name", name);
        exp.attributes.put("valuetype", dataType.getNameString());
        if (parameterArg != -1) {
            exp.attributes.put("parameter", String.valueOf(parameterArg));
        }
        int volt_funcType = funcType;

        switch (funcType) {
        case FUNC_SUBSTRING_CHAR :
            // A little tweaking is needed here because VoltDB wants to define separate functions for 2-argument and 3-argument SUBSTRING
            if (nodes.length == 2 || nodes[2] == null) {
                exp.attributes.put("volt_alias", "substring_from");
                volt_funcType = FUNC_VOLT_SUBSTRING_CHAR_FROM;
            } else {
                exp.attributes.put("volt_alias", "substring_from_for");
            }
            break;
        case FUNC_EXTRACT :
            // A little tweaking is needed here because VoltDB wants to define separate functions for each extract "field" (hard-coded node[0] value).
            String volt_alias = null;
            int keywordConstant = ((Integer) nodes[0].valueData).intValue();
            switch (keywordConstant) {
            case Tokens.DAY_NAME :
            // case DTIType.DAY_NAME :
                volt_alias = "day_name";
                break;
            case Tokens.MONTH_NAME :
            // case DTIType.MONTH_NAME :
                volt_alias = "month_name";
                break;
            case Tokens.QUARTER :
            // case DTIType.QUARTER :
                volt_alias = "quarter";
                break;
            case Tokens.DAY_OF_YEAR :
            // case DTIType.DAY_OF_YEAR :
                volt_alias = "day_of_year";
                break;
            case Tokens.WEEKDAY :
            	volt_alias = "weekday";
            	break;
            case Tokens.DAY_OF_WEEK :
            // case DTIType.DAY_OF_WEEK :
                volt_alias = "day_of_week";
                break;
            case Tokens.WEEK:
            	keywordConstant = Tokens.WEEK_OF_YEAR;
            case Tokens.WEEK_OF_YEAR :
            // case DTIType.WEEK_OF_YEAR :
                volt_alias = "week_of_year";
                break;
            case Types.SQL_INTERVAL_YEAR :
                volt_alias = "interval_year";
                break;
            case Types.SQL_INTERVAL_MONTH :
                volt_alias = "interval_month";
                break;
            case Types.SQL_INTERVAL_DAY :
                volt_alias = "interval_day";
                break;
            case Types.SQL_INTERVAL_HOUR :
                volt_alias = "interval_hour";
                break;
            case Types.SQL_INTERVAL_MINUTE :
                volt_alias = "interval_minute";
                break;
            case Types.SQL_INTERVAL_SECOND :
                volt_alias = "interval_second";
                break;
            case Tokens.YEAR :
                volt_alias = "year";
                break;
            case Tokens.MONTH :
                volt_alias = "month";
                break;
            case Tokens.DAY_OF_MONTH :
            // case DTIType.DAY_OF_MONTH :
                keywordConstant = Tokens.DAY;
            case Tokens.DAY :
                volt_alias = "day";
                break;
            case Tokens.HOUR :
                volt_alias = "hour";
                break;
            case Tokens.MINUTE :
                volt_alias = "minute";
                break;
            case Tokens.SECOND :
                volt_alias = "interval_second";
                break;
            case Tokens.SECONDS_MIDNIGHT :
            // case DTIType.SECONDS_MIDNIGHT :
                volt_alias = "seconds_midnight";
                break;
            case Tokens.TIMEZONE_HOUR :
            // case DTIType.TIMEZONE_HOUR :
                volt_alias = "timezone_hour";
                break;
            case Tokens.TIMEZONE_MINUTE :
            // case DTIType.TIMEZONE_MINUTE :
                volt_alias = "timezone_minute";
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeTypeForVoltDB: " + String.valueOf(keywordConstant));
            }

            exp.attributes.put("function_id", String.valueOf(keywordConstant + SQL_EXTRACT_VOLT_FUNC_OFFSET));
            exp.attributes.put("volt_alias", volt_alias);
            // Having accounted for the first argument, remove it from the child expression list.
            exp.children.remove(0);
            return exp;

        case FunctionForVoltDB.FunctionId.FUNC_VOLT_SINCE_EPOCH :
            volt_alias = null;
            keywordConstant = ((Integer) nodes[0].valueData).intValue();
            int since_epoch_func = -1;
            switch (keywordConstant) {
            case Tokens.SECOND :
                volt_alias = "since_epoch_second";
                since_epoch_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_SINCE_EPOCH_SECOND;
                break;
            case Tokens.MILLIS :
            case Tokens.MILLISECOND:
                volt_alias = "since_epoch_millisecond";
                since_epoch_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_SINCE_EPOCH_MILLISECOND;
                break;
            case Tokens.MICROS :
            case Tokens.MICROSECOND :
                volt_alias = "since_epoch_microsecond";
                since_epoch_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_SINCE_EPOCH_MICROSECOND;
                break;
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeTypeForVoltDB: " + String.valueOf(keywordConstant));
            }

            exp.attributes.put("function_id", String.valueOf(since_epoch_func));
            exp.attributes.put("volt_alias", volt_alias);

            // Having accounted for the first argument, remove it from the child expression list.
            exp.children.remove(0);
            return exp;

        case FunctionForVoltDB.FunctionId.FUNC_VOLT_TO_TIMESTAMP :
            volt_alias = null;
            keywordConstant = ((Integer) nodes[0].valueData).intValue();
            int to_timestamp_func = -1;
            switch (keywordConstant) {
            case Tokens.SECOND :
                volt_alias = "to_timestamp_second";
                to_timestamp_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TO_TIMESTAMP_SECOND;
                break;
            case Tokens.MILLIS :
            case Tokens.MILLISECOND :
                volt_alias = "to_timestamp_millisecond";
                to_timestamp_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TO_TIMESTAMP_MILLISECOND;
                break;
            case Tokens.MICROS :
            case Tokens.MICROSECOND :
                volt_alias = "to_timestamp_microsecond";
                to_timestamp_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TO_TIMESTAMP_MICROSECOND;
                break;
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeTypeForVoltDB: " + String.valueOf(keywordConstant));
            }

            exp.attributes.put("function_id", String.valueOf(to_timestamp_func));
            exp.attributes.put("volt_alias", volt_alias);

            // Having accounted for the first argument, remove it from the child expression list.
            exp.children.remove(0);
            return exp;

        case FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_TIMESTAMP :
            volt_alias = null;
            keywordConstant = ((Integer) nodes[0].valueData).intValue();
            int truncate_func = -1;
            switch (keywordConstant) {
            case Tokens.YEAR :
                volt_alias = "truncate_year";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_YEAR;
                break;
            case Tokens.QUARTER :
                volt_alias = "truncate_quarter";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_QUARTER;
                break;
            case Tokens.MONTH :
                volt_alias = "truncate_month";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_MONTH;
                break;
            case Tokens.DAY :
                volt_alias = "truncate_day";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_DAY;
                break;
            case Tokens.HOUR :
                volt_alias = "truncate_hour";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_HOUR;
                break;
            case Tokens.MINUTE :
                volt_alias = "truncate_minute";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_MINUTE;
                break;
            case Tokens.SECOND :
                volt_alias = "truncate_second";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_SECOND;
                break;
            case Tokens.MILLIS:
            case Tokens.MILLISECOND :
                volt_alias = "truncate_millisecond";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_MILLISECOND;
                break;
            case Tokens.MICROS:
            case Tokens.MICROSECOND :
                volt_alias = "truncate_microseconcd";
                truncate_func = FunctionForVoltDB.FunctionId.FUNC_VOLT_TRUNCATE_MICROSECOND;
                break;
            default:
                throw Error.runtimeError(ErrorCode.U_S0500, "DateTimeTypeForVoltDB: " + String.valueOf(keywordConstant));
            }

            exp.attributes.put("function_id", String.valueOf(truncate_func));
            exp.attributes.put("volt_alias", volt_alias);

            // Having accounted for the first argument, remove it from the child expression list.
            exp.children.remove(0);
            return exp;

        default :
            if (voltDisabled != null) {
                exp.attributes.put("disabled", voltDisabled);
            }
            break;
        }

        exp.attributes.put("function_id", String.valueOf(volt_funcType));

        switch (funcType) {
        case FUNC_SUBSTRING_CHAR :
            // A little tweaking is needed here because VoltDB wants to define separate functions for 2-argument and 3-argument SUBSTRING
            if (nodes.length == 2 || nodes[2] == null) {
                exp.attributes.put("volt_alias", "substring_from");
            } else {
                exp.attributes.put("volt_alias", "substring_from_for");
            }
            break;
        default :
            break;
        }

        return exp;
    }

    public static int voltGetCurrentTimestampId() {
        return FUNC_CURRENT_TIMESTAMP;
    }
    /**********************************************************************/
}
