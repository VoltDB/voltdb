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

import java.text.SimpleDateFormat;
import java.util.Date;

import org.hsqldb_voltpatches.lib.IntKeyIntValueHashMap;
import org.hsqldb_voltpatches.store.ValuePool;
import org.hsqldb_voltpatches.types.CharacterType;
import org.hsqldb_voltpatches.types.ClobData;
import org.hsqldb_voltpatches.types.DTIType;
import org.hsqldb_voltpatches.types.DateTimeType;
import org.hsqldb_voltpatches.types.IntervalMonthData;
import org.hsqldb_voltpatches.types.IntervalSecondData;
import org.hsqldb_voltpatches.types.IntervalType;
import org.hsqldb_voltpatches.types.NumberType;
import org.hsqldb_voltpatches.types.TimeData;
import org.hsqldb_voltpatches.types.TimestampData;
import org.hsqldb_voltpatches.types.Type;

/**
 * Implementation of calls to HSQLDB functions with reserved names or functions
 * that have an SQL standard equivalent.<p>
 *
 * Some functions are translated into equivalent SQL Standard functions.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.9.0
 * @since 1.9.0
 */
public class FunctionCustom extends FunctionSQL {

    public static final String[] openGroupNumericFunctions = {
        "ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "BITAND", "BITOR", "BITXOR",
        "CEILING", "COS", "COT", "DEGREES", "EXP", "FLOOR", "LOG", "LOG10",
        "MOD", "PI", "POWER", "RADIANS", "RAND", "ROUND", "ROUNDMAGIC", "SIGN",
        "SIN", "SQRT", "TAN", "TRUNCATE"
    };
    public static final String[] openGroupStringFunctions = {
        "ASCII", "CHAR", "CONCAT", "DIFFERENCE", "HEXTORAW", "INSERT", "LCASE",
        "LEFT", "LENGTH", "LOCATE", "LTRIM", "RAWTOHEX", "REPEAT", "REPLACE",
        "RIGHT", "RTRIM", "SOUNDEX", "SPACE", "SUBSTR", "UCASE",
    };
    public static final String[] openGroupDateTimeFunctions = {
        "CURDATE", "CURTIME", "DATEDIFF", "DAYNAME", "DAYOFMONTH", "DAYOFWEEK",
        "DAYOFYEAR", "HOUR", "MINUTE", "MONTH", "MONTHNAME", "NOW", "QUARTER",
        "SECOND", "SECONDS_SINCE_MIDNIGHT", "TIMESTAMPADD", "TIMESTAMPDIFF",
        "TO_CHAR", "WEEK", "YEAR"
    };
    public static final String[] openGroupSystemFunctions = {
        "DATABASE", "IFNULL", "USER"
    };

    //
    private final static int FUNC_ISAUTOCOMMIT            = 71;
    private final static int FUNC_ISREADONLYSESSION       = 72;
    private final static int FUNC_ISREADONLYDATABASE      = 73;
    private final static int FUNC_ISREADONLYDATABASEFILES = 74;
    private final static int FUNC_DATABASE                = 75;
    private final static int FUNC_IDENTITY                = 76;
    private final static int FUNC_SYSDATE                 = 77;
    private final static int FUNC_TIMESTAMPADD            = 78;
    private final static int FUNC_TIMESTAMPDIFF           = 79;
    private final static int FUNC_TRUNCATE                = 80;
    private final static int FUNC_TO_CHAR                 = 81;
    private final static int FUNC_TIMESTAMP               = 82;

    //
    private static final int FUNC_ACOS             = 101;
    private static final int FUNC_ASIN             = 102;
    private static final int FUNC_ATAN             = 103;
    private static final int FUNC_ATAN2            = 104;
    private static final int FUNC_COS              = 105;
    private static final int FUNC_COT              = 106;
    private static final int FUNC_DEGREES          = 107;
    private static final int FUNC_LOG10            = 110;
    private static final int FUNC_PI               = 111;
    private static final int FUNC_RADIANS          = 112;
    private static final int FUNC_RAND             = 113;
    private static final int FUNC_ROUND            = 114;
    private static final int FUNC_SIGN             = 115;
    private static final int FUNC_SIN              = 116;
    private static final int FUNC_TAN              = 117;
    private static final int FUNC_BITAND           = 118;
    private static final int FUNC_BITOR            = 119;
    private static final int FUNC_BITXOR           = 120;
    private static final int FUNC_ROUNDMAGIC       = 121;
    private static final int FUNC_ASCII            = 122;
    private static final int FUNC_CHAR             = 123;
    private static final int FUNC_CONCAT           = 124;
    private static final int FUNC_DIFFERENCE       = 125;
    private static final int FUNC_HEXTORAW         = 126;
    private static final int FUNC_LEFT             = 128;
    private static final int FUNC_LOCATE           = 130;
    private static final int FUNC_LTRIM            = 131;
    private static final int FUNC_RAWTOHEX         = 132;
    private static final int FUNC_REPEAT           = 133;
    private static final int FUNC_REPLACE          = 134;
    private static final int FUNC_RIGHT            = 135;
    private static final int FUNC_RTRIM            = 136;
    private static final int FUNC_SOUNDEX          = 137;
    private static final int FUNC_SPACE            = 138;
    private static final int FUNC_SUBSTR           = 139;
    private static final int FUNC_DATEDIFF         = 140;
    private static final int FUNC_SECONDS_MIDNIGHT = 141;

    //
    static final IntKeyIntValueHashMap customRegularFuncMap =
        new IntKeyIntValueHashMap();

    static {
        customRegularFuncMap.put(Tokens.LENGTH, FUNC_CHAR_LENGTH);
        customRegularFuncMap.put(Tokens.BITLENGTH, FUNC_BIT_LENGTH);
        customRegularFuncMap.put(Tokens.OCTETLENGTH, FUNC_OCTET_LENGTH);
        customRegularFuncMap.put(Tokens.LCASE, FUNC_FOLD_LOWER);
        customRegularFuncMap.put(Tokens.UCASE, FUNC_FOLD_UPPER);
        customRegularFuncMap.put(Tokens.LOG, FUNC_LN);

        //
        customRegularFuncMap.put(Tokens.CURDATE, FUNC_CURRENT_DATE);
        customRegularFuncMap.put(Tokens.CURTIME, FUNC_LOCALTIME);
        customRegularFuncMap.put(Tokens.SUBSTR, FUNC_SUBSTRING_CHAR);

        //
        customRegularFuncMap.put(Tokens.YEAR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.MONTH, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAY, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.HOUR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.MINUTE, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.SECOND, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYNAME, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.NONTHNAME, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFMONTH, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFWEEK, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.DAYOFYEAR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.QUARTER, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.WEEK, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.SECONDS_MIDNIGHT, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.LTRIM, FUNC_TRIM_CHAR);
        customRegularFuncMap.put(Tokens.RTRIM, FUNC_TRIM_CHAR);
        customRegularFuncMap.put(Tokens.LEFT, FUNC_LEFT);
        // A VoltDB extension to support WEEKOFYEAR, WEEKDAY function
        customRegularFuncMap.put(Tokens.WEEKOFYEAR, FUNC_EXTRACT);
        customRegularFuncMap.put(Tokens.WEEKDAY, FUNC_EXTRACT);
        // End of VoltDB extension

        //
        customRegularFuncMap.put(Tokens.IDENTITY, FUNC_IDENTITY);
        customRegularFuncMap.put(Tokens.TIMESTAMPADD, FUNC_TIMESTAMPADD);
        customRegularFuncMap.put(Tokens.TIMESTAMPDIFF, FUNC_TIMESTAMPDIFF);
        customRegularFuncMap.put(Tokens.TRUNCATE, FUNC_TRUNCATE);
        customRegularFuncMap.put(Tokens.TO_CHAR, FUNC_TO_CHAR);
        customRegularFuncMap.put(Tokens.TIMESTAMP, FUNC_TIMESTAMP);

        //
        customRegularFuncMap.put(Tokens.LOCATE, FUNC_LOCATE);
        customRegularFuncMap.put(Tokens.INSERT, FUNC_OVERLAY_CHAR);

        //
        //
        customRegularFuncMap.put(Tokens.DATABASE, FUNC_DATABASE);
        customRegularFuncMap.put(Tokens.ISAUTOCOMMIT, FUNC_ISAUTOCOMMIT);
        customRegularFuncMap.put(Tokens.ISREADONLYSESSION,
                                 FUNC_ISREADONLYSESSION);
        customRegularFuncMap.put(Tokens.ISREADONLYDATABASE,
                                 FUNC_ISREADONLYDATABASE);
        customRegularFuncMap.put(Tokens.ISREADONLYDATABASEFILES,
                                 FUNC_ISREADONLYDATABASEFILES);

        //
        customRegularFuncMap.put(Tokens.ACOS, FUNC_ACOS);
        customRegularFuncMap.put(Tokens.ASIN, FUNC_ASIN);
        customRegularFuncMap.put(Tokens.ATAN, FUNC_ATAN);
        customRegularFuncMap.put(Tokens.ATAN2, FUNC_ATAN2);
        customRegularFuncMap.put(Tokens.COS, FUNC_COS);
        customRegularFuncMap.put(Tokens.COT, FUNC_COT);
        customRegularFuncMap.put(Tokens.DEGREES, FUNC_DEGREES);
        customRegularFuncMap.put(Tokens.LOG10, FUNC_LOG10);
        customRegularFuncMap.put(Tokens.PI, FUNC_PI);
        customRegularFuncMap.put(Tokens.RADIANS, FUNC_RADIANS);
        customRegularFuncMap.put(Tokens.RAND, FUNC_RAND);
        customRegularFuncMap.put(Tokens.ROUND, FUNC_ROUND);
        customRegularFuncMap.put(Tokens.SIGN, FUNC_SIGN);
        customRegularFuncMap.put(Tokens.SIN, FUNC_SIN);
        customRegularFuncMap.put(Tokens.TAN, FUNC_TAN);
        customRegularFuncMap.put(Tokens.BITAND, FUNC_BITAND);
        customRegularFuncMap.put(Tokens.BITOR, FUNC_BITOR);
        customRegularFuncMap.put(Tokens.BITXOR, FUNC_BITXOR);
        customRegularFuncMap.put(Tokens.ROUNDMAGIC, FUNC_ROUNDMAGIC);
        customRegularFuncMap.put(Tokens.ASCII, FUNC_ASCII);
        customRegularFuncMap.put(Tokens.CHAR, FUNC_CHAR);
        customRegularFuncMap.put(Tokens.CONCAT_WORD, FUNC_CONCAT);
        customRegularFuncMap.put(Tokens.DIFFERENCE, FUNC_DIFFERENCE);
        customRegularFuncMap.put(Tokens.HEXTORAW, FUNC_HEXTORAW);
        customRegularFuncMap.put(Tokens.RAWTOHEX, FUNC_RAWTOHEX);
        customRegularFuncMap.put(Tokens.REPEAT, FUNC_REPEAT);
        customRegularFuncMap.put(Tokens.REPLACE, FUNC_REPLACE);
        customRegularFuncMap.put(Tokens.RIGHT, FUNC_RIGHT);
        customRegularFuncMap.put(Tokens.SOUNDEX, FUNC_SOUNDEX);
        customRegularFuncMap.put(Tokens.SPACE, FUNC_SPACE);
        customRegularFuncMap.put(Tokens.DATEDIFF, FUNC_DATEDIFF);
    }

    static final IntKeyIntValueHashMap customValueFuncMap =
        new IntKeyIntValueHashMap();

    static {
        customValueFuncMap.put(Tokens.SYSDATE, FUNC_SYSDATE);
        customValueFuncMap.put(Tokens.TODAY, FUNC_CURRENT_DATE);
        customValueFuncMap.put(Tokens.NOW, FUNC_CURRENT_TIMESTAMP);
    }

    private int extractSpec;

    public static FunctionSQL newCustomFunction(String token, int tokenType) {

        int id = customRegularFuncMap.get(tokenType, -1);

        if (id == -1) {
            id = customValueFuncMap.get(tokenType, -1);
        }

        if (id == -1) {
            return null;
        }

        switch (tokenType) {

            case Tokens.LN :
            case Tokens.LCASE :
            case Tokens.UCASE :
            case Tokens.LENGTH :
            case Tokens.BITLENGTH :
            case Tokens.OCTETLENGTH :
            case Tokens.TODAY :
            case Tokens.NOW :
                return new FunctionSQL(id);

            case Tokens.CURDATE :
            case Tokens.CURTIME : {
                FunctionSQL function = new FunctionSQL(id);

                function.parseList = emptyParamList;

                return function;
            }
            case Tokens.SUBSTR : {
                FunctionSQL function = new FunctionSQL(id);

                function.parseList = tripleParamList;
                // A VoltDB extension -- to make the third parameter optional
                function.parseListAlt = doubleParamList;
                // End of VoltDB extension

                return function;
            }
        }

        FunctionCustom function = new FunctionCustom(id);

        if (id == FUNC_TRIM_CHAR) {
            switch (tokenType) {

                case Tokens.LTRIM :
                    function.extractSpec = Tokens.LEADING;
                    break;

                case Tokens.RTRIM :
                    function.extractSpec = Tokens.TRAILING;
                    break;
            }
        }

        if (id == FUNC_EXTRACT) {
            switch (tokenType) {

                case Tokens.DAYNAME :
                    function.extractSpec = Tokens.DAY_OF_WEEK;
                    // A VoltDB extension to customize the SQL function set support
                    function.voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_FACTORY_METHOD;
                    // End of VoltDB extension
                    break;

                case Tokens.NONTHNAME :
                    function.extractSpec = Tokens.MONTH_NAME;
                    // A VoltDB extension to customize the SQL function set support
                    function.voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_FACTORY_METHOD;
                    // End of VoltDB extension
                    break;

                 // A VoltDB extension to customize the SQL function set support
                case Tokens.WEEK :
                case Tokens.WEEKOFYEAR:
                	function.extractSpec = Tokens.WEEK_OF_YEAR;
                	break;
                // case Tokens.WEEKDAY is handled by default case
                // End of VoltDB extension
                case Tokens.DAYOFMONTH :
                    function.extractSpec = Tokens.DAY_OF_MONTH;
                    break;

                case Tokens.DAYOFWEEK :
                    function.extractSpec = Tokens.DAY_OF_WEEK;
                    break;

                case Tokens.DAYOFYEAR :
                    function.extractSpec = Tokens.DAY_OF_YEAR;
                    break;

                default :
                    function.extractSpec = tokenType;
            }
        }

        if (function.name == null) {
            function.name = token;
        }

        return function;
    }

    public static boolean isRegularFunction(int tokenType) {
        return customRegularFuncMap.get(tokenType, -1) != -1;
    }

    public static boolean isValueFunction(int tokenType) {
        return customValueFuncMap.get(tokenType, -1) != -1;
    }

    private FunctionCustom(int id) {

        super();

        this.funcType = id;

        switch (id) {

            case FUNC_CONCAT :
            	// A VoltDB extension to let CONCAT support more than 2 parameters
            	// this line should be never called because volt check FunctionForVoltDB first
            	voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
            	break;
            	// End of VoltDB extension
            case FUNC_LEFT :
                parseList = doubleParamList;
                break;

            case FUNC_DATABASE :
                parseList = emptyParamList;
                break;

            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISREADONLYSESSION :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
                parseList = emptyParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_EXTRACT :
                name      = Tokens.T_EXTRACT;
                parseList = singleParamList;
                break;

            case FUNC_TRIM_CHAR :
                name      = Tokens.T_TRIM;
                parseList = singleParamList;
                break;

            case FUNC_OVERLAY_CHAR :
                name      = Tokens.T_OVERLAY;
                parseList = quadParamList;
                break;

            case FUNC_IDENTITY :
                name      = Tokens.T_IDENTITY;
                parseList = emptyParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_SYSDATE :
                name      = Tokens.T_SYSDATE;
                parseList = noParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_TIMESTAMPADD :
                name      = Tokens.T_TIMESTAMPADD;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 9,
                    Tokens.SQL_TSI_FRAC_SECOND, Tokens.SQL_TSI_SECOND,
                    Tokens.SQL_TSI_MINUTE, Tokens.SQL_TSI_HOUR,
                    Tokens.SQL_TSI_DAY, Tokens.SQL_TSI_WEEK,
                    Tokens.SQL_TSI_MONTH, Tokens.SQL_TSI_QUARTER,
                    Tokens.SQL_TSI_YEAR, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_TIMESTAMPDIFF :
                name      = Tokens.T_TIMESTAMPDIFF;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.X_KEYSET, 9,
                    Tokens.SQL_TSI_FRAC_SECOND, Tokens.SQL_TSI_SECOND,
                    Tokens.SQL_TSI_MINUTE, Tokens.SQL_TSI_HOUR,
                    Tokens.SQL_TSI_DAY, Tokens.SQL_TSI_WEEK,
                    Tokens.SQL_TSI_MONTH, Tokens.SQL_TSI_QUARTER,
                    Tokens.SQL_TSI_YEAR, Tokens.COMMA, Tokens.QUESTION,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_TRUNCATE :
                parseList = doubleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_TO_CHAR :
                parseList = doubleParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_TIMESTAMP :
                name      = Tokens.T_TIMESTAMP;
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.X_OPTION, 2,
                    Tokens.COMMA, Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_PI :
                parseList = emptyParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_RAND :
                parseList = optionalIntegerParamList;
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_ACOS :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_ATAN2 :
            case FUNC_COS :
            case FUNC_COT :
            case FUNC_DEGREES :
            case FUNC_SIN :
            case FUNC_TAN :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_ROUNDMAGIC :
            case FUNC_SIGN :
            case FUNC_SOUNDEX :
            case FUNC_ASCII :
            case FUNC_HEXTORAW :
            case FUNC_RAWTOHEX :
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // $FALL-THROUGH$
                // End of VoltDB extension
            case FUNC_CHAR :
            case FUNC_SPACE :
                parseList = singleParamList;
                break;

            case FUNC_ROUND :
            case FUNC_BITAND :
            case FUNC_BITOR :
            case FUNC_BITXOR :
            case FUNC_DIFFERENCE :
            // A VoltDB extension to customize the SQL function set support
            case FUNC_DATEDIFF :
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // $FALL-THROUGH$
            case FUNC_REPEAT :
            /* disable 2 lines ...
            case FUNC_REPEAT :
            case FUNC_DATEDIFF :
            ... disabled 2 lines */
            // End of VoltDB extension
            case FUNC_RIGHT :
                parseList = doubleParamList;
                break;

            case FUNC_LOCATE :
                parseList = new short[] {
                    Tokens.OPENBRACKET, Tokens.QUESTION, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.X_OPTION, 2, Tokens.COMMA,
                    Tokens.QUESTION, Tokens.CLOSEBRACKET
                };
                // A VoltDB extension to customize the SQL function set support
                voltDisabled = DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR;
                // End of VoltDB extension
                break;

            case FUNC_REPLACE :
                parseList = tripleParamList;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "SQLFunction");
        }
    }

    public void setArguments(Expression[] nodes) {

        switch (funcType) {

            case FUNC_OVERLAY_CHAR : {
                Expression start  = nodes[1];
                Expression length = nodes[2];

                nodes[1] = nodes[3];
                nodes[2] = start;
                nodes[3] = length;

                break;
            }
            case FUNC_EXTRACT : {
                Expression[] newNodes = new Expression[2];

                newNodes[0] =
                    new ExpressionValue(ValuePool.getInt(extractSpec),
                                        Type.SQL_INTEGER);
                newNodes[1] = nodes[0];
                nodes       = newNodes;

                break;
            }
            case FUNC_TRIM_CHAR : {
                Expression[] newNodes = new Expression[3];

                newNodes[0] =
                    new ExpressionValue(ValuePool.getInt(extractSpec),
                                        Type.SQL_INTEGER);
                newNodes[1] = new ExpressionValue(" ", Type.SQL_CHAR);
                newNodes[2] = nodes[0];
                nodes       = newNodes;
            }
        }

        super.setArguments(nodes);
    }

    public Expression getFunctionExpression() {

        switch (funcType) {

            case FUNC_SYSDATE : {
                FunctionSQL f = new FunctionSQL(FUNC_CURRENT_TIMESTAMP);

                f.nodes = new Expression[]{
                    new ExpressionValue(ValuePool.INTEGER_0,
                                        Type.SQL_INTEGER) };

                return f;
            }
            case FUNC_CONCAT :
                return new ExpressionArithmetic(OpTypes.CONCAT,
                                                nodes[Expression.LEFT],
                                                nodes[Expression.RIGHT]);
        }

        return super.getFunctionExpression();
    }

    Object getValue(Session session, Object[] data) {

        switch (funcType) {

            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                return super.getValue(session, data);

            case FUNC_DATABASE :
                return session.getDatabase().getPath();

            case FUNC_ISAUTOCOMMIT :
                return session.isAutoCommit() ? Boolean.TRUE
                                              : Boolean.FALSE;

            case FUNC_ISREADONLYSESSION :
                return session.isReadOnlyDefault() ? Boolean.TRUE
                                                   : Boolean.FALSE;

            case FUNC_ISREADONLYDATABASE :
                return session.getDatabase().databaseReadOnly ? Boolean.TRUE
                                                              : Boolean.FALSE;

            case FUNC_ISREADONLYDATABASEFILES :
                return session.getDatabase().isFilesReadOnly() ? Boolean.TRUE
                                                               : Boolean.FALSE;

            case FUNC_IDENTITY : {
                Number id = session.getLastIdentity();

                if (id instanceof Long) {
                    return id;
                } else {
                    return ValuePool.getLong(id.longValue());
                }
            }
            case FUNC_TIMESTAMPADD : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                int           part = ((Number) nodes[0].valueData).intValue();
                long          units  = ((Number) data[1]).longValue();
                TimestampData source = (TimestampData) data[2];
                IntervalType  t;
                Object        o;

                switch (part) {

                    case Tokens.SQL_TSI_FRAC_SECOND : {
                        long seconds = units / DTIType.limitNanoseconds;
                        int  nanos = (int) (units % DTIType.limitNanoseconds);

                        t = Type.SQL_INTERVAL_SECOND_MAX_FRACTION;
                        o = new IntervalSecondData(seconds, nanos, t);

                        return dataType.add(source, o, t);
                    }
                    case Tokens.SQL_TSI_SECOND :
                        t = Type.SQL_INTERVAL_SECOND;
                        o = IntervalSecondData.newIntervalSeconds(units, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE;
                        o = IntervalSecondData.newIntervalMinute(units, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR;
                        o = IntervalSecondData.newIntervalHour(units, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY;
                        o = IntervalSecondData.newIntervalDay(units, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY;
                        o = IntervalSecondData.newIntervalDay(units * 7, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH;
                        o = IntervalMonthData.newIntervalMonth(units, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH;
                        o = IntervalMonthData.newIntervalMonth(units * 3, t);

                        return dataType.add(source, o, t);

                    case Tokens.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR;
                        o = IntervalMonthData.newIntervalMonth(units, t);

                        return dataType.add(source, o, t);

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionCustom");
                }
            }
            case FUNC_TIMESTAMPDIFF : {
                if (data[1] == null || data[2] == null) {
                    return null;
                }

                int           part = ((Number) nodes[0].valueData).intValue();
                TimestampData a    = (TimestampData) data[2];
                TimestampData b    = (TimestampData) data[1];

                if (nodes[2].dataType.isDateTimeTypeWithZone()) {
                    a = (TimestampData) Type.SQL_TIMESTAMP.convertToType(
                        session, a, Type.SQL_TIMESTAMP_WITH_TIME_ZONE);
                }

                if (nodes[1].dataType.isDateTimeTypeWithZone()) {
                    b = (TimestampData) Type.SQL_TIMESTAMP.convertToType(
                        session, b, Type.SQL_TIMESTAMP_WITH_TIME_ZONE);
                }

                IntervalType t;

                switch (part) {

                    case Tokens.SQL_TSI_FRAC_SECOND :
                        t = Type.SQL_INTERVAL_SECOND_MAX_PRECISION;

                        IntervalSecondData interval =
                            (IntervalSecondData) t.subtract(a, b, null);

                        return new Long(
                            DTIType.limitNanoseconds * interval.getSeconds()
                            + interval.getNanos());

                    case Tokens.SQL_TSI_SECOND :
                        t = Type.SQL_INTERVAL_SECOND_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    case Tokens.SQL_TSI_MINUTE :
                        t = Type.SQL_INTERVAL_MINUTE_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    case Tokens.SQL_TSI_HOUR :
                        t = Type.SQL_INTERVAL_HOUR_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    case Tokens.SQL_TSI_DAY :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    case Tokens.SQL_TSI_WEEK :
                        t = Type.SQL_INTERVAL_DAY_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b, null))
                                        / 7);

                    case Tokens.SQL_TSI_MONTH :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    case Tokens.SQL_TSI_QUARTER :
                        t = Type.SQL_INTERVAL_MONTH_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b, null))
                                        / 3);

                    case Tokens.SQL_TSI_YEAR :
                        t = Type.SQL_INTERVAL_YEAR_MAX_PRECISION;

                        return new Long(t.convertToLong(t.subtract(a, b,
                                null)));

                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500,
                                                 "FunctionCustom");
                }
            }
            case FUNC_SECONDS_MIDNIGHT : {
                if (data[0] == null) {
                    return null;
                }
            }

            // $FALL-THROUGH$
            case FUNC_TRUNCATE : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                return ((NumberType) dataType).truncate(data[0],
                        ((Number) data[1]).intValue());
            }
            case FUNC_TO_CHAR : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                SimpleDateFormat format = session.getSimpleDateFormatGMT();
                String javaPattern =
                    HsqlDateTime.toJavaDatePattern((String) data[1]);

                try {
                    format.applyPattern(javaPattern);
                } catch (Exception e) {
                    throw Error.error(ErrorCode.X_22511);
                }

                Date date =
                    (Date) ((DateTimeType) nodes[0].dataType)
                        .convertSQLToJavaGMT(session, data[0]);

                return format.format(date);
            }
            case FUNC_TIMESTAMP : {
                boolean unary = nodes[1] == null;

                if (data[0] == null) {
                    return null;
                }

                if (unary) {
                    return Type.SQL_TIMESTAMP.convertToType(session, data[0],
                            nodes[0].dataType);
                }

                if (data[1] == null) {
                    return null;
                }

                TimestampData date =
                    (TimestampData) Type.SQL_DATE.convertToType(session,
                        data[0], nodes[0].dataType);
                TimeData time = (TimeData) Type.SQL_TIME.convertToType(session,
                    data[1], nodes[1].dataType);

                return new TimestampData(date.getSeconds()
                                         + time.getSeconds(), time.getNanos());
            }
            case FUNC_PI :
                return Double.valueOf(Math.PI);

            case FUNC_RAND : {
                if (nodes[0] == null) {
                    return Double.valueOf(session.random());
                } else {
                    long seed = ((Number) data[0]).longValue();

                    return Double.valueOf(seed);
                }
            }
            case FUNC_ACOS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.acos(d));
            }
            case FUNC_ASIN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.asin(d));
            }
            case FUNC_ATAN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.atan(d));
            }
            case FUNC_COS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.cos(d));
            }
            case FUNC_COT : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);
                double c = 1.0 / java.lang.Math.tan(d);

                return Double.valueOf(c);
            }
            case FUNC_DEGREES : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.toDegrees(d));
            }
            case FUNC_SIN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.sin(d));
            }
            case FUNC_TAN : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.tan(d));
            }
            case FUNC_LOG10 : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.log10(d));
            }
            case FUNC_RADIANS : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Double.valueOf(java.lang.Math.toRadians(d));
            }

            //
            case FUNC_SIGN : {
                if (data[0] == null) {
                    return null;
                }

                return ((NumberType) nodes[0].dataType).compareToZero(data[0]);
            }
            case FUNC_ATAN2 : {
                if (data[0] == null) {
                    return null;
                }

                double a = NumberType.toDouble(data[0]);
                double b = NumberType.toDouble(data[1]);

                return Double.valueOf(java.lang.Math.atan2(a, b));
            }
            case FUNC_ASCII : {
                String arg;

                if (data[0] == null) {
                    return null;
                }

                if (nodes[0].dataType.isLobType()) {
                    arg = ((ClobData) data[0]).getSubString(session, 0, 1);
                } else {
                    arg = (String) data[0];
                }

                if (arg.length() == 0) {
                    return null;
                }

                return ValuePool.getInt(arg.charAt(0));
            }
            case FUNC_CHAR :
                if (data[0] == null) {
                    return null;
                }

                int arg = ((Number) data[0]).intValue();

                return String.valueOf(arg);

            case FUNC_ROUND : {
                if (data[0] == null || data[1] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);
                int    i = ((Number) data[1]).intValue();

                return Library.round(d, i);
            }
            case FUNC_ROUNDMAGIC : {
                if (data[0] == null) {
                    return null;
                }

                double d = NumberType.toDouble(data[0]);

                return Library.roundMagic(d);
            }
            case FUNC_SOUNDEX : {
                if (data[0] == null) {
                    return null;
                }

                String s = (String) data[0];

                return Library.soundex(s);
            }
            case FUNC_BITAND :
            case FUNC_BITOR :
            case FUNC_BITXOR : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                if (nodes[0].dataType.isIntegralType()) {
                    int v = 0;
                    int a = ((Number) data[0]).intValue();
                    int b = ((Number) data[0]).intValue();

                    switch (funcType) {

                        case FUNC_BITAND :
                            v = a & b;
                            break;

                        case FUNC_BITOR :
                            v = a | b;
                            break;

                        case FUNC_BITXOR :
                            v = a ^ b;
                            break;
                    }

                    return ValuePool.getInt(v);
                } else {

                    /** @todo - for binary */
                    return null;
                }
            }
            case FUNC_DIFFERENCE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                int v = Library.difference((String) data[0], (String) data[1]);

                return ValuePool.getInt(v);
            }
            case FUNC_HEXTORAW : {
                if (data[0] == null) {
                    return null;
                }

                return dataType.convertToType(session, data[0],
                                              nodes[0].dataType);
            }
            case FUNC_RAWTOHEX : {
                if (data[0] == null) {
                    return null;
                }

                return nodes[0].dataType.convertToString(data[0]);
            }
            case FUNC_LOCATE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                int v = Library.locate((String) data[0], (String) data[1],
                                       (Integer) data[2]);

                return ValuePool.getInt(v);
            }
            case FUNC_REPEAT : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                return Library.repeat((String) data[0],
                                      ((Number) data[1]).intValue());
            }
            case FUNC_REPLACE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                return Library.replace((String) data[0], (String) data[1],
                                       (String) data[2]);
            }
            case FUNC_LEFT :
            case FUNC_RIGHT : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                int count = ((Number) data[1]).intValue();

                return ((CharacterType) dataType).substring(session, data[0],
                        0, count, true, funcType == FUNC_RIGHT);
            }
            case FUNC_SPACE : {
                for (int i = 0; i < data.length; i++) {
                    if (data[0] == null) {
                        return null;
                    }
                }

                int count = ((Number) data[0]).intValue();

                return ValuePool.getSpaces(count);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        switch (funcType) {

            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                super.resolveTypes(session, parent);

                return;

            case FUNC_DATABASE :
                dataType = Type.SQL_VARCHAR;

                return;

            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISREADONLYSESSION :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
                dataType = Type.SQL_BOOLEAN;

                return;

            case FUNC_IDENTITY :
                dataType = Type.SQL_BIGINT;

                return;

            case FUNC_TIMESTAMPADD :
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_BIGINT;
                }

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (!nodes[1].dataType.isIntegralType()
                        || nodes[2].dataType.typeCode != Types.SQL_TIMESTAMP) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = nodes[2].dataType;

                return;

            case FUNC_DATEDIFF : {
                int part;

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if ("yy".equalsIgnoreCase((String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_YEAR;
                } else if ("mm".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_MONTH;
                } else if ("dd".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_DAY;
                } else if ("hh".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_HOUR;
                } else if ("mi".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_MINUTE;
                } else if ("ss".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_SECOND;
                } else if ("ms".equalsIgnoreCase(
                        (String) nodes[0].valueData)) {
                    part = Tokens.SQL_TSI_FRAC_SECOND;
                } else {
                    throw Error.error(ErrorCode.X_42561);
                }

                nodes[0].valueData = ValuePool.getInt(part);
                funcType           = FUNC_TIMESTAMPDIFF;
            }

            // $FALL-THROUGH$
            case FUNC_TIMESTAMPDIFF : {
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = Type.SQL_TIMESTAMP;
                }

                if (nodes[1].dataType.typeCode != Types.SQL_TIMESTAMP
                        && nodes[1].dataType.typeCode
                           != Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[2].dataType.typeCode != Types.SQL_TIMESTAMP
                        && nodes[2].dataType.typeCode
                           != Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_BIGINT;

                return;
            }
            case FUNC_TRUNCATE : {
                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                } else if (!nodes[1].dataType.isIntegralType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                dataType = nodes[0].dataType;

                return;
            }
            case FUNC_TO_CHAR : {
                if (nodes[0].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (nodes[1].dataType == null
                        || !nodes[1].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42567);
                }

                if (!nodes[0].dataType.isExactNumberType()
                        && !nodes[0].dataType.isDateTimeType()) {
                    throw Error.error(ErrorCode.X_42565);
                }

                // fixed maximum as format is a variable
                dataType = CharacterType.getCharacterType(Types.SQL_VARCHAR,
                        40);

                if (nodes[1].opType == OpTypes.VALUE) {
                    nodes[1].setAsConstantValue(session);
                }

                return;
            }
            case FUNC_TIMESTAMP : {
                boolean unary = nodes[1] == null;

                if (unary) {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_TIMESTAMP;
                    }

                    if (nodes[0].dataType.isCharacterType()) {}
                    else {
                        if (nodes[0].dataType.typeCode != Types.SQL_TIMESTAMP
                                && nodes[0].dataType.typeCode
                                   != Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
                            throw Error.error(ErrorCode.X_42561);
                        }
                    }
                } else {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_DATE;
                    }

                    if (nodes[1].dataType == null) {
                        nodes[1].dataType = Type.SQL_TIME;
                    }

                    if (nodes[0].dataType.typeCode != Types.SQL_DATE
                            && nodes[0].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }

                    if (nodes[1].dataType.typeCode != Types.SQL_TIME
                            && nodes[1].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_TIMESTAMP;

                return;
            }
            case FUNC_PI :
                dataType = Type.SQL_DOUBLE;
                break;

            case FUNC_RAND : {
                if (nodes[0] != null) {
                    if (nodes[0].dataType == null) {
                        nodes[0].dataType = Type.SQL_BIGINT;
                    } else if (!nodes[0].dataType.isExactNumberType()) {
                        throw Error.error(ErrorCode.X_42565);
                    }
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_ROUND :
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[1].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

            // $FALL-THROUGH$
            case FUNC_ACOS :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_COS :
            case FUNC_COT :
            case FUNC_DEGREES :
            case FUNC_SIN :
            case FUNC_TAN :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_ROUNDMAGIC : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_SIGN : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_ATAN2 : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_DOUBLE;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_DOUBLE;
                }

                if (!nodes[0].dataType.isNumberType()
                        || !nodes[1].dataType.isNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_DOUBLE;

                break;
            }
            case FUNC_SOUNDEX : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.getType(Types.SQL_VARCHAR, 0, 4, 0);

                break;
            }
            case FUNC_BITAND :
            case FUNC_BITOR :
            case FUNC_BITXOR : {
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        nodes[i].dataType = Type.SQL_INTEGER;
                    } else if (nodes[i].dataType.typeCode
                               != Types.SQL_INTEGER) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_ASCII : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_CHAR : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[0].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.getType(Types.SQL_VARCHAR, 0, 1, 0);

                break;
            }
            case FUNC_DIFFERENCE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                dataType = Type.SQL_INTEGER;

                break;
            }
            case FUNC_HEXTORAW : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_VARBINARY;

                break;
            }
            case FUNC_RAWTOHEX : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARBINARY;
                }

                if (!nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_VARCHAR;

                break;
            }
            case FUNC_LOCATE : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_VARCHAR;
                }

                if (nodes[2] == null) {
                    nodes[2] = new ExpressionValue(ValuePool.INTEGER_0,
                                                   Type.SQL_INTEGER);
                }

                if (nodes[2].dataType == null) {
                    nodes[2].dataType = Type.SQL_INTEGER;
                }

                boolean isChar = nodes[0].dataType.isCharacterType()
                                 && nodes[1].dataType.isCharacterType();

                if (!isChar || !nodes[2].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_INTEGER;;

                break;
            }
            case FUNC_REPEAT : {
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                boolean isChar = nodes[0].dataType.isCharacterType();

                if (!isChar && !nodes[0].dataType.isBinaryType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                // A VoltDB extension to customize the SQL function set support
                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }
                // End of VoltDB extension
                dataType = isChar ? Type.SQL_VARCHAR
                                  : Type.SQL_VARBINARY;;

                break;
            }
            case FUNC_REPLACE : {
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        nodes[i].dataType = Type.SQL_VARCHAR;
                    } else if (!nodes[i].dataType.isCharacterType()) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }

                dataType = Type.SQL_VARCHAR;

                break;
            }
            case FUNC_LEFT :
            case FUNC_RIGHT :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_VARCHAR;
                }

                if (!nodes[0].dataType.isCharacterType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                if (nodes[1].dataType == null) {
                    nodes[1].dataType = Type.SQL_INTEGER;
                }

                if (!nodes[1].dataType.isExactNumberType()) {
                    throw Error.error(ErrorCode.X_42561);
                }

                dataType = Type.SQL_VARCHAR;
                break;

            case FUNC_SPACE :
                if (nodes[0].dataType == null) {
                    nodes[0].dataType = Type.SQL_INTEGER;
                }

                dataType = Type.SQL_VARCHAR;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "FunctionCustom");
        }
    }

    public String getSQL() {

        switch (funcType) {

            case FUNC_EXTRACT :
            case FUNC_TRIM_CHAR :
            case FUNC_OVERLAY_CHAR :
                return super.getSQL();

            case FUNC_DATABASE :
            case FUNC_ISAUTOCOMMIT :
            case FUNC_ISREADONLYSESSION :
            case FUNC_ISREADONLYDATABASE :
            case FUNC_ISREADONLYDATABASEFILES :
                return new StringBuffer(name).append(
                    Tokens.T_OPENBRACKET).append(
                    Tokens.T_CLOSEBRACKET).toString();

            case FUNC_IDENTITY :
                return new StringBuffer(Tokens.T_IDENTITY).append(
                    Tokens.T_OPENBRACKET).append(
                    Tokens.T_CLOSEBRACKET).toString();

            case FUNC_TIMESTAMPADD :
                return new StringBuffer(Tokens.T_TIMESTAMPADD).append(
                    Tokens.T_OPENBRACKET).append(nodes[0].getSQL())       //
                    .append(Tokens.T_COMMA).append(nodes[1].getSQL())     //
                    .append(Tokens.T_COMMA).append(nodes[2].getSQL())     //
                    .append(Tokens.T_CLOSEBRACKET).toString();

            case FUNC_TIMESTAMPDIFF :
                return new StringBuffer(Tokens.T_TIMESTAMPDIFF).append(
                    Tokens.T_OPENBRACKET).append(nodes[0].getSQL())       //
                    .append(Tokens.T_COMMA).append(nodes[1].getSQL())     //
                    .append(Tokens.T_COMMA).append(nodes[2].getSQL())     //
                    .append(Tokens.T_CLOSEBRACKET).toString();

            case FUNC_TRUNCATE : {
                return new StringBuffer(Tokens.T_TRUNCATE).append('(')    //
                    .append(nodes[0].getSQL()).append(Tokens.T_COMMA)     //
                    .append(nodes[1].getSQL()).append(')').toString();
            }
            case FUNC_TO_CHAR : {
                return new StringBuffer(Tokens.T_TO_CHAR).append('(')     //
                    .append(nodes[0].getSQL()).append(Tokens.T_COMMA)     //
                    .append(nodes[1].getSQL()).append(')').toString();
            }
            case FUNC_PI :
            case FUNC_RAND : {
                return new StringBuffer(name).append('(').append(
                    ')').toString();
            }
            case FUNC_ACOS :
            case FUNC_ASIN :
            case FUNC_ATAN :
            case FUNC_ATAN2 :
            case FUNC_COS :
            case FUNC_COT :
            case FUNC_DEGREES :
            case FUNC_SIN :
            case FUNC_TAN :
            case FUNC_LOG10 :
            case FUNC_RADIANS :
            case FUNC_ROUNDMAGIC :
            case FUNC_SIGN : {
                return new StringBuffer(name).append('(')                 //
                    .append(nodes[0].getSQL()).append(')').toString();
            }
            case FUNC_ROUND : {
                return new StringBuffer(Tokens.ROUND).append('(')         //
                    .append(nodes[0].getSQL()).append(Tokens.T_COMMA)     //
                    .append(nodes[1].getSQL()).append(')').toString();
            }
            default :
                return super.getSQL();
        }
    }

    /************************* Volt DB Extensions *************************/

    public static final String FUNC_CONCAT_ID_STRING = String.valueOf(FunctionCustom.FUNC_CONCAT);

    private static String DISABLED_IN_FUNCTIONCUSTOM_CONSTRUCTOR = "Custom Function";
    private static String DISABLED_IN_FUNCTIONCUSTOM_FACTORY_METHOD = "Custom Function Special Case";
    /**********************************************************************/
}
