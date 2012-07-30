/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "expressions/functionexpression.h"
#include "expressions/expressionutil.h"

namespace voltdb {

/** implement a forced SQL ERROR function (for test and example purposes) for either integer or string types **/
template<> inline NValue NValue::callUnary<FUNC_VOLT_SQL_ERROR>() const {
    int64_t intValue = -1;
    char buffer[1024];
    const char* msgcode;
    const char* msgtext;
    const ValueType type = getValueType();
    if (type == VALUE_TYPE_VARCHAR) {
        const int32_t valueLength = getObjectLength();
        const char *valueChars = reinterpret_cast<char*>(getObjectValue());
        snprintf(buffer, std::min((int32_t)sizeof(buffer), valueLength+1), "%s", valueChars);
        msgcode = SQLException::nonspecific_error_code_for_error_forced_by_user;
        msgtext = buffer;
    } else {
        intValue = castAsBigIntAndGetValue(); // let cast throw if invalid
        snprintf(buffer, sizeof(buffer), "%05ld", (long) intValue);
        msgcode = buffer;
        msgtext = SQLException::specific_error_specified_by_user;
    }
    if (intValue != 0) {
        throw SQLException(msgcode, msgtext);
    }
    return *this;
}

/** implement the 2-argument forced SQL ERROR function (for test and example purposes) */
template<> inline NValue NValue::call<FUNC_VOLT_SQL_ERROR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    int64_t intValue = -1;
    char buffer[1024];
    char buffer2[1024];
    const char* msgcode;
    const char* msgtext;

    const NValue& codeArg = arguments[0];
    if (codeArg.isNull()) {
        msgcode = SQLException::nonspecific_error_code_for_error_forced_by_user;
    } else {
        intValue = codeArg.castAsBigIntAndGetValue(); // let cast throw if invalid
        snprintf(buffer, sizeof(buffer), "%05ld", (long) intValue);
        msgcode = buffer;
    }

    const NValue& strValue = arguments[1];
    if (strValue.isNull()) {
        msgtext = "";
    } else {
        if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
        }

        const int32_t valueLength = strValue.getObjectLength();
        char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
        snprintf(buffer2, std::min((int32_t)sizeof(buffer2), valueLength+1), "%s", valueChars);
        msgtext = buffer2;
    }
    if (intValue != 0) {
        throw SQLException(msgcode, msgtext);
    }
    return codeArg;
}

namespace functionexpression {

/*
 * Constant (no parameter) function. (now, random)
 */
template <int F>
class ConstantFunctionExpression : public AbstractExpression {
public:
    ConstantFunctionExpression(const std::string& sqlName, const std::string& uniqueName)
        : AbstractExpression(EXPRESSION_TYPE_FUNCTION) {
    };

    NValue eval(const TableTuple *, const TableTuple *) const {
        return NValue::callConstant<F>();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ConstantFunctionExpression " + expressionToString(getExpressionType()));
    }
};

/*
 * Unary functions. (abs, upper, lower)
 */

template <int F>
class UnaryFunctionExpression : public AbstractExpression {
    AbstractExpression * const m_child;
public:
    UnaryFunctionExpression(AbstractExpression *child)
        : AbstractExpression(EXPRESSION_TYPE_FUNCTION)
        , m_child(child) {
    }

    virtual ~UnaryFunctionExpression() {
        delete m_child;
    }

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_child);
        return (m_child->eval(tuple1, tuple2)).callUnary<F>();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "UnaryFunctionExpression " + expressionToString(getExpressionType()));
    }
};

/*
 * N-ary functions.
 */
template <int F>
class GeneralFunctionExpression : public AbstractExpression {
public:
    GeneralFunctionExpression(const std::vector<AbstractExpression *>& args)
        : AbstractExpression(EXPRESSION_TYPE_FUNCTION), m_args(args) {}

    virtual ~GeneralFunctionExpression() {
        size_t i = m_args.size();
        while (i--) {
            delete m_args[i];
        }
        delete &m_args;
    }

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        //TODO: Could make this vector a member, if the memory management implications
        // (of the NValue internal state) were clear -- is there a penalty for longer-lived
        // NValues that outweighs the current per-eval allocation penalty?
        std::vector<NValue> nValue(m_args.size());
        for (int i = 0; i < m_args.size(); ++i) {
            nValue[i] = m_args[i]->eval(tuple1, tuple2);
        }
        return NValue::call<F>(nValue);
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "GeneralFunctionExpression " + expressionToString(getExpressionType()));
    }

private:
    const std::vector<AbstractExpression *>& m_args;
};

}

using namespace functionexpression;


/** implement a forced SQL ERROR function (for test and example purposes) for either integer or string types **/
template<> inline NValue NValue::callUnary<EXPRESSION_TYPE_FUNCTION_SQL_ERROR>() const {
    int64_t intValue = -1;
    char buffer[1024];
    const char* msgcode;
    const char* msgtext;
    const ValueType type = getValueType();
    if (type == VALUE_TYPE_VARCHAR) {
        const int32_t valueUTF8Length = getObjectLength();
        const char *valueChars = reinterpret_cast<char*>(getObjectValue());
        snprintf(buffer, std::min((int32_t)sizeof(buffer), valueUTF8Length), "%s", valueChars);
        msgcode = SQLException::nonspecific_error_code_for_error_forced_by_user;
        msgtext = buffer;
    } else {
        intValue = castAsBigIntAndGetValue(); // let cast throw if invalid
        snprintf(buffer, sizeof(buffer), "%ld", (long) intValue);
        msgcode = buffer;
        msgtext = SQLException::specific_error_specified_by_user;
    }
    if (intValue != 0) {
        throw SQLException(msgcode, msgtext);
    }
    return *this;
}

/** implement the 2-argument forced SQL ERROR function (for test and example purposes) */
template<> inline NValue NValue::call<EXPRESSION_TYPE_FUNCTION_SQL_ERROR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    int64_t intValue = -1;
    char buffer[1024];
    char buffer2[1024];
    const char* msgcode;
    const char* msgtext;

    const NValue& codeArg = arguments[0];
    if (codeArg.isNull()) {
        msgcode = SQLException::nonspecific_error_code_for_error_forced_by_user;
    } else {
        intValue = codeArg.castAsBigIntAndGetValue(); // let cast throw if invalid
        snprintf(buffer, sizeof(buffer), "%ld", (long) intValue);
        msgcode = buffer;
    }

    const NValue& strValue = arguments[1];
    if (strValue.isNull()) {
        msgtext = "";
    } else {
        if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
        }

        const int32_t valueUTF8Length = strValue.getObjectLength();
        char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
        snprintf(buffer2, std::min((int32_t)sizeof(buffer), valueUTF8Length), "%s", valueChars);
        msgtext = buffer2;
    }
    if (intValue != 0) {
        throw SQLException(msgcode, msgtext);
    }
    return codeArg;
}


AbstractExpression*
ExpressionUtil::functionFactory(int functionId, const std::vector<AbstractExpression*>* arguments) {
    assert(arguments);
    AbstractExpression* ret = 0;
    size_t nArgs = arguments->size();
    switch(nArgs) {
    case 0:
        // ret = new ConstantFunctionExpression<???>();
        delete arguments;
        break;
    case 1:
        if (functionId == FUNC_ABS) {
            ret = new UnaryFunctionExpression<FUNC_ABS>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_MONTH) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MONTH>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY_OF_WEEK) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_WEEK>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_WEEK_OF_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEK_OF_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY_OF_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_QUARTER) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_QUARTER>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_HOUR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_HOUR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_MINUTE) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MINUTE>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_SECOND) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_SECOND>((*arguments)[0]);
        }


        else if (functionId == FUNC_VOLT_SQL_ERROR) {
            ret = new UnaryFunctionExpression<FUNC_VOLT_SQL_ERROR>((*arguments)[0]);
        }
        delete arguments;
        break;
    default:
        // GeneralFunctions defer deleting the arguments container until through with it.
        if (functionId == FUNC_VOLT_SUBSTRING_CHAR_FROM) {
            ret = new GeneralFunctionExpression<FUNC_VOLT_SUBSTRING_CHAR_FROM>(*arguments);
        } else if (functionId == FUNC_SUBSTRING_CHAR) {
            ret = new GeneralFunctionExpression<FUNC_SUBSTRING_CHAR>(*arguments);


        } else if (functionId == FUNC_VOLT_SQL_ERROR) {
            ret = new GeneralFunctionExpression<FUNC_VOLT_SQL_ERROR>(*arguments);
        }
    }
    // May return null, leaving it to the caller (with more context) to generate an exception.
    return ret;
}

}

