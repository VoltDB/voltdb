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
    const char* sqlstatecode;
    const char* msgtext;
    const ValueType type = getValueType();
    char msg_format_buffer[1024];
    char state_format_buffer[6];
    if (type == VALUE_TYPE_VARCHAR) {
        const int32_t valueLength = getObjectLength();
        const char *valueChars = reinterpret_cast<char*>(getObjectValue());
        std::string valueStr(valueChars, valueLength);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
        sqlstatecode = SQLException::nonspecific_error_code_for_error_forced_by_user;
        msgtext = msg_format_buffer;
    } else {
        int64_t intValue = castAsBigIntAndGetValue(); // let cast throw if invalid
        if (intValue == 0) {
            return *this;
        }
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld", (long) intValue);
        sqlstatecode = state_format_buffer;
        msgtext = SQLException::specific_error_specified_by_user;
    }
    throw SQLException(sqlstatecode, msgtext);
}

/** implement the 2-argument forced SQL ERROR function (for test and example purposes) */
template<> inline NValue NValue::call<FUNC_VOLT_SQL_ERROR>(const std::vector<NValue>& arguments) {
    assert(arguments.size() == 2);
    const char* sqlstatecode;
    char msg_format_buffer[1024];
    char state_format_buffer[6];

    const NValue& codeArg = arguments[0];
    if (codeArg.isNull()) {
        sqlstatecode = SQLException::nonspecific_error_code_for_error_forced_by_user;
    } else {
        int64_t intValue = codeArg.castAsBigIntAndGetValue(); // let cast throw if invalid
        if (intValue == 0) {
            return codeArg;
        }
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld", (long) intValue);
        sqlstatecode = state_format_buffer;
    }

    const NValue& strValue = arguments[1];
    if (strValue.isNull()) {
        msg_format_buffer[0] = '\0';
    } else {
        if (strValue.getValueType() != VALUE_TYPE_VARCHAR) {
            throwCastSQLException (strValue.getValueType(), VALUE_TYPE_VARCHAR);
        }
        const int32_t valueLength = strValue.getObjectLength();
        char *valueChars = reinterpret_cast<char*>(strValue.getObjectValue());
        std::string valueStr(valueChars, valueLength);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
    }
    throw SQLException(sqlstatecode, msg_format_buffer);
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
        std::stringstream buffer;
        buffer << spacer << "ConstantFunctionExpression " << F << std::endl;
        return (buffer.str());
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

    virtual bool hasParameter() const {
        return m_child->hasParameter();
    }

    virtual void substitute(const NValueArray &params) {
        assert (m_child);

        if (!m_hasParameter)
            return;

        VOLT_TRACE("Substituting parameters for expression \n%s ...", debug(true).c_str());
        m_child->substitute(params);
    }

    NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
        assert (m_child);
        return (m_child->eval(tuple1, tuple2)).callUnary<F>();
    }

    std::string debugInfo(const std::string &spacer) const {
        std::stringstream buffer;
        buffer << spacer << "UnaryFunctionExpression " << F << std::endl;
        return (buffer.str());
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

    virtual bool hasParameter() const {
        for (size_t i = 0; i < m_args.size(); i++) {
            assert(m_args[i]);
            if (m_args[i]->hasParameter()) {
                return true;
            }
        }
        return false;
    }

    virtual void substitute(const NValueArray &params) {
        if (!m_hasParameter)
            return;

        VOLT_TRACE("Substituting parameters for expression \n%s ...", debug(true).c_str());
        for (size_t i = 0; i < m_args.size(); i++) {
            assert(m_args[i]);
            VOLT_TRACE("Substituting parameters for arg at index %d...", static_cast<int>(i));
            m_args[i]->substitute(params);
        }
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
        std::stringstream buffer;
        buffer << spacer << "GeneralFunctionExpression " << F << std::endl;
        return (buffer.str());
    }

private:
    const std::vector<AbstractExpression *>& m_args;
};

}

using namespace functionexpression;

AbstractExpression*
ExpressionUtil::functionFactory(int functionId, const std::vector<AbstractExpression*>* arguments) {
    assert(arguments);
    AbstractExpression* ret = 0;
    size_t nArgs = arguments->size();
    switch(nArgs) {
    case 0:
        // ret = new ConstantFunctionExpression<???>();
        if (ret) {
            delete arguments;
        }
        break;
    case 1:
        // TODO: consider converting this else-if series to a switch statement
        // Please keep these blocks sorted alphabetically for ease of reference and to avoid merge
        // conflicts that occur when appending new blocks at the same line.
        if (functionId == FUNC_ABS) {
            ret = new UnaryFunctionExpression<FUNC_ABS>((*arguments)[0]);
        } else if (functionId == FUNC_CHAR_LENGTH) {
            ret = new UnaryFunctionExpression<FUNC_CHAR_LENGTH>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY_OF_WEEK) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_WEEK>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_DAY_OF_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_HOUR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_HOUR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_MINUTE) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MINUTE>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_MONTH) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MONTH>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_QUARTER) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_QUARTER>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_SECOND) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_SECOND>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_WEEK_OF_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEK_OF_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_EXTRACT_YEAR) {
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_YEAR>((*arguments)[0]);
        } else if (functionId == FUNC_OCTET_LENGTH) {
            ret = new UnaryFunctionExpression<FUNC_OCTET_LENGTH>((*arguments)[0]);
        } else if (functionId == FUNC_SPACE) {
            ret = new UnaryFunctionExpression<FUNC_SPACE>((*arguments)[0]);
        } else if (functionId == FUNC_VOLT_SQL_ERROR) {
            ret = new UnaryFunctionExpression<FUNC_VOLT_SQL_ERROR>((*arguments)[0]);
        }
        if (ret) {
            delete arguments;
        }
        break;
    default:
        // GeneralFunctions defer deleting the arguments container until through with it.
        // TODO: consider converting this else-if series to a switch statement
        // Please keep these blocks sorted alphabetically for ease of reference and to avoid merge
        // conflicts that occur when appending new blocks at the same line.
        if (functionId == FUNC_CONCAT) {
            ret = new GeneralFunctionExpression<FUNC_CONCAT>(*arguments);
        } else if (functionId == FUNC_DECODE) {
            ret = new GeneralFunctionExpression<FUNC_DECODE>(*arguments);
        } else if (functionId == FUNC_LEFT) {
            ret = new GeneralFunctionExpression<FUNC_LEFT>(*arguments);
        } else if (functionId == FUNC_POSITION_CHAR) {
            ret = new GeneralFunctionExpression<FUNC_POSITION_CHAR>(*arguments);
        } else if (functionId == FUNC_REPEAT) {
            ret = new GeneralFunctionExpression<FUNC_REPEAT>(*arguments);
        } else if (functionId == FUNC_RIGHT) {
            ret = new GeneralFunctionExpression<FUNC_RIGHT>(*arguments);
        } else if (functionId == FUNC_SUBSTRING_CHAR) {
            ret = new GeneralFunctionExpression<FUNC_SUBSTRING_CHAR>(*arguments);
        } else if (functionId == FUNC_VOLT_SUBSTRING_CHAR_FROM) {
            ret = new GeneralFunctionExpression<FUNC_VOLT_SUBSTRING_CHAR_FROM>(*arguments);
        } else if (functionId == FUNC_VOLT_SQL_ERROR) {
            ret = new GeneralFunctionExpression<FUNC_VOLT_SQL_ERROR>(*arguments);
        }
    }
    // May return null, leaving it to the caller (with more context) to generate an exception.
    return ret;
}

}

