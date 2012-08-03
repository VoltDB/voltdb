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

#include "common/common.h"
#include "common/serializeio.h"
#include "common/valuevector.h"

#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"

#include <string>
#include <cassert>

namespace voltdb {

namespace functionexpressions {

/*
 * Constant (no parameter) function. (now, random)
 */
template <ExpressionType E>
class ConstantFunctionExpression : public AbstractExpression {
public:
    ConstantFunctionExpression(const std::string& sqlName, const std::string& uniqueName)
        : AbstractExpression(E) {
    };

    NValue eval(const TableTuple *, const TableTuple *) const {
        return NValue::callConstant<E>();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "ConstantFunctionExpression " + expressionToString(getExpressionType()));
    }
};

/*
 * Unary functions. (abs, upper, lower)
 */

template <ExpressionType E>
class UnaryFunctionExpression : public AbstractExpression {
    AbstractExpression * const m_child;
public:
    UnaryFunctionExpression(AbstractExpression *child)
        : AbstractExpression(E)
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
        return (m_child->eval(tuple1, tuple2)).callUnary<E>();
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "UnaryFunctionExpression " + expressionToString(getExpressionType()));
    }
};

/*
 * N-ary functions.
 */
template <ExpressionType E>
class GeneralFunctionExpression : public AbstractExpression {
public:
    GeneralFunctionExpression(const std::vector<AbstractExpression *>& args)
        : AbstractExpression(E), m_args(args) {}

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
        std::vector<NValue> nValue(m_args.size());
        for (int i = 0; i < m_args.size(); ++i) {
            nValue[i] = m_args[i]->eval(tuple1, tuple2);
        }
        return NValue::call<E>(nValue);
    }

    std::string debugInfo(const std::string &spacer) const {
        return (spacer + "GeneralFunctionExpression " + expressionToString(getExpressionType()));
    }

private:
    const std::vector<AbstractExpression *>& m_args;
};

}
}

using namespace voltdb;
using namespace functionexpressions;

namespace voltdb {

AbstractExpression*
ExpressionUtil::functionFactory(ExpressionType et, const std::vector<AbstractExpression*>* arguments) {
    assert(arguments);
    AbstractExpression* ret = 0;
    size_t nArgs = arguments->size();
    switch(nArgs) {
    case 0:
        // ret = new ConstantFunctionExpression<???>();
        delete arguments;
        break;
    case 1:
        if (et == EXPRESSION_TYPE_FUNCTION_ABS) {
            ret = new UnaryFunctionExpression<EXPRESSION_TYPE_FUNCTION_ABS>((*arguments)[0]);
            delete arguments;
        }
        break;
    default:
        // GeneralFunctions delete the arguments container when through with it.
        if (et == EXPRESSION_TYPE_FUNCTION_SUBSTRING_FROM) {
            ret = new GeneralFunctionExpression<EXPRESSION_TYPE_FUNCTION_SUBSTRING_FROM>(*arguments);
        } else if (et == EXPRESSION_TYPE_FUNCTION_SUBSTRING_FROM_FOR) {
            ret = new GeneralFunctionExpression<EXPRESSION_TYPE_FUNCTION_SUBSTRING_FROM_FOR>(*arguments);
        }
    }
    assert(ret);
    return ret;
}

}

