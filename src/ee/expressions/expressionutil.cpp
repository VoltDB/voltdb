/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "expressionutil.h"

#include "common/debuglog.h"
#include "common/ValueFactory.hpp"
#include "common/FatalException.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressions.h"

#include <cassert>
#include <sstream>
#include <cstdlib>
#include <stdexcept>

#include "json_spirit/json_spirit.h"

namespace voltdb {

/** Function static helper templated functions to vivify an optimal
    comparison class. */
static AbstractExpression*
getGeneral(ExpressionType c,
           AbstractExpression *l,
           AbstractExpression *r)
{
    assert (l);
    assert (r);
    switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
        return new ComparisonExpression<CmpEq>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
        return new ComparisonExpression<CmpNe>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
        return new ComparisonExpression<CmpLt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
        return new ComparisonExpression<CmpGt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
        return new ComparisonExpression<CmpLte>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
        return new ComparisonExpression<CmpGte>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LIKE):
        return new ComparisonExpression<CmpLike>(c, l, r);
    default:
        char message[256];
        snprintf(message, 256, "Invalid ExpressionType '%s' called"
                " for ComparisonExpression",
                expressionToString(c).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
}


template <typename L, typename R>
static AbstractExpression*
getMoreSpecialized(ExpressionType c, L* l, R* r)
{
    assert (l);
    assert (r);
    switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
        return new InlinedComparisonExpression<CmpEq, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
        return new InlinedComparisonExpression<CmpNe, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
        return new InlinedComparisonExpression<CmpLt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
        return new InlinedComparisonExpression<CmpGt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
        return new InlinedComparisonExpression<CmpLte, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
        return new InlinedComparisonExpression<CmpGte, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LIKE):
        return new InlinedComparisonExpression<CmpLike, L, R>(c, l, r);
    default:
        char message[256];
        snprintf(message, 256, "Invalid ExpressionType '%s' called for"
                " ComparisonExpression",expressionToString(c).c_str());
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
}

/** convert the enumerated value type into a concrete c type for the
 * comparison helper templates. */
AbstractExpression *
ExpressionUtil::comparisonFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc)
{
    assert(lc);
    /*printf("left: %s\n", left_optimized->debug("").c_str());
    fflush(stdout);
    printf("right: %s\n", right_optimized->debug("").c_str());
    fflush(stdout);*/

    //printf("%s\n", right_optimized->debug().c_str());
    //fflush(stdout);

    // more specialization available?
    ConstantValueExpression *l_const =
      dynamic_cast<ConstantValueExpression*>(lc);

    ConstantValueExpression *r_const =
      dynamic_cast<ConstantValueExpression*>(rc);

    TupleValueExpression *l_tuple =
      dynamic_cast<TupleValueExpression*>(lc);

    TupleValueExpression *r_tuple =
      dynamic_cast<TupleValueExpression*>(rc);

    // this will inline getValue(), hooray!
    if (l_const != NULL && r_const != NULL) { // CONST-CONST can it happen?
        return getMoreSpecialized<ConstantValueExpression, ConstantValueExpression>(et, l_const, r_const);
    } else if (l_const != NULL && r_tuple != NULL) { // CONST-TUPLE
        return getMoreSpecialized<ConstantValueExpression, TupleValueExpression>(et, l_const, r_tuple);
    } else if (l_tuple != NULL && r_const != NULL) { // TUPLE-CONST
        return getMoreSpecialized<TupleValueExpression, ConstantValueExpression >(et, l_tuple, r_const);
    } else if (l_tuple != NULL && r_tuple != NULL) { // TUPLE-TUPLE
        return getMoreSpecialized<TupleValueExpression, TupleValueExpression>(et, l_tuple, r_tuple);
    }

    //okay, still getTypedValue is beneficial.
    return getGeneral(et, lc, rc);
}

/** convert the enumerated value type into a concrete c type for the
 *  operator expression templated ctors */
static AbstractExpression *
operatorFactory(ExpressionType et,
                AbstractExpression *lc, AbstractExpression *rc)
{
    AbstractExpression *ret = NULL;

   switch(et) {
     case (EXPRESSION_TYPE_OPERATOR_PLUS):
       ret = new OperatorExpression<OpPlus>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_MINUS):
       ret = new OperatorExpression<OpMinus>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
       ret = new OperatorExpression<OpMultiply>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
       ret = new OperatorExpression<OpDivide>(et, lc, rc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_NOT):
       ret = new OperatorNotExpression(lc);
       break;

     case (EXPRESSION_TYPE_OPERATOR_IS_NULL):
         ret = new OperatorIsNullExpression(lc);
         break;

     case (EXPRESSION_TYPE_OPERATOR_MOD):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Mod operator is not yet supported.");

     case (EXPRESSION_TYPE_OPERATOR_CONCAT):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Concat operator not yet supported.");

     case (EXPRESSION_TYPE_OPERATOR_CAST):
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "Cast operator not yet supported.");

     default:
       throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                     "operator ctor helper out of sync");
   }
   return ret;
}

/** convert the enumerated value type into a concrete type for
 * constant value expressions templated ctors */
static AbstractExpression*
constantValueFactory(json_spirit::Object &obj,
                     ValueType vt, ExpressionType et,
                     AbstractExpression *lc, AbstractExpression *rc)
{
    // read before ctor - can then instantiate fully init'd obj.
    NValue newvalue;
    json_spirit::Value isNullValue = json_spirit::find_value( obj, "ISNULL");
    if (isNullValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Could not find"
                                      " ISNULL value");
    }
    if (isNullValue.type() != json_spirit::bool_type) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: ISNULL value"
                                      " is not a boolean.");
    }
    bool isNull = isNullValue.get_bool();
    if (isNull)
    {
        newvalue = NValue::getNullValue(vt);
        return new ConstantValueExpression(newvalue);
    }

    json_spirit::Value valueValue = json_spirit::find_value( obj, "VALUE");
    if (valueValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Could not find"
                                      " VALUE value");
    }

    switch (vt) {
    case VALUE_TYPE_INVALID:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Value type should"
                                      " never be VALUE_TYPE_INVALID");
    case VALUE_TYPE_NULL:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: And they should be"
                                      " never be this either! VALUE_TYPE_NULL");
    case VALUE_TYPE_TINYINT:
        newvalue = ValueFactory::getTinyIntValue(static_cast<int8_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_SMALLINT:
        newvalue = ValueFactory::getSmallIntValue(static_cast<int16_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_INTEGER:
        newvalue = ValueFactory::getIntegerValue(static_cast<int32_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_BIGINT:
        newvalue = ValueFactory::getBigIntValue(static_cast<int64_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_DOUBLE:
        newvalue = ValueFactory::getDoubleValue(static_cast<double>(valueValue.get_real()));
        break;
    case VALUE_TYPE_VARCHAR:
        newvalue = ValueFactory::getStringValue(valueValue.get_str());
        break;
    case VALUE_TYPE_VARBINARY:
        // uses hex encoding
        newvalue = ValueFactory::getBinaryValue(valueValue.get_str());
        break;
    case VALUE_TYPE_TIMESTAMP:
        newvalue = ValueFactory::getTimestampValue(static_cast<int64_t>(valueValue.get_int64()));
        break;
    case VALUE_TYPE_DECIMAL:
        newvalue = ValueFactory::getDecimalValueFromString(valueValue.get_str());
        break;
    default:
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "constantValueFactory: Unrecognized value"
                                      " type");
    }

    return new ConstantValueExpression(newvalue);
}


/** convert the enumerated value type into a concrete c type for
 * parameter value expression templated ctors */
static AbstractExpression*
parameterValueFactory(json_spirit::Object &obj,
                      ExpressionType et,
                      AbstractExpression *lc, AbstractExpression *rc)
{
    // read before ctor - can then instantiate fully init'd obj.
    json_spirit::Value paramIdxValue = json_spirit::find_value( obj, "PARAM_IDX");
    if (paramIdxValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "parameterValueFactory: Could not find"
                                      " PARAM_IDX value");
    }
    int param_idx = paramIdxValue.get_int();
    assert (param_idx >= 0);
    return new ParameterValueExpression(param_idx);
}

/** convert the enumerated value type into a concrete c type for
 * tuple value expression templated ctors */
static AbstractExpression*
tupleValueFactory(json_spirit::Object &obj, ExpressionType et,
                  AbstractExpression *lc, AbstractExpression *rc)
{
    // read the tuple value expression specific data
    json_spirit::Value valueIdxValue =
      json_spirit::find_value( obj, "COLUMN_IDX");

    json_spirit::Value tableName =
      json_spirit::find_value(obj, "TABLE_NAME");

    json_spirit::Value columnName =
      json_spirit::find_value(obj, "COLUMN_NAME");

    // verify input
    if (valueIdxValue == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: Could not find"
                                      " COLUMN_IDX value");
    }
    if (valueIdxValue.get_int() < 0) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: invalid column_idx.");
    }

    if (tableName == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: no table name in TVE");
    }

    if (columnName == json_spirit::Value::null) {
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                      "tupleValueFactory: no column name in"
                                      " TVE");
    }


    return new TupleValueExpression(valueIdxValue.get_int(),
                                    tableName.get_str(),
                                    columnName.get_str());
}

AbstractExpression *
ExpressionUtil::conjunctionFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc)
{
    switch (et) {
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
        return new ConjunctionExpression<ConjunctionAnd>(et, lc, rc);
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
        return new ConjunctionExpression<ConjunctionOr>(et, lc, rc);
    default:
        return NULL;
    }

}


/** Given an expression type and a valuetype, find the best
 * templated ctor to invoke. Several helpers, above, aid in this
 * pursuit. Each instantiated expression must consume any
 * class-specific serialization from serialize_io. */
AbstractExpression*
ExpressionUtil::expressionFactory(json_spirit::Object &obj,
                  ExpressionType et, ValueType vt, int vs,
                  AbstractExpression* lc,
                  AbstractExpression* rc,
                  const std::vector<AbstractExpression*>* args)
{
    AbstractExpression *ret = NULL;

    switch (et) {

        // Operators
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
    case (EXPRESSION_TYPE_OPERATOR_MINUS):
    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
    case (EXPRESSION_TYPE_OPERATOR_MOD):
    case (EXPRESSION_TYPE_OPERATOR_CAST):
    case (EXPRESSION_TYPE_OPERATOR_NOT):
    case (EXPRESSION_TYPE_OPERATOR_IS_NULL):
        ret = operatorFactory(et, lc, rc);
    break;

    // Comparisons
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
        ret = comparisonFactory( et, lc, rc);
    break;

    // Conjunctions
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
        ret = conjunctionFactory(et, lc, rc);
    break;

    // Functions and pseudo-functions
    case (EXPRESSION_TYPE_FUNCTION): {
        // add the function id
        json_spirit::Value functionIdValue = json_spirit::find_value(obj, "FUNCTION_ID");
        if (functionIdValue == json_spirit::Value::null) {
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "ExpressionUtil::"
                                          "expressionFactory:"
                                          " Couldn't find FUNCTION_ID value");
        }
        int functionId = functionIdValue.get_int();

        ret = functionFactory(functionId, args);
        if ( ! ret) {
            json_spirit::Value functionNameValue = json_spirit::find_value(obj, "NAME");
            std::string nameString;
            if (functionNameValue == json_spirit::Value::null) {
                nameString = "?";
            } else {
                nameString = functionNameValue.get_str();
            }

            char aliasBuffer[256];
            json_spirit::Value functionAliasValue = json_spirit::find_value(obj, "ALIAS");
            if (functionAliasValue == json_spirit::Value::null) {
                aliasBuffer[0] = '\0';
            } else {
                std::string aliasString = functionAliasValue.get_str();
                snprintf(aliasBuffer, sizeof(aliasBuffer), " aliased to '%s'", aliasString.c_str());
            }

            char fn_message[1024];
            snprintf(fn_message, sizeof(fn_message),
                     "SQL function '%s'%s with ID (%d) with (%d) parameters is not implemented in VoltDB (or may have been incorrectly parsed)",
                     nameString.c_str(), aliasBuffer, functionId, (int)args->size());
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, fn_message);
        }
    }
    break;

    // Constant Values, parameters, tuples
    case (EXPRESSION_TYPE_VALUE_CONSTANT):
        ret = constantValueFactory(obj, vt, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_PARAMETER):
        ret = parameterValueFactory(obj, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_TUPLE):
        ret = tupleValueFactory(obj, et, lc, rc);
        break;

    case (EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
        ret = new TupleAddressExpression();
        break;

        // must handle all known expressions in this factory
    default:

        char message[256];
        snprintf(message,256, "Invalid ExpressionType '%s' (%d) requested from factory",
                expressionToString(et).c_str(), (int)et);
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }

    ret->setValueType(vt);
    ret->setValueSize(vs);
    // written thusly to ease testing/inspecting return content.
    VOLT_TRACE("Created expression %p", ret);
    return ret;
}

boost::shared_array<int>
ExpressionUtil::convertIfAllTupleValues(const std::vector<voltdb::AbstractExpression*> &expressions)
{
    size_t cnt = expressions.size();
    boost::shared_array<int> ret(new int[cnt]);
    for (int i = 0; i < cnt; ++i) {
        voltdb::TupleValueExpression* casted=
          dynamic_cast<voltdb::TupleValueExpression*>(expressions[i]);
        if (casted == NULL) {
            return boost::shared_array<int>();
        }
        ret[i] = casted->getColumnId();
    }
    return ret;
}

boost::shared_array<int>
ExpressionUtil::convertIfAllParameterValues(const std::vector<voltdb::AbstractExpression*> &expressions)
{
    size_t cnt = expressions.size();
    boost::shared_array<int> ret(new int[cnt]);
    for (int i = 0; i < cnt; ++i) {
        voltdb::ParameterValueExpression *casted =
          dynamic_cast<voltdb::ParameterValueExpression*>(expressions[i]);
        if (casted == NULL) {
            return boost::shared_array<int>();
        }
        ret[i] = casted->getParameterId();
    }
    return ret;
}

void ExpressionUtil::loadIndexedExprsFromJson(std::vector<AbstractExpression*>& indexed_exprs, const std::string& jsonarraystring)
{
    json_spirit::Value jValue;
    json_spirit::read( jsonarraystring, jValue );
    json_spirit::Array expressionsArray = jValue.get_array();
    for (int ii = 0; ii < expressionsArray.size(); ii++) {
        json_spirit::Object expressionObject = expressionsArray[ii].get_obj();
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(expressionObject);
        indexed_exprs.push_back(expr);
    }
}


}
