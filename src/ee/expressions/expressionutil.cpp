/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
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
#include "common/ValueFactory.hpp"
#include "expressions/expressions.h"
#include "expressions/functionexpression.h"
#include "expressions/vectorexpression.h"

namespace voltdb {

/** Parse JSON parameters to create a hash range expression */
static AbstractExpression* hashRangeFactory(PlannerDomValue obj) {
    PlannerDomValue hashColumnValue = obj.valueForKey("HASH_COLUMN");

    PlannerDomValue rangesArray = obj.valueForKey("RANGES");

    srange_type *ranges = new srange_type[rangesArray.arrayLen()];
    for (int ii = 0; ii < rangesArray.arrayLen(); ii++) {
        PlannerDomValue arrayObject = rangesArray.valueAtIndex(ii);
        PlannerDomValue rangeStartValue = arrayObject.valueForKey("RANGE_START");
        PlannerDomValue rangeEndValue = arrayObject.valueForKey("RANGE_END");

        ranges[ii] = srange_type(rangeStartValue.asInt(), rangeEndValue.asInt());
    }
    return new HashRangeExpression(hashColumnValue.asInt(), ranges, static_cast<int>(rangesArray.arrayLen()));
}

/** Parse JSON parameters to create a subquery expression */
static AbstractExpression* subqueryFactory(ExpressionType subqueryType, PlannerDomValue obj, const std::vector<AbstractExpression*>& args) {
    int subqueryId = obj.valueForKey("SUBQUERY_ID").asInt();
    std::vector<int> paramIdxs;
    if (obj.hasNonNullKey("PARAM_IDX")) {
        PlannerDomValue params = obj.valueForKey("PARAM_IDX");
        int paramSize = params.arrayLen();
        paramIdxs.reserve(paramSize);
        if (args.size() != paramSize) {
            throw SerializableEEException("subqueryFactory: parameter indexes/tve count mismatch");
        }
        for (int i = 0; i < paramSize; ++i) {
            int paramIdx = params.valueAtIndex(i).asInt();
            paramIdxs.push_back(paramIdx);
        }
    }
    std::vector<int> otherParamIdxs;
    if (obj.hasNonNullKey("OTHER_PARAM_IDX")) {
        PlannerDomValue otherParams = obj.valueForKey("OTHER_PARAM_IDX");
        int otherParamSize = otherParams.arrayLen();
        otherParamIdxs.reserve(otherParamSize);
        otherParamIdxs.reserve(otherParamSize);
        for (int i = 0; i < otherParamSize; ++i) {
            int paramIdx = otherParams.valueAtIndex(i).asInt();
            otherParamIdxs.push_back(paramIdx);
        }
    }
    return new SubqueryExpression(subqueryType, subqueryId, paramIdxs, otherParamIdxs, args);
}

template<typename OuterExtractor, typename InnerExtractor>
inline AbstractExpression* subqueryComparisonFactory(ExpressionType const c,
      AbstractExpression* outer, AbstractExpression* inner, QuantifierType quantifier) {
   switch(c) {
      case EXPRESSION_TYPE_COMPARE_EQUAL:
         return new VectorComparisonExpression<CmpEq, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
         return new VectorComparisonExpression<CmpNe, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_LESSTHAN:
         return new VectorComparisonExpression<CmpLt, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
         return new VectorComparisonExpression<CmpGt, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
         return new VectorComparisonExpression<CmpLte, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
         return new VectorComparisonExpression<CmpGte, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_NOTDISTINCT:
         return new VectorComparisonExpression<CmpNotDistinct, OuterExtractor, InnerExtractor>(
               c, outer, inner, quantifier);
      case EXPRESSION_TYPE_COMPARE_LIKE:
         // LIKE operator only works when inner relation is not a tuple
      case EXPRESSION_TYPE_COMPARE_STARTSWITH:  // likewise for startswith
      default:
         throwSerializableEEException(
                 "Invalid ExpressionType '%s' called for VectorComparisonExpression",
                 expressionToString(c).c_str());
   }
}

/** Function static helper templated functions to vivify an optimal
    comparison class. */
static AbstractExpression* subqueryComparisonFactory(PlannerDomValue obj,
      ExpressionType c, AbstractExpression *l, AbstractExpression *r) {
   QuantifierType quantifier = QUANTIFIER_TYPE_NONE;
   if (obj.hasNonNullKey("QUANTIFIER")) {
      quantifier = static_cast<QuantifierType>(obj.valueForKey("QUANTIFIER").asInt());
   }
   SubqueryExpression const *l_subquery = dynamic_cast<SubqueryExpression*>(l);
   SubqueryExpression const *r_subquery = dynamic_cast<SubqueryExpression*>(r);
   if (l_subquery != NULL && r_subquery != NULL) {
      return subqueryComparisonFactory<TupleExtractor, TupleExtractor>(c, l, r, quantifier);
   } else if (l_subquery != NULL) {
      switch (c) {
         case EXPRESSION_TYPE_COMPARE_LIKE:
            return new VectorComparisonExpression<CmpLike, TupleExtractor, NValueExtractor>(
                  c, l, r, quantifier);
         case EXPRESSION_TYPE_COMPARE_STARTSWITH:
            return new VectorComparisonExpression<CmpStartsWith, TupleExtractor, NValueExtractor>(
                  c, l, r, quantifier);
         default:
            return subqueryComparisonFactory<TupleExtractor, NValueExtractor>(c, l, r, quantifier);
      }
   } else {
      vassert(r_subquery != NULL);
      switch (c) {
         case EXPRESSION_TYPE_COMPARE_LIKE:
            return new VectorComparisonExpression<CmpLike, NValueExtractor, TupleExtractor>(
                  c, l, r, quantifier);
         case EXPRESSION_TYPE_COMPARE_STARTSWITH:
            return new VectorComparisonExpression<CmpStartsWith, NValueExtractor, TupleExtractor>(
                  c, l, r, quantifier);
         default:
            return subqueryComparisonFactory<NValueExtractor, TupleExtractor>(c, l, r, quantifier);
      }
   }
}

static AbstractExpression* getGeneral(ExpressionType c, AbstractExpression *l, AbstractExpression *r) {
    vassert(l);
    vassert(r);
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
    case (EXPRESSION_TYPE_COMPARE_STARTSWITH):
        return new ComparisonExpression<CmpStartsWith>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_IN):
        return new ComparisonExpression<CmpIn>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTDISTINCT):
        return new ComparisonExpression<CmpNotDistinct>(c, l, r);
    default:
        throwSerializableEEException(
                "Invalid ExpressionType '%s' called for ComparisonExpression",
                expressionToString(c).c_str());
    }
}


template <typename L, typename R>
static AbstractExpression* getMoreSpecialized(ExpressionType c, L* l, R* r)
{
    vassert(l);
    vassert(r);
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
    case (EXPRESSION_TYPE_COMPARE_STARTSWITH):
        return new InlinedComparisonExpression<CmpStartsWith, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_IN):
        return new InlinedComparisonExpression<CmpIn, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTDISTINCT):
        return new InlinedComparisonExpression<CmpNotDistinct, L, R>(c, l, r);
    default:
        throwSerializableEEException(
                "Invalid ExpressionType '%s' called for ComparisonExpression",
                expressionToString(c).c_str());
    }
}

/** convert the enumerated value type into a concrete c type for the
 * comparison helper templates. */
AbstractExpression* ExpressionUtil::comparisonFactory(
      PlannerDomValue obj, ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
    vassert(lc);

    // more specialization available?
    ConstantValueExpression *l_const = dynamic_cast<ConstantValueExpression*>(lc);
    ConstantValueExpression *r_const = dynamic_cast<ConstantValueExpression*>(rc);
    TupleValueExpression *l_tuple = dynamic_cast<TupleValueExpression*>(lc);
    TupleValueExpression *r_tuple = dynamic_cast<TupleValueExpression*>(rc);

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

    SubqueryExpression *l_subquery = dynamic_cast<SubqueryExpression*>(lc);
    SubqueryExpression *r_subquery = dynamic_cast<SubqueryExpression*>(rc);

    if (l_subquery != NULL || r_subquery != NULL) {
        return subqueryComparisonFactory(obj, et, lc, rc);
    }

    //okay, still getTypedValue is beneficial.
    return getGeneral(et, lc, rc);
}

/** convert the enumerated value type into a concrete type for
 * constant value expressions templated ctors */
static AbstractExpression* constantValueFactory(
      PlannerDomValue obj, ValueType vt, ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
    // read before ctor - can then instantiate fully init'd obj.
    NValue newvalue;
    bool isNull = obj.valueForKey("ISNULL").asBool();
    if (isNull) {
        newvalue = NValue::getNullValue(vt);
        return new ConstantValueExpression(newvalue);
    }

    PlannerDomValue valueValue = obj.valueForKey("VALUE");

    switch (vt) {
        case ValueType::tINVALID:
            throw SerializableEEException("constantValueFactory: Value type should never be tINVALID");
        case ValueType::tNULL:
            throw SerializableEEException("constantValueFactory: And they should be never be this either! tNULL");
        case ValueType::tTINYINT:
            newvalue = ValueFactory::getTinyIntValue(static_cast<int8_t>(valueValue.asInt64()));
            break;
        case ValueType::tSMALLINT:
            newvalue = ValueFactory::getSmallIntValue(static_cast<int16_t>(valueValue.asInt64()));
            break;
        case ValueType::tINTEGER:
            newvalue = ValueFactory::getIntegerValue(static_cast<int32_t>(valueValue.asInt64()));
            break;
        case ValueType::tBIGINT:
            newvalue = ValueFactory::getBigIntValue(static_cast<int64_t>(valueValue.asInt64()));
            break;
        case ValueType::tDOUBLE:
            newvalue = ValueFactory::getDoubleValue(static_cast<double>(valueValue.asDouble()));
            break;
        case ValueType::tVARCHAR:
            newvalue = ValueFactory::getStringValue(valueValue.asStr());
            break;
        case ValueType::tVARBINARY:
            // uses hex encoding
            newvalue = ValueFactory::getBinaryValue(valueValue.asStr());
            break;
        case ValueType::tTIMESTAMP:
            newvalue = ValueFactory::getTimestampValue(static_cast<int64_t>(valueValue.asInt64()));
            break;
        case ValueType::tDECIMAL:
            newvalue = ValueFactory::getDecimalValueFromString(valueValue.asStr());
            break;
        case ValueType::tBOOLEAN:
            newvalue = ValueFactory::getBooleanValue(valueValue.asBool());
            break;
        default:
            throw SerializableEEException("constantValueFactory: Unrecognized value type");
    }

    return new ConstantValueExpression(newvalue);
}


static AbstractExpression* castFactory(ValueType vt, AbstractExpression *lc) {
    return new OperatorCastExpression(vt, lc);
}

static void raiseFunctionFactoryError(
      const std::string& nameString, int functionId, const std::vector<AbstractExpression*>& args) {
   char fn_message[1024];
   snprintf(fn_message, sizeof(fn_message),
         "Internal Error: SQL function '%s' with ID (%d) with (%lu) parameters is not implemented "
         "in VoltDB (or may have been incorrectly parsed)",
         nameString.c_str(), functionId, args.size());
   fn_message[sizeof fn_message - 1] = '\0';
   DEBUG_ASSERT_OR_THROW_OR_CRASH(false, fn_message);
   throw SerializableEEException(fn_message);
}

AbstractExpression* ExpressionUtil::vectorFactory(
      ValueType elementType, const std::vector<AbstractExpression*>& arguments) {
    return new VectorExpression(elementType, arguments);
}

static AbstractExpression* caseWhenFactory(ValueType vt, AbstractExpression *lc, AbstractExpression *rc) {

    OperatorAlternativeExpression* alternative = dynamic_cast<OperatorAlternativeExpression*> (rc);
    if (!rc) {
        throw SerializableEEException("operator case when has incorrect expression");
    }
    return new OperatorCaseWhenExpression(vt, lc, alternative);
}


/** convert the enumerated value type into a concrete c type for
 * parameter value expression templated ctors */
static AbstractExpression* parameterValueFactory(
      PlannerDomValue obj, ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
    // read before ctor - can then instantiate fully init'd obj.
    int param_idx = obj.valueForKey("PARAM_IDX").asInt();
    vassert(param_idx >= 0);
    return new ParameterValueExpression(param_idx);
}

/** convert the enumerated value type into a concrete c type for
 * tuple value expression templated ctors */
static AbstractExpression* tupleValueFactory(
      PlannerDomValue obj, ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
    // read the tuple value expression specific data
    int columnIndex = obj.valueForKey("COLUMN_IDX").asInt();
    int tableIdx = 0;
    if (obj.hasNonNullKey("TABLE_IDX")) {
        tableIdx = obj.valueForKey("TABLE_IDX").asInt();
    }

    // verify input
    if (columnIndex < 0) {
        std::ostringstream message;
        message << "tupleValueFactory: invalid column_idx " << columnIndex <<
                " for " << ((tableIdx == 0) ? "" : "inner ") << "table\nStack trace:\n";
        StackTrace::streamStackTrace(message, "");
        throw UnexpectedEEException(message.str());
    }

    return new TupleValueExpression(tableIdx, columnIndex);
}


AbstractExpression* ExpressionUtil::conjunctionFactory(
      ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
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
AbstractExpression* ExpressionUtil::expressionFactory(
      PlannerDomValue obj, ExpressionType et, ValueType vt, int vs, AbstractExpression* lc, AbstractExpression* rc,
      const std::vector<AbstractExpression*>& args) {
   AbstractExpression *ret = NULL;
   int functionId;
   switch (et) {

      // Casts
      case (EXPRESSION_TYPE_OPERATOR_CAST):
         ret = castFactory(vt, lc);
         break;

         // Operators
      case (EXPRESSION_TYPE_OPERATOR_PLUS):
      case (EXPRESSION_TYPE_OPERATOR_MINUS):
      case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
      case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
      case (EXPRESSION_TYPE_OPERATOR_CONCAT):
      case (EXPRESSION_TYPE_OPERATOR_MOD):
      case (EXPRESSION_TYPE_OPERATOR_NOT):
      case (EXPRESSION_TYPE_OPERATOR_IS_NULL):
      case (EXPRESSION_TYPE_OPERATOR_EXISTS):
      case (EXPRESSION_TYPE_OPERATOR_UNARY_MINUS):
         ret = ExpressionUtil::operatorFactory(et, lc, rc);
         break;

         // Comparisons
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
      case (EXPRESSION_TYPE_COMPARE_LIKE):
      case (EXPRESSION_TYPE_COMPARE_STARTSWITH):
      case (EXPRESSION_TYPE_COMPARE_IN):
      case (EXPRESSION_TYPE_COMPARE_NOTDISTINCT):
         ret = ExpressionUtil::comparisonFactory(obj, et, lc, rc);
         break;

         // Conjunctions
      case (EXPRESSION_TYPE_CONJUNCTION_AND):
      case (EXPRESSION_TYPE_CONJUNCTION_OR):
         ret = ExpressionUtil::conjunctionFactory(et, lc, rc);
         break;

         // Functions and pseudo-functions
      case (EXPRESSION_TYPE_FUNCTION):
         // add the function id
         functionId = obj.valueForKey("FUNCTION_ID").asInt();
         ret = functionFactory(functionId, args);

         if (ret == nullptr) {
            std::string nameString;
            if (obj.hasNonNullKey("NAME")) {
               nameString = obj.valueForKey("NAME").asStr();
            } else {
               nameString = "?";
            }
            raiseFunctionFactoryError(nameString, functionId, args);
         }
         break;

      case (EXPRESSION_TYPE_VALUE_VECTOR):
         // Parse whatever is needed out of obj and pass the pieces to inListFactory
         // to make it easier to unit test independently of the parsing.
         // The first argument is used as the list element type.
         // If the ValueType of the list builder expression needs to be "ARRAY" or something else,
         // a separate element type attribute will have to be serialized and passed in here.
         ret = vectorFactory(vt, args);
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
      case (EXPRESSION_TYPE_VALUE_SCALAR):
         ret = new ScalarValueExpression(lc);
         break;
      case (EXPRESSION_TYPE_HASH_RANGE):
         ret = hashRangeFactory(obj);
         break;
      case (EXPRESSION_TYPE_OPERATOR_CASE_WHEN):
         ret = caseWhenFactory(vt, lc, rc);
         break;
      case (EXPRESSION_TYPE_OPERATOR_ALTERNATIVE):
         ret = new OperatorAlternativeExpression(lc, rc);
         break;

         // Subquery
      case (EXPRESSION_TYPE_ROW_SUBQUERY):
      case (EXPRESSION_TYPE_SELECT_SUBQUERY):
         ret = subqueryFactory(et, obj, args);
         break;

         // must handle all known expressions in this factory
      default:
         throwSerializableEEException(
                 "Invalid ExpressionType '%s' (%d) requested from factory",
                 expressionToString(et).c_str(), (int)et);
   }
   ret->setValueType(vt);
   ret->setValueSize(vs);
   // written thusly to ease testing/inspecting return content.
   VOLT_TRACE("Created expression %p", ret);
   return ret;
}

/** convert the enumerated value type into a concrete c type for the
 *  operator expression templated ctors */
AbstractExpression* ExpressionUtil::operatorFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc) {
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

     case (EXPRESSION_TYPE_OPERATOR_EXISTS):
         ret = new OperatorExistsExpression(lc);
         break;

     case (EXPRESSION_TYPE_OPERATOR_UNARY_MINUS):
         ret = new OperatorUnaryMinusExpression(lc);
         break;

     case (EXPRESSION_TYPE_OPERATOR_MOD):
       throw SerializableEEException("Mod operator is not yet supported.");

     case (EXPRESSION_TYPE_OPERATOR_CONCAT):
       throw SerializableEEException("Concat operator not yet supported.");

     default:
       throw SerializableEEException("operator ctor helper out of sync");
   }
   return ret;
}

boost::shared_array<int> ExpressionUtil::convertIfAllTupleValues(
      const std::vector<voltdb::AbstractExpression*>& expressions) {
    size_t cnt = expressions.size();
    boost::shared_array<int> ret(new int[cnt]);
    for (int i = 0; i < cnt; ++i) {
        voltdb::TupleValueExpression* casted =
          dynamic_cast<voltdb::TupleValueExpression*>(expressions[i]);
        if (casted == NULL) {
            return boost::shared_array<int>();
        }
        ret[i] = casted->getColumnId();
    }
    return ret;
}

boost::shared_array<int> ExpressionUtil::convertIfAllParameterValues(
      const std::vector<voltdb::AbstractExpression*> &expressions) {
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

void ExpressionUtil::extractTupleValuesColumnIdx(const AbstractExpression* expr, std::vector<int> &columnIds) {
   if (expr != nullptr) {
      if(expr->getExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
         auto const* tve = dynamic_cast<const TupleValueExpression*>(expr);
         vassert(tve != NULL);
         columnIds.emplace_back(tve->getColumnId());
      } else {
         ExpressionUtil::extractTupleValuesColumnIdx(expr->getLeft(), columnIds);
         ExpressionUtil::extractTupleValuesColumnIdx(expr->getRight(), columnIds);
         for(auto const* e : expr->getArgs()) {
            ExpressionUtil::extractTupleValuesColumnIdx(e, columnIds);
         }
      }
   }
}

std::vector<int> ExpressionUtil::extractTupleValuesColumnIdx(const AbstractExpression* expr) {
   std::vector<int> columnIds;
   extractTupleValuesColumnIdx(expr, columnIds);
   return columnIds;
}

void ExpressionUtil::loadIndexedExprsFromJson(
      std::vector<AbstractExpression*>& indexed_exprs, const std::string& jsonarraystring) {
    PlannerDomRoot domRoot(jsonarraystring.c_str());
    PlannerDomValue expressionsArray = domRoot();
    for (int i = 0; i < expressionsArray.arrayLen(); i++) {
        PlannerDomValue exprValue = expressionsArray.valueAtIndex(i);
        AbstractExpression *expr = AbstractExpression::buildExpressionTree(exprValue);
        indexed_exprs.push_back(expr);
    }
}

AbstractExpression* ExpressionUtil::loadExpressionFromJson(const std::string& jsonstring) {
    PlannerDomRoot domRoot(jsonstring.c_str());
    return AbstractExpression::buildExpressionTree(domRoot());
}

OperatorIsNullExpression* ExpressionUtil::columnIsNull(const int tableIndex, const int valueIndex) {
   return new OperatorIsNullExpression(new TupleValueExpression(tableIndex, valueIndex));
}

OperatorNotExpression* ExpressionUtil::columnNotNull(const int tableIndex, const int valueIndex) {
   return new OperatorNotExpression(columnIsNull(tableIndex, valueIndex));
}

}
