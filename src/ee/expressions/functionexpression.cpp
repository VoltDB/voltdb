/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

#include "expressions/constantvalueexpression.h"
#include "expressions/functionexpression.h"
#include "expressions/geofunctions.h"
#include "expressions/expressionutil.h"

namespace voltdb {

/** implement a forced SQL ERROR function (for test and example purposes) for either integer or string types **/
template<> inline NValue NValue::callUnary<FUNC_VOLT_SQL_ERROR>() const {
    const char* sqlstatecode;
    const char* msgtext;
    const ValueType type = getValueType();
    char msg_format_buffer[1024];
    char state_format_buffer[6];
    if (type == ValueType::tVARCHAR) {
        if (isNull()) {
             throw SQLException(SQLException::dynamic_sql_error,
                     "Must not ask for object length on sql null object.");
        }
        int32_t length;
        const char* buf = getObject_withoutNull(length);
        std::string valueStr(buf, length);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
        msg_format_buffer[sizeof msg_format_buffer - 1] = '\0';
        sqlstatecode = SQLException::nonspecific_error_code_for_error_forced_by_user;
        msgtext = msg_format_buffer;
    } else {
        int64_t intValue = castAsBigIntAndGetValue(); // let cast throw if invalid
        if (intValue == 0) {
            return *this;
        }
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld", (long) intValue);
        state_format_buffer[sizeof state_format_buffer - 1] = '\0';
        sqlstatecode = state_format_buffer;
        msgtext = SQLException::specific_error_specified_by_user;
    }
    throw SQLException(sqlstatecode, msgtext);
}

/** implement the 2-argument forced SQL ERROR function (for test and example purposes) */
template<> inline NValue NValue::call<FUNC_VOLT_SQL_ERROR>(const std::vector<NValue>& arguments) {
    vassert(arguments.size() == 2);
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
        snprintf(state_format_buffer, sizeof(state_format_buffer), "%05ld",
                 static_cast<long>(intValue));
        state_format_buffer[sizeof state_format_buffer - 1] = '\0';
        sqlstatecode = state_format_buffer;
    }

    const NValue& strValue = arguments[1];
    if (strValue.isNull()) {
        msg_format_buffer[0] = '\0';
    } else {
        if (strValue.getValueType() != ValueType::tVARCHAR) {
            throwCastSQLException(strValue.getValueType(), ValueType::tVARCHAR);
        }
        int32_t length;
        const char* buf = strValue.getObject_withoutNull(length);
        std::string valueStr(buf, length);
        snprintf(msg_format_buffer, sizeof(msg_format_buffer), "%s", valueStr.c_str());
        msg_format_buffer[sizeof msg_format_buffer - 1] = '\0';
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
            ConstantFunctionExpression()
               : AbstractExpression(EXPRESSION_TYPE_FUNCTION) {
               };

            NValue eval(const TableTuple*, const TableTuple*) const override {
               return NValue::callConstant<F>();
            }

            std::string debugInfo(const std::string &spacer) const override {
               std::stringstream buffer;
               buffer << spacer << "ConstantFunctionExpression " << F << std::endl;
               return (buffer.str());
            }
      };

   template<> NValue ConstantFunctionExpression<FUNC_VOLT_MIGRATING>::eval(
         const TableTuple* tuple1, const TableTuple*) const {
      // For MIGRATING(), check if we are evaluating on a migrating table (the table with a migrate target).
      if (tuple1 != NULL && tuple1->getSchema()->hasHiddenColumn(HiddenColumn::MIGRATE_TXN)) {
         // we have at most 3 hidden columns, DR Timestamp, count for view and transaction id for migrating
         // and transaction id for migrating is always the last one.
         return tuple1->getHiddenNValue( // use callUnary instead of callConstant since callConstant is a static method
               tuple1->getSchema()->getHiddenColumnIndex(HiddenColumn::MIGRATE_TXN)).callUnary<FUNC_VOLT_MIGRATING>();
      } else {
         throw SQLException(SQLException::dynamic_sql_error,
               "Can not apply MIGRATING function on non-migrating tables.");
      }
   }

   /*
    * Unary functions. (abs, upper, lower)
    */

   template <int F>
      class UnaryFunctionExpression : public AbstractExpression {
         AbstractExpression * const m_child;
         public:
         UnaryFunctionExpression(AbstractExpression *child)
            : AbstractExpression(EXPRESSION_TYPE_FUNCTION)
              , m_child(child) { }

         virtual ~UnaryFunctionExpression() {
            delete m_child;
         }

         bool hasParameter() const override {
            return m_child->hasParameter();
         }

         const std::vector<AbstractExpression*> getArgs() const override {
            return std::vector<AbstractExpression*>(1, m_child);
         }

         NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const override {
            vassert(m_child);
            return (m_child->eval(tuple1, tuple2)).callUnary<F>();
         }

         std::string debugInfo(const std::string &spacer) const override {
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
               for(auto const* expr : m_args) {
                  delete expr;
               }
            }

            virtual bool hasParameter() const override {
               for (size_t i = 0; i < m_args.size(); i++) {
                  vassert(m_args[i]);
                  if (m_args[i]->hasParameter()) {
                     return true;
                  }
               }
               return false;
            }

            const std::vector<AbstractExpression*> getArgs() const override {
               return m_args;
            }

            NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const override {
               //TODO: Could make this vector a member, if the memory management implications
               // (of the NValue internal state) were clear -- is there a penalty for longer-lived
               // NValues that outweighs the current per-eval allocation penalty?
               std::vector<NValue> nValue(m_args.size());
               for (int i = 0; i < m_args.size(); ++i) {
                  nValue[i] = m_args[i]->eval(tuple1, tuple2);
               }
               return NValue::call<F>(nValue);
            }

            std::string debugInfo(const std::string &spacer) const override {
               std::stringstream buffer;
               buffer << spacer << "GeneralFunctionExpression " << F << std::endl;
               return (buffer.str());
            }

         private:
            const std::vector<AbstractExpression *> m_args;
      };

   /*
    * User-defined scalar function.
    */
   class UserDefinedFunctionExpression : public AbstractExpression {
      public:
         UserDefinedFunctionExpression(int functionId, const std::vector<AbstractExpression *>& args)
            : AbstractExpression(EXPRESSION_TYPE_FUNCTION),
            m_functionId(functionId),
            m_args(args),
            m_engine(ExecutorContext::getEngine()) {}

         virtual ~UserDefinedFunctionExpression() {
            for(auto const* expr : m_args) {
               delete expr;
            }
         }

         bool hasParameter() const override {
            for (size_t i = 0; i < m_args.size(); i++) {
               vassert(m_args[i]);
               if (m_args[i]->hasParameter()) {
                  return true;
               }
            }
            return false;
         }

         const std::vector<AbstractExpression*> getArgs() const override {
            return m_args;
         }

         NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const override {
            std::vector<NValue> nValue(m_args.size());
            for (int i = 0; i < m_args.size(); ++i) {
               nValue[i] = m_args[i]->eval(tuple1, tuple2);
            }
            return m_engine->callJavaUserDefinedFunction(m_functionId, nValue);
         }

         std::string debugInfo(const std::string &spacer) const override {
            std::stringstream buffer;
            buffer << spacer << "UserDefinedFunctionExpression (function ID = " << m_functionId << ")" << std::endl;
            return (buffer.str());
         }

      private:
         int m_functionId;
         const std::vector<AbstractExpression*> m_args;
         // We need the help from the VoltDBEngine to initiate the call into the Java top end for UDF execution.
         // So we cache a pointer to the engine object that is tied to the current site thread for direct access.
         VoltDBEngine* m_engine;
   };
}

AbstractExpression* functionFactory(int functionId, const std::vector<AbstractExpression*>& arguments) {
   using namespace functionexpression;
   if (IS_USER_DEFINED_ID(functionId)) {
      return new UserDefinedFunctionExpression(functionId, arguments);
   }
   AbstractExpression* ret = nullptr;
   size_t nArgs = arguments.size();
   if (nArgs == 0) {
      switch(functionId) {
         case FUNC_CURRENT_TIMESTAMP:
            ret = new ConstantFunctionExpression<FUNC_CURRENT_TIMESTAMP>();
            break;
         case FUNC_PI:
            ret = new ConstantFunctionExpression<FUNC_PI>();
            break;
         case FUNC_VOLT_MIN_VALID_TIMESTAMP:
            ret = new ConstantFunctionExpression<FUNC_VOLT_MIN_VALID_TIMESTAMP>();
            break;
         case FUNC_VOLT_MAX_VALID_TIMESTAMP:
            ret = new ConstantFunctionExpression<FUNC_VOLT_MAX_VALID_TIMESTAMP>();
            break;
         case FUNC_VOLT_MIGRATING:
            ret = new ConstantFunctionExpression<FUNC_VOLT_MIGRATING>();
            break;
         default:
            return NULL;
      }
   } else if (nArgs == 1) {
      switch(functionId) {
         case FUNC_ABS:
            ret = new UnaryFunctionExpression<FUNC_ABS>(arguments[0]);
            break;
         case FUNC_CEILING:
            ret = new UnaryFunctionExpression<FUNC_CEILING>(arguments[0]);
            break;
         case FUNC_CHAR:
            ret = new UnaryFunctionExpression<FUNC_CHAR>(arguments[0]);
            break;
         case FUNC_CHAR_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_CHAR_LENGTH>(arguments[0]);
            break;
         case FUNC_EXP:
            ret = new UnaryFunctionExpression<FUNC_EXP>(arguments[0]);
            break;
         case FUNC_EXTRACT_DAY:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY>(arguments[0]);
            break;
         case FUNC_EXTRACT_DAY_OF_WEEK:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_WEEK>(arguments[0]);
            break;
         case FUNC_EXTRACT_WEEKDAY:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEKDAY>(arguments[0]);
            break;
         case FUNC_EXTRACT_DAY_OF_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_DAY_OF_YEAR>(arguments[0]);
            break;
         case FUNC_EXTRACT_HOUR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_HOUR>(arguments[0]);
            break;
         case FUNC_EXTRACT_MINUTE:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MINUTE>(arguments[0]);
            break;
         case FUNC_EXTRACT_MONTH:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_MONTH>(arguments[0]);
            break;
         case FUNC_EXTRACT_QUARTER:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_QUARTER>(arguments[0]);
            break;
         case FUNC_EXTRACT_SECOND:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_SECOND>(arguments[0]);
            break;
         case FUNC_EXTRACT_WEEK_OF_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_WEEK_OF_YEAR>(arguments[0]);
            break;
         case FUNC_EXTRACT_YEAR:
            ret = new UnaryFunctionExpression<FUNC_EXTRACT_YEAR>(arguments[0]);
            break;
         case FUNC_SINCE_EPOCH_SECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_SECOND>(arguments[0]);
            break;
         case FUNC_SINCE_EPOCH_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_MILLISECOND>(arguments[0]);
            break;
         case FUNC_SINCE_EPOCH_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_SINCE_EPOCH_MICROSECOND>(arguments[0]);
            break;
         case FUNC_TO_TIMESTAMP_SECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_SECOND>(arguments[0]);
            break;
         case FUNC_TO_TIMESTAMP_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_MILLISECOND>(arguments[0]);
            break;
         case FUNC_TO_TIMESTAMP_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_MICROSECOND>(arguments[0]);
            break;
         case FUNC_TRUNCATE_YEAR:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_YEAR>(arguments[0]);
            break;
         case FUNC_TRUNCATE_QUARTER:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_QUARTER>(arguments[0]);
            break;
         case FUNC_TRUNCATE_MONTH:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MONTH>(arguments[0]);
            break;
         case FUNC_TRUNCATE_DAY:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_DAY>(arguments[0]);
            break;
         case FUNC_TRUNCATE_HOUR:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_HOUR>(arguments[0]);
            break;
         case FUNC_TRUNCATE_MINUTE:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MINUTE>(arguments[0]);
            break;
         case FUNC_TRUNCATE_SECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_SECOND>(arguments[0]);
            break;
         case FUNC_TRUNCATE_MILLISECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MILLISECOND>(arguments[0]);
            break;
         case FUNC_TRUNCATE_MICROSECOND:
            ret = new UnaryFunctionExpression<FUNC_TRUNCATE_MICROSECOND>(arguments[0]);
            break;
            // Alias for function FUNC_TO_TIMESTAMP_SECOND
         case FUNC_VOLT_FROM_UNIXTIME:
            ret = new UnaryFunctionExpression<FUNC_TO_TIMESTAMP_SECOND>(arguments[0]);
            break;
         case FUNC_FLOOR:
            ret = new UnaryFunctionExpression<FUNC_FLOOR>(arguments[0]);
            break;
         case FUNC_OCTET_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_OCTET_LENGTH>(arguments[0]);
            break;
         case FUNC_SPACE:
            ret = new UnaryFunctionExpression<FUNC_SPACE>(arguments[0]);
            break;
         case FUNC_FOLD_LOWER:
            ret = new UnaryFunctionExpression<FUNC_FOLD_LOWER>(arguments[0]);
            break;
         case FUNC_FOLD_UPPER:
            ret = new UnaryFunctionExpression<FUNC_FOLD_UPPER>(arguments[0]);
            break;
         case FUNC_SQRT:
            ret = new UnaryFunctionExpression<FUNC_SQRT>(arguments[0]);
            break;
         case FUNC_VOLT_ARRAY_LENGTH:
            ret = new UnaryFunctionExpression<FUNC_VOLT_ARRAY_LENGTH>(arguments[0]);
            break;
         case FUNC_VOLT_BITNOT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_BITNOT>(arguments[0]);
            break;
         case FUNC_VOLT_HEX:
            ret = new UnaryFunctionExpression<FUNC_VOLT_HEX>(arguments[0]);
            break;
         case FUNC_VOLT_BIN:
            ret = new UnaryFunctionExpression<FUNC_VOLT_BIN>(arguments[0]);
            break;
         case FUNC_VOLT_POINTFROMTEXT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POINTFROMTEXT>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGONFROMTEXT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGONFROMTEXT>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGON_NUM_INTERIOR_RINGS>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGON_NUM_POINTS:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGON_NUM_POINTS>(arguments[0]);
            break;
         case FUNC_VOLT_POINT_LATITUDE:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POINT_LATITUDE>(arguments[0]);
            break;
         case FUNC_VOLT_POINT_LONGITUDE:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POINT_LONGITUDE>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGON_CENTROID:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGON_CENTROID>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGON_AREA:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGON_AREA>(arguments[0]);
            break;
         case FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_ASTEXT_GEOGRAPHY_POINT>(arguments[0]);
            break;
         case FUNC_VOLT_ASTEXT_GEOGRAPHY:
            ret = new UnaryFunctionExpression<FUNC_VOLT_ASTEXT_GEOGRAPHY>(arguments[0]);
            break;
         case FUNC_VOLT_SQL_ERROR:
            ret = new UnaryFunctionExpression<FUNC_VOLT_SQL_ERROR>(arguments[0]);
            break;
         case FUNC_LN:
            ret = new UnaryFunctionExpression<FUNC_LN>(arguments[0]);
            break;
         case FUNC_LOG10:
            ret = new UnaryFunctionExpression<FUNC_LOG10>(arguments[0]);
            break;
         case FUNC_VOLT_IS_VALID_POLYGON:
            ret = new UnaryFunctionExpression<FUNC_VOLT_IS_VALID_POLYGON>(arguments[0]);
            break;
         case FUNC_VOLT_POLYGON_INVALID_REASON:
            ret = new UnaryFunctionExpression<FUNC_VOLT_POLYGON_INVALID_REASON>(arguments[0]);
            break;
         case FUNC_VOLT_MAKE_VALID_POLYGON:
            ret = new UnaryFunctionExpression<FUNC_VOLT_MAKE_VALID_POLYGON>(arguments[0]);
            break;
         case FUNC_VOLT_VALIDPOLYGONFROMTEXT:
            ret = new UnaryFunctionExpression<FUNC_VOLT_VALIDPOLYGONFROMTEXT>(arguments[0]);
            break;
         case FUNC_VOLT_STR:
            ret = new UnaryFunctionExpression<FUNC_VOLT_STR>(arguments[0]);
            break;
         case FUNC_VOLT_IS_VALID_TIMESTAMP:
            ret = new UnaryFunctionExpression<FUNC_VOLT_IS_VALID_TIMESTAMP>(arguments[0]);
            break;
         case FUNC_SIN:
            ret = new UnaryFunctionExpression<FUNC_SIN>(arguments[0]);
            break;
         case FUNC_COS:
            ret = new UnaryFunctionExpression<FUNC_COS>(arguments[0]);
            break;
         case FUNC_TAN:
            ret = new UnaryFunctionExpression<FUNC_TAN>(arguments[0]);
            break;
         case FUNC_COT:
            ret = new UnaryFunctionExpression<FUNC_COT>(arguments[0]);
            break;
         case FUNC_CSC:
            ret = new UnaryFunctionExpression<FUNC_CSC>(arguments[0]);
            break;
         case FUNC_SEC:
            ret = new UnaryFunctionExpression<FUNC_SEC>(arguments[0]);
            break;
         case FUNC_INET_NTOA:
            ret = new UnaryFunctionExpression<FUNC_INET_NTOA>(arguments[0]);
            break;
         case FUNC_INET_ATON:
            ret = new UnaryFunctionExpression<FUNC_INET_ATON>(arguments[0]);
            break;
         case FUNC_INET6_NTOA:
            ret = new UnaryFunctionExpression<FUNC_INET6_NTOA>(arguments[0]);
            break;
         case FUNC_INET6_ATON:
            ret = new UnaryFunctionExpression<FUNC_INET6_ATON>(arguments[0]);
            break;
         case FUNC_DEGREES:
            ret = new UnaryFunctionExpression<FUNC_DEGREES>(arguments[0]);
            break;
         case FUNC_RADIANS:
            ret = new UnaryFunctionExpression<FUNC_RADIANS>(arguments[0]);
            break;
         default:
            return NULL;
      }
   } else {
      // GeneralFunctions defer deleting the arguments container until through with it.
      switch(functionId) {
         case FUNC_BITAND:
            ret = new GeneralFunctionExpression<FUNC_BITAND>(arguments);
            break;
         case FUNC_BITOR:
            ret = new GeneralFunctionExpression<FUNC_BITOR>(arguments);
            break;
         case FUNC_BITXOR:
            ret = new GeneralFunctionExpression<FUNC_BITXOR>(arguments);
            break;
         case FUNC_CONCAT:
            ret = new GeneralFunctionExpression<FUNC_CONCAT>(arguments);
            break;
         case FUNC_DECODE:
            ret = new GeneralFunctionExpression<FUNC_DECODE>(arguments);
            break;
         case FUNC_LEFT:
            ret = new GeneralFunctionExpression<FUNC_LEFT>(arguments);
            break;
         case FUNC_MOD:
            ret = new GeneralFunctionExpression<FUNC_MOD>(arguments);
            break;
         case FUNC_OVERLAY_CHAR:
            ret = new GeneralFunctionExpression<FUNC_OVERLAY_CHAR>(arguments);
            break;
         case FUNC_POSITION_CHAR:
            ret = new GeneralFunctionExpression<FUNC_POSITION_CHAR>(arguments);
            break;
         case FUNC_POWER:
            ret = new GeneralFunctionExpression<FUNC_POWER>(arguments);
            break;
         case FUNC_REPEAT:
            ret = new GeneralFunctionExpression<FUNC_REPEAT>(arguments);
            break;
         case FUNC_REPLACE:
            ret = new GeneralFunctionExpression<FUNC_REPLACE>(arguments);
            break;
         case FUNC_RIGHT:
            ret = new GeneralFunctionExpression<FUNC_RIGHT>(arguments);
            break;
         case FUNC_SUBSTRING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_SUBSTRING_CHAR>(arguments);
            break;
         case FUNC_TRIM_BOTH_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_BOTH_CHAR>(arguments);
            break;
         case FUNC_TRIM_LEADING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_LEADING_CHAR>(arguments);
            break;
         case FUNC_TRIM_TRAILING_CHAR:
            ret = new GeneralFunctionExpression<FUNC_TRIM_TRAILING_CHAR>(arguments);
            break;
         case FUNC_VOLT_ARRAY_ELEMENT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_ARRAY_ELEMENT>(arguments);
            break;
         case FUNC_VOLT_BIT_SHIFT_LEFT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_BIT_SHIFT_LEFT>(arguments);
            break;
         case FUNC_VOLT_BIT_SHIFT_RIGHT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_BIT_SHIFT_RIGHT>(arguments);
            break;
         case FUNC_VOLT_DATEADD_YEAR:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_YEAR>(arguments);
            break;
         case FUNC_VOLT_DATEADD_QUARTER:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_QUARTER>(arguments);
            break;
         case FUNC_VOLT_DATEADD_MONTH:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_MONTH>(arguments);
            break;
         case FUNC_VOLT_DATEADD_DAY:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_DAY>(arguments);
            break;
         case FUNC_VOLT_DATEADD_HOUR:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_HOUR>(arguments);
            break;
         case FUNC_VOLT_DATEADD_MINUTE:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_MINUTE>(arguments);
            break;
         case FUNC_VOLT_DATEADD_SECOND:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_SECOND>(arguments);
            break;
         case FUNC_VOLT_DATEADD_MILLISECOND:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_MILLISECOND>(arguments);
            break;
         case FUNC_VOLT_DATEADD_MICROSECOND:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DATEADD_MICROSECOND>(arguments);
            break;
         case FUNC_VOLT_FIELD:
            ret = new GeneralFunctionExpression<FUNC_VOLT_FIELD>(arguments);
            break;
         case FUNC_VOLT_FORMAT_CURRENCY:
            ret = new GeneralFunctionExpression<FUNC_VOLT_FORMAT_CURRENCY>(arguments);
            break;
         case FUNC_VOLT_STR:
            ret = new GeneralFunctionExpression<FUNC_VOLT_STR>(arguments);
            break;
         case FUNC_VOLT_ROUND:
            ret = new GeneralFunctionExpression<FUNC_VOLT_ROUND>(arguments);
            break;
         case FUNC_VOLT_REGEXP_POSITION:
            ret = new GeneralFunctionExpression<FUNC_VOLT_REGEXP_POSITION>(arguments);
            break;
         case FUNC_VOLT_SET_FIELD:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SET_FIELD>(arguments);
            break;
         case FUNC_VOLT_SQL_ERROR:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SQL_ERROR>(arguments);
            break;
         case FUNC_VOLT_SUBSTRING_CHAR_FROM:
            ret = new GeneralFunctionExpression<FUNC_VOLT_SUBSTRING_CHAR_FROM>(arguments);
            break;
         case FUNC_VOLT_CONTAINS:
            ret = new GeneralFunctionExpression<FUNC_VOLT_CONTAINS>(arguments);
            break;
         case FUNC_VOLT_DISTANCE_POINT_POINT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DISTANCE_POINT_POINT>(arguments);
            break;
         case FUNC_VOLT_DISTANCE_POLYGON_POINT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DISTANCE_POLYGON_POINT>(arguments);
            break;
         case FUNC_VOLT_DWITHIN_POINT_POINT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DWITHIN_POINT_POINT>(arguments);
            break;
         case FUNC_VOLT_DWITHIN_POLYGON_POINT:
            ret = new GeneralFunctionExpression<FUNC_VOLT_DWITHIN_POLYGON_POINT>(arguments);
            break;
          case FUNC_VOLT_TIME_WINDOW_YEAR_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_YEAR_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_YEAR_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_YEAR_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_QUARTER_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_QUARTER_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_QUARTER_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_QUARTER_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MONTH_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MONTH_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MONTH_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MONTH_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_WEEK_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_WEEK_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_WEEK_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_WEEK_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_DAY_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_DAY_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_DAY_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_DAY_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_HOUR_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_HOUR_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_HOUR_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_HOUR_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MINUTE_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MINUTE_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MINUTE_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MINUTE_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_SECOND_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_SECOND_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_SECOND_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_SECOND_END>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MILLIS_START:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MILLIS_START>(arguments);
              break;
          case FUNC_VOLT_TIME_WINDOW_MILLIS_END:
              ret = new GeneralFunctionExpression<FUNC_VOLT_TIME_WINDOW_MILLIS_END>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_YEAR:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_YEAR>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_QUARTER:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_QUARTER>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_MONTH:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_MONTH>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_WEEK:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_WEEK>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_DAY:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_DAY>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_HOUR:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_HOUR>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_MINUTE:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_MINUTE>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_SECOND:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_SECOND>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_MILLIS:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_MILLIS>(arguments);
              break;
          case FUNC_VOLT_DATETIME_DIFF_MICROS:
              ret = new GeneralFunctionExpression<FUNC_VOLT_DATETIME_DIFF_MICROS>(arguments);
              break;
         case FUNC_VOLT_FORMAT_TIMESTAMP:
            ret = new UserDefinedFunctionExpression(functionId, arguments);
            break;
         default:
            return NULL;
      }
   }
   return ret;
}


}
