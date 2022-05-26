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
#pragma once
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "common/ValueFactory.hpp"

namespace voltdb {

/*
 * Expression for collecting the various elements of an "IN LIST" for passing to the IN comparison
 * operator as a single ARRAY-valued NValue.
 * It is always the rhs of an IN expression like "col IN (0, -1, ?)", especially useful when the
 * IN filter is not index-optimized and when the list element expressions are not all constants.
 */
   class VectorExpression : public AbstractExpression {
      public:
         VectorExpression(ValueType elementType, const std::vector<AbstractExpression *>& arguments)
            : AbstractExpression(EXPRESSION_TYPE_VALUE_VECTOR), m_args(arguments) {
               m_inList = ValueFactory::getArrayValueFromSizeAndType(arguments.size(), elementType);
            }

         virtual ~VectorExpression() {
            for (auto const* expr : m_args) {
               delete expr;
            }
            m_inList.free();
         }

         virtual bool hasParameter() const {
            return m_args.cend() !=
               std::find_if(m_args.cbegin(), m_args.cend(), [](AbstractExpression const* expr) {
                     return expr->hasParameter();
                     });
         }

         NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
            //TODO: Could make this vector a member, if the memory management implications
            // (of the NValue internal state) were clear -- is there a penalty for longer-lived
            // NValues that outweighs the current per-eval allocation penalty?
            std::vector<NValue> nValues(m_args.size());
            for (int i = 0; i < m_args.size(); ++i) {
               nValues[i] = m_args[i]->eval(tuple1, tuple2);
            }
            m_inList.setArrayElements(nValues);
            return m_inList;
         }

         std::string debugInfo(const std::string &spacer) const {
            return spacer + "VectorExpression\n";
         }

      private:
         const std::vector<AbstractExpression *> m_args;
         NValue m_inList;
   };
}
