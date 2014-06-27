/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

#ifndef HSTOREINSUBQUERYEXPRESSION_H
#define HSTOREINSUBQUERYEXPRESSION_H


#include "common/NValue.hpp"
#include "expressions/abstractexpression.h"

namespace voltdb {

class NValue;

class InSubqueryExpression : public AbstractExpression {
    public:

        InSubqueryExpression(AbstractExpression* inVectorExpresion, AbstractExpression* subqueryExpression);

        NValue eval(const TableTuple *tuple1, const TableTuple *tuple2) const;

        std::string debugInfo(const std::string &spacer) const;

};

inline NValue InSubqueryExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const {
    // The outer_expr IN (SELECT inner_expr ...) evaluates as follows:
    // There is a match outer_expr = inner_expr => TRUE
    // There no match and the subquery produces any row where inner_expr is NULL => NULL
    // There no match and the subquery produces only non- NULL rows or empty => FASLE
    // The outer_expr is NULL and the subquery is empty => FASLE
    // The outer_expr is NULL and the subquery produces any row => FALSE
    return m_right->eval(tuple1, tuple2);
}

}
#endif
