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

#include <sstream>

#include "existssubqueryexpression.h"

#include "common/debuglog.h"
#include "expressions/subqueryexpression.h"

namespace voltdb {

ExistsSubqueryExpression::ExistsSubqueryExpression(AbstractExpression* subqueryExpression) :
    AbstractExpression(EXPRESSION_TYPE_OPERATOR_EXISTS, subqueryExpression, NULL) {
    VOLT_TRACE("ExistsSubqueryExpression");
    SubqueryExpression* se = dynamic_cast<SubqueryExpression*> (subqueryExpression);
    assert(se != NULL);
    se->setParentExpressionType(EXPRESSION_TYPE_OPERATOR_EXISTS);
}

std::string ExistsSubqueryExpression::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "ExistsSubqueryExpression";
    return (buffer.str());
}

}

