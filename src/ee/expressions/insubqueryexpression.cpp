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
#include "insubqueryexpression.h"

#include "common/debuglog.h"

namespace voltdb {

InSubqueryExpression::InSubqueryExpression(AbstractExpression* inVectorExpression, AbstractExpression* subqueryExpression) :
    AbstractExpression(EXPRESSION_TYPE_COMPARE_IN, inVectorExpression, subqueryExpression) {
    VOLT_TRACE("InSubqueryExpression");
    assert(subqueryExpression != NULL);
}

std::string InSubqueryExpression::debugInfo(const std::string &spacer) const {
    std::ostringstream buffer;
    buffer << spacer << "InSubqueryExpression";
    return (buffer.str());
}

}

