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

#include <sstream>

#include "operatorexpression.h"

#include "common/executorcontext.hpp"

#include "storage/table.h"


namespace voltdb {

NValue OperatorExistsExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // Execute the subquery and get its subquery id
    vassert(m_left != NULL);
    NValue lnv = m_left->eval(tuple1, tuple2);
    int subqueryId = ValuePeeker::peekInteger(lnv);

    // Get the subquery context

    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();

    // The EXISTS (SELECT inner_expr ...) evaluates as follows:
    // The subquery produces a row => TRUE
    // The subquery produces an empty result set => FALSE
    Table* outputTable = exeContext->getSubqueryOutputTable(subqueryId);
    vassert(outputTable != NULL);
    if (outputTable->activeTupleCount() > 0) {
        return NValue::getTrue();
    } else {
        return NValue::getFalse();
    }
}

}
