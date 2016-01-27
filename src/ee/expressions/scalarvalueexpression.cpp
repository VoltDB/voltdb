/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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


#include "expressions/scalarvalueexpression.h"
#include "common/executorcontext.hpp"
#include "common/ValuePeeker.hpp"
#include "common/NValue.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tableiterator.h"

namespace voltdb {

voltdb::NValue ScalarValueExpression::eval(const TableTuple *tuple1, const TableTuple *tuple2) const
{
    // Execute the subquery and get its subquery id
    assert(m_left != NULL);
    NValue lnv = m_left->eval(tuple1, tuple2);
    int subqueryId = ValuePeeker::peekInteger(lnv);

    // Get the subquery context
    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
    Table* table = exeContext->getSubqueryOutputTable(subqueryId);
    assert(table != NULL);
    if (table->activeTupleCount() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    TableIterator& iterator = table->iterator();
    TableTuple tuple(table->schema());
    if (iterator.next(tuple)) {
        return tuple.getNValue(0);
    } else {
        return NValue::getNullValue(m_left->getValueType());
    }

}

std::string ScalarValueExpression::debugInfo(const std::string &spacer) const {
    return "ScalarValueExpression";
}

}
