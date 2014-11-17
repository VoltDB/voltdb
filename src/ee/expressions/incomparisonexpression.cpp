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


#include "incomparisonexpression.hpp"

namespace voltdb {

SubqueryValueExtractor::SubqueryValueExtractor(NValue value) :
    m_tuple()
{
    int subqueryId = ValuePeeker::peekInteger(value);
    ExecutorContext* exeContext = ExecutorContext::getExecutorContext();
    Table* table = exeContext->getSubqueryOutputTable(subqueryId);
    assert(table != NULL);
    if (table->activeTupleCount() > 1) {
        // throw runtime exception
        char message[256];
        snprintf(message, 256, "More than one row returned by a scalar/row subquery");
        throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
    }
    if (table->activeTupleCount() != 0) {
        m_tuple.setSchema(table->schema());
        TableIterator& iterator = table->iterator();
        iterator.next(m_tuple);
    }
}

bool SubqueryValueExtractor::isNUllOrEmpty() const
{
    if (m_tuple.isNullTuple() == false)
    {
        int size = m_tuple.getSchema()->columnCount();
        for (int i = 0; i < size; ++i)
        {
            if (m_tuple.isNull(i) == true)
            {
                    return true;
            }
        }
        return false;
    }
    return true;
}

}
