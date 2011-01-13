/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
#include "storage/ConstraintFailureException.h"
#include "storage/persistenttable.h"
#include "storage/table.h"
#include <cassert>

namespace voltdb {
/*
 * @param table Table that the update or insert was performed on
 * @param tuple Tuple that was being inserted or updated
 * @param otherTuple updated tuple values or null.
 * @param type Type of constraint that was violated
 */
ConstraintFailureException::ConstraintFailureException(
        PersistentTable *table,
        TableTuple tuple,
        TableTuple otherTuple,
        ConstraintType type) :
            SQLException(
                    SQLException::integrity_constraint_violation,
                    "Attempted violation of constraint",
                    voltdb::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION),
            m_table(table), m_tuple(tuple), m_otherTuple(otherTuple), m_type(type) {
        assert(table);
        assert(!tuple.isNullTuple());
}

void ConstraintFailureException::p_serialize(ReferenceSerializeOutput *output) {
    SQLException::p_serialize(output);
    output->writeInt(m_type);
    output->writeTextString(m_table->name());
    std::size_t tableSizePosition = output->reserveBytes(4);
    TableTuple tuples[] = { m_tuple, m_otherTuple };
    if (m_otherTuple.isNullTuple()) {
        m_table->serializeTupleTo(*output, tuples, 1);
    } else {
        m_table->serializeTupleTo(*output, tuples, 2);
    }
    output->writeIntAt(tableSizePosition, static_cast<int32_t>(output->position() - tableSizePosition - 4));
}
ConstraintFailureException::~ConstraintFailureException() {
    // TODO Auto-generated destructor stub
}

}
