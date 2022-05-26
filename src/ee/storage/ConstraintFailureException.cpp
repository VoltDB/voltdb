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
#include "storage/ConstraintFailureException.h"
#include "storage/constraintutil.h"
#include "storage/table.h"
#include <common/debuglog.h>

using namespace voltdb;
using std::string;

ConstraintFailureException::ConstraintFailureException(
        Table *table, TableTuple tuple, TableTuple otherTuple,
        ConstraintType type, PersistentTableSurgeon *surgeon) :
    SQLException(SQLException::integrity_constraint_violation,
            "Attempted violation of constraint",
            VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION),
    m_table(table),
    m_tuple(tuple),
    m_otherTuple(otherTuple),
    m_type(type),
    m_surgeon(surgeon) {
    vassert(table);
    vassert(!tuple.isNullTuple());
}

ConstraintFailureException::ConstraintFailureException(
        Table *table,
        TableTuple tuple,
        string const& message,
        PersistentTableSurgeon *surgeon) :
        SQLException(
                SQLException::integrity_constraint_violation,
                message,
                VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION),
    m_table(table),
    m_tuple(tuple),
    m_otherTuple(TableTuple()),
    m_type(CONSTRAINT_TYPE_PARTITIONING),
    m_surgeon(surgeon) {
    vassert(table);
    vassert(!tuple.isNullTuple());
}

void ConstraintFailureException::p_serialize(ReferenceSerializeOutput *output) const {
    SQLException::p_serialize(output);
    output->writeInt(m_type);
    output->writeTextString(m_table->name());
    size_t tableSizePosition = output->reserveBytes(4);
    TableTuple tuples[] = { m_tuple, m_otherTuple };
    if (m_otherTuple.isNullTuple()) {
        m_table->serializeTupleTo(*output, tuples, 1);
    } else {
        m_table->serializeTupleTo(*output, tuples, 2);
    }
    output->writeIntAt(tableSizePosition, static_cast<int32_t>(output->position() - tableSizePosition - 4));
}

ConstraintFailureException::~ConstraintFailureException() throw () {
    // if delayed tuple deallocation for serialization (by passing in tableSurgeon),
    // do cleanup here
    VOLT_DEBUG("ConstraintFailureException has table surgeon %s", ((m_surgeon!=NULL) ? "true": "false"));
    if (m_surgeon && !m_tuple.isNullTuple()) {
        m_surgeon->deleteTupleStorage(m_tuple);
    }
}

string ConstraintFailureException::message() const {
    // This should probably be an override of the << operator and then used here, but meh
    string msg = SQLException::message();
    msg.append("\nConstraint violation type: ");
    string type_string = constraintutil::getTypeName(m_type);
    msg.append(type_string);
    msg.append("\non table: ");
    msg.append(m_table->name());

    msg.append("\nNew tuple:\n\t");
    msg.append(m_tuple.debug(m_table->name()));
    if (!m_otherTuple.isNullTuple()) {
        msg.append("\nOriginal tuple:\n\t");
        msg.append(m_otherTuple.debug(m_table->name()));
    }
    msg.append("\n");
    return msg;
}
