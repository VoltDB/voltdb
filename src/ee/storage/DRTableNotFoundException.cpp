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
#include <string>
#include "common/serializeio.h"
#include "storage/DRTableNotFoundException.h"

using namespace voltdb;

DRTableNotFoundException::DRTableNotFoundException(int64_t hash, std::string const& message) :
    SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_DR_TABLE_NOT_FOUND, message),
    m_hash(hash)
{ }

void DRTableNotFoundException::p_serialize(ReferenceSerializeOutput *output) const {
    output->writeLong(m_hash);
    // placeholder for remote cluster's txn unique id and catalog version that gets added in the java layer.
    output->writeLong(-1);
    output->writeInt(-1);
}

std::string DRTableNotFoundException::message() const {
    std::string msg = SerializableEEException::message();
    msg.append(" [");
    msg.append((char *) m_hash);
    msg.append("]");
    return msg;
}

