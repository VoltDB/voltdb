/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
#include "common/InterruptException.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include <iostream>
#include <cassert>

using namespace voltdb;

InterruptException::InterruptException(std::string message) :
    SerializableEEException(VOLT_EE_EXCEPTION_TYPE_INTERRUPT, message) {
}

void InterruptException::p_serialize(ReferenceSerializeOutput *output) const {
}
