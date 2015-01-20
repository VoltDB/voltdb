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

#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include "execution/VoltDBEngine.h"

#include <stdint.h>

namespace voltdb {

#ifdef VOLT_DEBUG_ENABLED
static const char* translateVoltEEExceptionTypeToString(VoltEEExceptionType exceptionType)
{
    switch(exceptionType) {
    case VOLT_EE_EXCEPTION_TYPE_NONE: return "VOLT_EE_EXCEPTION_TYPE_NONE";
    case VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION: return "VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION";
    case VOLT_EE_EXCEPTION_TYPE_SQL: return "VOLT_EE_EXCEPTION_TYPE_SQL";
    case VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION: return "VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION";
    case VOLT_EE_EXCEPTION_TYPE_INTERRUPT: return "VOLT_EE_EXCEPTION_TYPE_INTERRUPT";
    default: return "UNKNOWN";
    }
}
#endif


SerializableEEException::SerializableEEException(VoltEEExceptionType exceptionType, std::string message) :
    m_exceptionType(exceptionType), m_message(message)
{
    VOLT_DEBUG("Created SerializableEEException: type: %s message: %s",
               translateVoltEEExceptionTypeToString(exceptionType), message.c_str());
}

void SerializableEEException::serialize(ReferenceSerializeOutput *output) const {
    const std::size_t lengthPosition = output->reserveBytes(sizeof(int32_t));
    output->writeByte(static_cast<int8_t>(m_exceptionType));
    const char *messageBytes = m_message.c_str();
    const std::size_t messageLength = m_message.length();
    output->writeInt(static_cast<int32_t>(messageLength));
    output->writeBytes(messageBytes, messageLength);
    p_serialize(output);
    if (m_exceptionType == VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION)
        output->writeInt(ENGINE_ERRORCODE_ERROR);
    const int32_t length = static_cast<int32_t>(output->position() - (lengthPosition + sizeof(int32_t)));
    output->writeIntAt( lengthPosition, length);
}

SerializableEEException::~SerializableEEException() {
    // TODO Auto-generated destructor stub
}

}
