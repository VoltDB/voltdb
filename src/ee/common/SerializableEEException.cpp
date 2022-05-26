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

#include "common/debuglog.h"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include "execution/VoltDBEngine.h"

namespace voltdb {

#ifdef VOLT_DEBUG_ENABLED
static const char* translateVoltEEExceptionTypeToString(VoltEEExceptionType exceptionType) {
    switch(exceptionType) {
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_NONE:
            return "VOLT_EE_EXCEPTION_TYPE_NONE";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION:
            return "VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SQL:
            return "VOLT_EE_EXCEPTION_TYPE_SQL";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION:
            return "VOLT_EE_EXCEPTION_TYPE_CONSTRAINT_VIOLATION";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INTERRUPT:
            return "VOLT_EE_EXCEPTION_TYPE_INTERRUPT";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_TXN_RESTART:
            return "VOLT_EE_EXCEPTION_TYPE_TXN_RESTART";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_TXN_TERMINATION:
            return "VOLT_EE_EXCEPTION_TYPE_TXN_TERMINATION";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_SPECIFIED:
            return "VOLT_EE_EXCEPTION_TYPE_SPECIFIED";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_GENERIC:
            return "VOLT_EE_EXCEPTION_TYPE_GENERIC";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_TXN_MISPARTITIONED:
            return "VOLT_EE_EXCEPTION_TYPE_TXN_MISPARTITIONED";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE:
            return "VOLT_EE_EXCEPTION_TYPE_REPLICATED_TABLE";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_DR_TABLE_NOT_FOUND:
            return "VOLT_EE_EXCEPTION_TYPE_DR_TABLE_NOT_FOUND";
        case VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE:
            return "VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE";
        default:
            return "UNKNOWN";
    }
}
#endif

#ifdef NDEBUG
static inline std::string const enrich(std::string const& a) noexcept {
    return a;
}
#else   // On DEBUG build, enrich our exception message with stack trace of the place it is thrown.
static inline std::string enrich(std::string src) {
    return src.append("\nSTACK TRACE:")
        .append(voltdb::StackTrace::stringStackTrace("\t"))
        .substr(0, 2048);
}
#endif

SerializableEEException::SerializableEEException(VoltEEExceptionType exceptionType, std::string const& message) :
    std::runtime_error(enrich(message)), m_exceptionType(exceptionType), m_message(what()) {
    VOLT_DEBUG("Created SerializableEEException: type: %s message: %s",
               translateVoltEEExceptionTypeToString(exceptionType), message.c_str());
}

SerializableEEException::SerializableEEException(std::string const& message) :
    std::runtime_error(enrich(message)),
    m_exceptionType(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION),
    m_message(what()) {
    VOLT_DEBUG("Created SerializableEEException: default type, %s", message.c_str());
}

void SerializableEEException::serialize(ReferenceSerializeOutput *output) const {
    const std::size_t lengthPosition = output->reserveBytes(sizeof(int32_t));
    output->writeByte(static_cast<int8_t>(m_exceptionType));
    const char *messageBytes = m_message.c_str();
    const std::size_t messageLength = m_message.length();
    output->writeInt(messageLength);
    output->writeBytes(messageBytes, messageLength);
    p_serialize(output);
    if (m_exceptionType == VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION)
        output->writeInt(ENGINE_ERRORCODE_ERROR);
    output->writeIntAt(lengthPosition, output->position() - (lengthPosition + sizeof(int32_t)));
}

}
