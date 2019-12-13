/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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


#pragma once

#include <sstream>

#include "common/NValue.hpp"
#include "common/SerializableEEException.h"
#include "common/serializeio.h"
#include "common/ValueFactory.hpp"
#include "common/ValuePeeker.hpp"

#include "kipling/messages/Error.h"

namespace voltdb { namespace kipling {

/**
 *  Base classes for request messages
 */
class RequestComponent {

protected:
    inline void checkBounds(SerializeInputBE& in, size_t size) {
        if (in.remaining() < size) {
            std::ostringstream message;
            message << "Unable to read " << size << " only " << in.remaining() << " remaining: " << typeid(this).name();
            message.flush();
            throw new SerializableEEException(VoltEEExceptionType::VOLT_EE_EXCEPTION_TYPE_INVALID_MESSAGE,
                    message.str());
        }
    }

    // Wrappers around read methods from SerializeInput that always perform bounds checking
    inline int8_t readByte(SerializeInputBE& in) {
        checkBounds(in, sizeof(int8_t));
        return in.readByte();
    }

    inline int16_t readShort(SerializeInputBE& in) {
        checkBounds(in, sizeof(int16_t));
        return in.readShort();
    }

    inline int32_t readInt(SerializeInputBE& in) {
        checkBounds(in, sizeof(int32_t));
        return in.readInt();
    }

    inline int64_t readLong(SerializeInputBE& in) {
        checkBounds(in, sizeof(int64_t));
        return in.readLong();
    }

    /**
     * Read a string out of in. This does not copy the data but just refers to the data from in
     */
    inline NValue readString(SerializeInputBE& in) {
        int16_t length = readShort(in);
        if (length < 0) {
            ValueFactory::getNullStringValue();
        }

        checkBounds(in, length);
        return ValueFactory::getTempStringValue(in.getRawPointer(length), length);
    }

    /***
     * Read a byte array from in. This does not copy the data but just refers to the data from in
     */
    inline NValue readBytes(SerializeInputBE& in) {
        int32_t length = readInt(in);
        if (length < 0) {
            ValueFactory::getNullBinaryValue();
        }

        checkBounds(in, length);
        return ValueFactory::getTempBinaryValue(in.getRawPointer(length), length);
    }

    /**
     * Read a variable number of RequestComponents and store them in a vector
     */
    template <typename RequestType>
    inline void readRequestComponents(const int16_t version, SerializeInputBE& in, std::vector<RequestType> vector) {
        const int32_t count = readInt(in);
        for (int i = 0; i < count; ++i) {
            vector.push_back(RequestType(version, in));
        }
    }
};

/**
 * Base class group requests which includes the groupId and version of the request
 */
class GroupRequest : protected RequestComponent {

public:
    GroupRequest(const int16_t version, const NValue& groupId) : m_version(version), m_groupId(groupId) {}

    /**
     * return the version of this request message
     */
    inline const int16_t version() const {
        return m_version;
    }

    /**
     * return the groupId for this request
     */
    inline const NValue& groupId() const {
        return m_groupId;
    }

private:
    const int16_t m_version;
    const NValue& m_groupId;
};

// Base classes for response messages

/**
 * Base response message for all of the classes which are serialized as part of a response
 */
class ResponseComponent {
public:

    virtual ~ResponseComponent() {};

    /**
     * Write this response instance as a response to request to out
     */
    void write(GroupRequest& request, SerializeOutput& out) {
        write(request.version(), out);
    }

protected:
    /**
     * Write this response to out for the specified version
     */
    virtual void write(const int16_t version, SerializeOutput& out) const = 0;

    inline void writeError(const Error error, SerializeOutput& out) const {
        out.writeShort(static_cast<int16_t>(error));
    }

    /**
     * Write an NValue as a string
     */
    inline void writeString(const NValue &value, SerializeOutput &out) const {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tVARCHAR);
        int length = -1;
        const char *data = nullptr;
        if (!value.isNull()) {
            data = ValuePeeker::peekObject(value, &length);
        }
        out.writeShort(static_cast<int16_t>(length));
        if (length > 0) {
            out.writeBytes(data, length);
        }
    }

    /**
     * Write an NValue as byte array
     */
    inline void writeBytes(const NValue &value, SerializeOutput &out) const {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tVARBINARY);
        int length = -1;
        const char *data = nullptr;
        if (!value.isNull()) {
            data = ValuePeeker::peekObject(value, &length);
        }
        out.writeInt(length);
        if (length > 0) {
            out.writeBytes(data, length);
        }
    }

    /**
     * Write a vector of classes which extend Response to out
     */
    template<class ResponseType>
    inline void writeResponses(const std::vector<ResponseType>& responses, const int16_t version,
            SerializeOutput& out) const {
        out.writeInt(responses.size());
        for (const ResponseType& response : responses) {
            response.write(version, out);
        }
    }
};

/**
 * Base class for the outer response message which has throttleTime and error member variable
 */
template<typename ResponseType>
class Response : protected ResponseComponent {

public:
    Response(Error error): m_error(error) {}
    Response() {}

    virtual void write(const int16_t version, SerializeOutput& out) const = 0;

    inline const int32_t throttleTimeMs() const {
        return m_throttleTimeMs;
    }

    inline const Error error() const {
        return m_error;
    }

    inline ResponseType& errorCode(Error error) {
        m_error = error;
        return *this;
    }

protected:
    /**
     * Utility method for writing throttle time and error
     */
    void writeCommon(const int16_t minThrottleVersion, const int16_t version, SerializeOutput& out) const {
        if (minThrottleVersion <= version) {
            out.writeInt(m_throttleTimeMs);
        }
        writeError(m_error, out);
    }

private:
    // Amount if time which this response was delayed due to throttling. EE doesn't throttle so this is always 0 here
    const int32_t m_throttleTimeMs = 0;
    // Error code for the response
    Error m_error = Error::NONE;
};

} }
