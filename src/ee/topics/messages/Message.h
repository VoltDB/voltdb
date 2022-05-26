/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

namespace voltdb { namespace topics {

/**
 * Base response message for all of the classes which are serialized as part of a response
 */
class ResponseComponent {
public:

    virtual ~ResponseComponent() {};

    /**
     * Write an NValue as a string
     *
     * @param nullToEmpty: If true null values will be converted to empty strings upon serialization. Default false
     */
    static inline void writeString(const NValue &value, SerializeOutput &out, bool nullToEmpty = false) {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tVARCHAR);
        int length = nullToEmpty ? 0 : -1;
        const char *data = nullptr;
        if (!value.isNull()) {
            data = ValuePeeker::peekObject_withoutNull(value, &length);
        }
        out.writeShort(static_cast<int16_t>(length));
        if (length > 0) {
            out.writeBytes(data, length);
        }
    }

    /**
     * Write an NValue as byte array
     */
    static inline void writeBytes(const NValue &value, SerializeOutput &out, bool nullToEmpty = false) {
        vassert(ValuePeeker::peekValueType(value) == ValueType::tVARBINARY);
        if (nullToEmpty && value.isNull()) {
            out.writeInt(0);
        } else {
            value.serializeTo(out);
        }
    }

protected:
    /**
     * Write this response to out for the specified version
     */
    virtual void write(const int16_t version, SerializeOutput& out) const = 0;

    /**
     * Write the given error code to out
     */
    inline void writeError(SerializeOutput& out) const {
        // Always write no error since this layer cannot return errors
        out.writeShort(0);
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
 * Base class for responses which holds the version of the response and handles throttle time serialization
 */
class Response : protected ResponseComponent {

public:
    /**
     * Return true if this response is an error response
     */
    virtual bool isError() const { return false; }
};

} }
