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

#ifndef PLANNERDOMVALUE_H_
#define PLANNERDOMVALUE_H_

#include "common/SerializableEEException.h"

#include "rapidjson/document.h"

#include <cstdio>
#include <cstdlib>
#include <climits>
#include <inttypes.h>

namespace voltdb {

    /**
     * Represents a JSON value in a parser-library-neutral kind of way. It throws
     * VoltDB-style exceptions when things are amiss and should be otherwise pretty
     * simple to figure out how to use. See plannodes or expressions for examples.
     *
     * It might require some fudging to move from rapidjson to jsoncpp or something,
     * but WAY less fudging than it would take to edit every bit of code that uses
     * this shim.
     */
    class PlannerDomValue {
        friend class PlannerDomRoot;
    public:

        int32_t asInt() const {
            if (m_value.IsNull()) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                              "PlannerDomValue: int value is null");
            }
            else if (m_value.IsInt()) {
                return m_value.GetInt();
            }
            else if (m_value.IsString()) {
                return (int32_t) strtoimax(m_value.GetString(), NULL, 10);
            }
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "PlannerDomValue: int value is not an integer");
        }

        int64_t asInt64() const {
            if (m_value.IsNull()) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                              "PlannerDomValue: int64 value is null");
            }
            else if (m_value.IsInt64()) {
                return m_value.GetInt64();
            }
            else if (m_value.IsInt()) {
                return m_value.GetInt();
            }
            else if (m_value.IsString()) {
                return (int64_t) strtoimax(m_value.GetString(), NULL, 10);
            }
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "PlannerDomValue: int64 value is non-integral");
        }

        double asDouble() const {
            if (m_value.IsNull()) {
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                              "PlannerDomValue: double value is null");
            }
            else if (m_value.IsDouble()) {
                return m_value.GetDouble();
            }
            else if (m_value.IsInt()) {
                return m_value.GetInt();
            }
            else if (m_value.IsInt64()) {
                return (double) m_value.GetInt64();
            }
            else if (m_value.IsString()) {
                return std::strtod(m_value.GetString(), NULL);
            }
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                                          "PlannerDomValue: double value is not a number");
        }

        bool asBool() const {
            if (m_value.IsNull() || (m_value.IsBool() == false)) {
                char msg[1024];
                snprintf(msg, 1024, "PlannerDomValue: value is null or not a bool");
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
            return m_value.GetBool();
        }

        std::string asStr() const {
            if (m_value.IsNull() || (m_value.IsString() == false)) {
                char msg[1024];
                snprintf(msg, 1024, "PlannerDomValue: value is null or not a string");
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
            return m_value.GetString();

        }

        bool hasKey(const char *key) const {
            return m_value.HasMember(key);
        }

        bool hasNonNullKey(const char *key) const {
            if (!hasKey(key)) {
                return false;
            }
            rapidjson::Value &value = m_value[key];
            return !value.IsNull();
        }

        PlannerDomValue valueForKey(const char *key) const {
            rapidjson::Value &value = m_value[key];
            if (value.IsNull()) {
                char msg[1024];
                snprintf(msg, 1024, "PlannerDomValue: %s key is null or missing", key);
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
            return PlannerDomValue(value);
        }

        int arrayLen() const {
            if (m_value.IsArray() == false) {
                char msg[1024];
                snprintf(msg, 1024, "PlannerDomValue: value is not an array");
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
            return m_value.Size();
        }

        PlannerDomValue valueAtIndex(int index) const {
            if (m_value.IsArray() == false) {
                char msg[1024];
                snprintf(msg, 1024, "PlannerDomValue: value is not an array");
                throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
            }
            return m_value[index];
        }

    private:
        PlannerDomValue(rapidjson::Value &value) : m_value(value) {}

        rapidjson::Value &m_value;
    };

    /**
     * Class that parses the JSON document and provides the root.
     * Also owns the memory, as it's sole member var is not a reference, but a value.
     * This means if you're still using the DOM when this object gets popped off the
     * stack, bad things might happen. Best to use the DOM and be done with it.
     */
    class PlannerDomRoot {
    public:
        PlannerDomRoot(const char *jsonStr) {
            m_document.Parse<0>(jsonStr);
        }

        bool isNull() {
            return m_document.IsNull();
        }

        PlannerDomValue rootObject() {
            return PlannerDomValue(m_document);
        }

    private:
        rapidjson::Document m_document;
    };
}

#endif // PLANNERDOMVALUE_H_
