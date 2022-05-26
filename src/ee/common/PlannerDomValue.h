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

#pragma once

#include "common/SerializableEEException.h"
#include "json/json.h"
#include <cstdio>
#include <cstdlib>
#include <inttypes.h>

namespace voltdb {

    /**
     * Represents a JSON value in a parser-library-neutral kind of way. It throws
     * VoltDB-style exceptions when things are amiss and should be otherwise pretty
     * simple to figure out how to use. See plannodes or expressions for examples.
     */
   class PlannerDomValue {
      Json::Value const m_value;
      public:
      PlannerDomValue(Json::Value const &value) : m_value(value) {}

      int32_t asInt() const {
         if (m_value.isNull()) {
            throw SerializableEEException("PlannerDomValue: int value is null");
         } else if (m_value.isInt()) {
            return m_value.asInt();
         } else if (m_value.isString()) {
            return (int32_t) strtoimax(m_value.asCString(), NULL, 10);
         }
         throw SerializableEEException("PlannerDomValue: int value is not an integer");
         return 0;
      }

      int64_t asInt64() const {
         if (m_value.isNull()) {
            throw SerializableEEException("PlannerDomValue: int64 value is null");
         } else if (m_value.isInt64()) {
            return m_value.asInt64();
         } else if (m_value.isInt()) {
            return m_value.asInt();
         } else if (m_value.isString()) {
            return (int64_t) strtoimax(m_value.asCString(), NULL, 10);
         }
         throw SerializableEEException("PlannerDomValue: int64 value is non-integral");
         return 0;
      }

      double asDouble() const {
         if (m_value.isNull()) {
            throw SerializableEEException("PlannerDomValue: double value is null");
         } else if (m_value.isDouble()) {
            return m_value.asDouble();
         } else if (m_value.isInt()) {
            return m_value.asInt();
         } else if (m_value.isInt64()) {
            return m_value.asInt64();
         } else if (m_value.isString()) {
            return std::strtod(m_value.asCString(), NULL);
         }
         throw SerializableEEException("PlannerDomValue: double value is not a number");
         return 0;
      }

      bool asBool() const {
         if (m_value.isNull() || ! m_value.isBool()) {
            throw SerializableEEException("PlannerDomValue: value is null or not a bool");
         }
         return m_value.asBool();
      }

      std::string asStr() const {
         if (m_value.isNull() || ! m_value.isString()) {
            throw SerializableEEException("PlannerDomValue: value is null or not a string");
         }
         return m_value.asString();
      }

      bool hasKey(const char *key) const {
         return m_value.isMember(key);
      }

      bool hasNonNullKey(const char *key) const {
         return hasKey(key) && ! m_value[key].isNull();
      }

      PlannerDomValue valueForKey(const char *key) const {
         auto const value = m_value[key];
         if (value.isNull()) {
            throwSerializableEEException("PlannerDomValue: %s key is null or missing", key);
         }
         return {PlannerDomValue(value)};
      }

      int arrayLen() const {
         if (! m_value.isArray()) {
            throw SerializableEEException("PlannerDomValue: value is not an array");
         }
         return m_value.size();
      }

      PlannerDomValue valueAtIndex(int index) const {
         if (! m_value.isArray()) {
            throw SerializableEEException("PlannerDomValue: value is not an array");
         }
         return {m_value[index]};
      }
   };

   /**
    * Class that parses the JSON document and provides the root.
    * Also owns the memory, as it's sole member var is not a reference, but a value.
    * This means if you're still using the DOM when this object gets popped off the
    * stack, bad things might happen. Best to use the DOM and be done with it.
    */
   class PlannerDomRoot {
      public:
         PlannerDomRoot(const PlannerDomRoot& other) = delete;
         PlannerDomRoot& operator=(const PlannerDomRoot& other) = delete;
         PlannerDomRoot(const char *json) : m_document(fromJSONString(json)) { }
         bool isNull() const {
            return m_document.isNull();
         }
         PlannerDomValue operator()() const {
            return PlannerDomValue(m_document);
         }
      private:
         Json::Value const m_document;
         static Json::Value fromJSONString(char const* json);
   };
}

