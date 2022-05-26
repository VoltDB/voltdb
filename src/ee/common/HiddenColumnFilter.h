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

#ifndef VOLTDB_EE_COMMON_HIDDENCOLUMNFILTER_H_
#define VOLTDB_EE_COMMON_HIDDENCOLUMNFILTER_H_

#include "stdint.h"
#include "common/HiddenColumn.h"
#include "common/TupleSchema.h"

namespace voltdb {

/**
 * Very basic hidden column filter which currently only can skip a single column at a specific index
 */
class HiddenColumnFilter {
public:
    // Values must match those also in java enum HiddenColumnFilterType
    enum Type : uint8_t {
        ALL = 0,
        NONE = 1,
        EXCLUDE_MIGRATE = 2
    };

    inline static const HiddenColumnFilter create(Type type, const TupleSchema *schema) {
        uint8_t skip;
        uint8_t reduceCount = 0;

        switch (type) {
        case ALL:
            return HiddenColumnFilter(TupleSchema::UNSET_HIDDEN_COLUMN, 0);
            break;
        case EXCLUDE_MIGRATE:
            skip = schema->getHiddenColumnIndex(HiddenColumn::MIGRATE_TXN);
            reduceCount = skip != TupleSchema::UNSET_HIDDEN_COLUMN;
            break;
        default:
            vassert(false);
            /* no break */
        case NONE:
            skip = TupleSchema::UNSET_HIDDEN_COLUMN;
        }

        uint8_t hiddenCount = schema->hiddenColumnCount() - reduceCount;
        return HiddenColumnFilter(skip, hiddenCount);
    }

    inline bool include(uint16_t index) const {
        return index != m_skip && m_hiddenCount;
    }

    inline const uint8_t getHiddenColumnCount() const {
        return m_hiddenCount;
    }

private:
    HiddenColumnFilter(uint8_t skip, uint8_t hiddenCount) : m_skip(skip), m_hiddenCount(hiddenCount) {}

    const uint8_t m_skip;
    const uint8_t m_hiddenCount;
};

}

#endif /* VOLTDB_EE_COMMON_HIDDENCOLUMNFILTER_H_ */
