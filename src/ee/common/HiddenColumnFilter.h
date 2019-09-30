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
        NONE = 0,
        EXCLUDE_MIGRATE = 1
    };

    inline static const HiddenColumnFilter create(Type type, const TupleSchema *schema) {
        uint8_t skip;
        switch (type) {
        case EXCLUDE_MIGRATE:
            skip = schema->getHiddenColumnIndex(HiddenColumn::Type::MIGRATE_TXN);
            break;
        default:
            vassert(false);
            /* no break */
        case NONE:
            skip = TupleSchema::UNSET_HIDDEN_COLUMN;
        }
        return HiddenColumnFilter(skip);
    }

    HiddenColumnFilter() : m_skip(TupleSchema::UNSET_HIDDEN_COLUMN) {}

    inline bool include(uint16_t index) const {
        return index != m_skip;
    }

private:
    HiddenColumnFilter(uint8_t skip) : m_skip(skip) {}

    const uint8_t m_skip;
};

}

