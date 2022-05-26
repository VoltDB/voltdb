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

#ifndef VOLTDB_EE_COMMON_HIDDENCOLUMN_H_
#define VOLTDB_EE_COMMON_HIDDENCOLUMN_H_

#include "stdint.h"
#include "common/NValue.hpp"

namespace voltdb {

/**
 * Helper class for using hidden columns
 */
class HiddenColumn {
public:
    enum Type : uint8_t {
        XDCR_TIMESTAMP = 0,
        MIGRATE_TXN,
        VIEW_COUNT,
        MAX_HIDDEN_COUNT
    };

    /**
     * Get the default NValue for the given hidden column type
     */
    static NValue getDefaultValue(Type type);

    /**
     * Get the name of the hidden column
     */
    static const char *getName(Type type);
};

}

#endif /* VOLTDB_EE_COMMON_HIDDENCOLUMN_H_ */
