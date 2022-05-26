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

#include "common/executorcontext.hpp"
#include "common/HiddenColumn.h"
#include "common/ValueFactory.hpp"

namespace voltdb {
NValue HiddenColumn::getDefaultValue(HiddenColumn::Type columnType) {
    switch (columnType) {
    case HiddenColumn::MIGRATE_TXN:
        return NValue::getNullValue(ValueType::tBIGINT);
    case HiddenColumn::XDCR_TIMESTAMP:
        return ValueFactory::getBigIntValue(ExecutorContext::getExecutorContext()->currentDRTimestamp());
    default:
        // Unsupported hidden column type passed in
        vassert(false);
        return NValue::getNullValue(ValueType::tBIGINT);
    }
}

const char *HiddenColumn::getName(HiddenColumn::Type columnType) {
    switch (columnType) {
    case HiddenColumn::MIGRATE_TXN:
        return "migrate_column";
    case HiddenColumn::XDCR_TIMESTAMP:
        return "dr_clusterid_timestamp";
    case HiddenColumn::VIEW_COUNT:
        return "count_star";
    default:
        // Unsupported hidden column type passed in
        vassert(false);
        return "UNKNOWN";
    }
}

}
