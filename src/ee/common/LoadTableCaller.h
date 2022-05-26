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

#ifndef VOLTDB_EE_COMMON_LOADTABLECALLER_H_
#define VOLTDB_EE_COMMON_LOADTABLECALLER_H_

#include "stdint.h"
#include "common/debuglog.h"
#include "common/TupleSchema.h"

namespace voltdb {

/**
 * Class to describe the behavior of a load table as expected by the caller.
 *
 * This class is mirrored in the java code with an enum of the same name.
 */
class LoadTableCaller {
public:
    // Enum used to identify all of the load table callers
    enum Id : int8_t {
        // Snapshot restore which can either report or throw when a unique violation is encountered
        SNAPSHOT_REPORT_UNIQ_VIOLATIONS = 0,
        SNAPSHOT_THROW_ON_UNIQ_VIOLATION,
        // Loading tables during the snapshot phase of DR. Does not inculde migrate column
        DR,
        // Used during balance partitions. Currently ignores the migrate column since that is partition specific
        BALANCE_PARTITIONS,
        // External client invocation of load table. Never provides hidden columns
        CLIENT,
        // Internal EE caller
        INTERNAL,
        // Total count of load table callers
        ID_COUNT
    };

    static inline const LoadTableCaller &get(Id id) {
        vassert(id >=0 && id < ID_COUNT);
        return s_callers[id];
    }

    inline Id getId() const {
        return m_id;
    }

    /**
     * @return If true any unique violations encountered will be returned to the caller if false then a unique
     * constraint violation should be thrown
     */
    inline bool returnConflictRows() const {
        return m_returnUniqueViolations;
    }

    /**
     * @return true if DR producer should append inserted tuples to binary log
     */
    inline bool shouldDrStream() const {
        return m_shouldDRStream;
    }

    /**
     * @return the expected column count of the table being loaded for the given schema
     */
    inline uint16_t getExpectedColumnCount(TupleSchema *schema) const {
        uint16_t hiddenColumnCount = schema->hiddenColumnCount();

        if (hiddenColumnCount > 0) {
            switch(m_id) {
            case CLIENT:
                hiddenColumnCount = 0;
                break;
            case DR:
            case BALANCE_PARTITIONS:
                if (schema->hasHiddenColumn(HiddenColumn::MIGRATE_TXN)) {
                    --hiddenColumnCount;
                }
                break;
            default:
                break;
            }
        }

        return schema->columnCount() + hiddenColumnCount;
    }

    /**
     * @return true if the default value for the hidden column should be used and not included in the load table
     */
    inline bool useDefaultValue(HiddenColumn::Type columnType) const {
        switch (m_id) {
        case CLIENT:
            if (columnType == HiddenColumn::XDCR_TIMESTAMP) return true;
            /* fallthrough */
        case DR:
        case BALANCE_PARTITIONS:
            return columnType == HiddenColumn::MIGRATE_TXN;
        default:
            return false;
        }
    }

private:
    static const LoadTableCaller s_callers[ID_COUNT];

    const Id m_id;
    const bool m_returnUniqueViolations;
    const bool m_shouldDRStream;

    /**
     * @param returnUniqueViolations If true any unique violations encountered will be returned to the caller if false
     *      then a unique constraint violation will be thrown
     * @param shouldDrStream If true any inserted tuples will be appended to the dr buffer if dr is enabled
     */
    LoadTableCaller(Id id, bool returnUniqueViolations, bool shouldDRStream) :
        m_id(id), m_returnUniqueViolations(returnUniqueViolations), m_shouldDRStream(shouldDRStream)
    {
        vassert(id >=0 && id < ID_COUNT);
    }
};

}
#endif /* VOLTDB_EE_COMMON_LOADTABLECALLER_H_ */
