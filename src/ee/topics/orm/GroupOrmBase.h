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

#include "common/tabletuple.h"
#include "storage/persistenttable.h"
#include "topics/GroupTables.h"

namespace voltdb { namespace topics {

/**
 * Base class for updating tuples in a table by tracking the the changes in an external tuple and applying all changes
 * at once by calling commit()
 */
class GroupOrmBase {
public:
    virtual ~GroupOrmBase() {
        freeUpdateData();
    }

    /**
     * Return the ID of the group which this object is part of
     */
    const NValue& getGroupId() const {
        return m_groupId;
    }

    /**
     * Mark this tuple for deletion from the table
     */
    virtual void markForDelete();

    /**
     * Returns true if there are updates that need to be pushed to the table
     */
    inline bool isDirty() const {
        return m_dirty;
    }

    /**
     * Returns false if currently the table does not a have this tuple in it
     */
    inline bool isInTable() const {
        return !m_tableTuple.isNullTuple();
    }

    /**
     * Returns true if this tuple is deleted or will be deleted upon commit
     */
    inline bool isDeleted() const {
        return (isDirty() ? m_update : m_tableTuple).isNullTuple();
    }

    virtual bool operator==(const GroupOrmBase& other);

    bool operator!=(const GroupOrmBase& other) { return !(*this == other); }

protected:
    GroupOrmBase(const GroupTables& tables, TableTuple& original, const NValue& groupId) :
            m_tables(tables), m_tableTuple(original), m_update(original.getSchema()), m_dirty(false), m_groupId(groupId) {}

    GroupOrmBase(const GroupTables& tables, const NValue& groupId) :
            m_tables(tables), m_dirty(false), m_groupId(groupId) {}

    /**
     * Commit any outstanding changes to the backing table
     */
    virtual void commit(int64_t timestamp);

    /**
     * Used during initialization when a TableTuple is not provided to the construct and setTableTuple is not used
     */
    void setSchema(const TupleSchema* schema) {
        m_update.setSchema(schema);
    }

    /**
     * Set the tuple which is in the table and the schema
     */
    void setTableTuple(const TableTuple& tableTuple);

    /**
     * Set a single value at the given index
     *
     * @param index: Index at which to set the value
     * @param value: NValue to set at the given index
     */
    template <typename Index>
    void setNValue(const Index index, const NValue& value) {
        setupForUpdate();
        m_update.setNValue(static_cast<int>(index), value);
    }

    /**
     * Set multiple values at once from values
     *
     * @param values: Vector of values to set in the update tuple
     * @param startIndex: Index at which to start setting values. Default: 0
     */
    void setNValues(const std::vector<NValue>& values, int startIndex = 0);

    /**
     * Set multiple values at once from values
     *
     * @param values: Vector of values to set in the update tuple
     * @param column: Column identifier which can be cast to an int
     */
    template <class Column>
    void setNValues(const std::vector<NValue>& values, Column column) {
        setNValues(values, static_cast<int>(column));
    }

    /**
     * Return the NValue from index of the most up to date tuple. Undefined behavior if there is no tuple with data
     */
    template <class Index>
    const NValue getNValue(const Index index) const{
        vassert(!(isDirty() ? m_update : m_tableTuple).isNullTuple());
        return (isDirty() ? m_update : m_tableTuple).getNValue(static_cast<int>(index));
    }

    /**
     * Add an index to the list of indexes which need to be modified when the tuple is updated in the table.
     *
     * This method usually does not need to be called unless one of the modified entries in the tuple is part of an index.
     */
    void addUpdatedIndex(TableIndex* index) {
        m_updatedIndexes.push_back(index);
    }

    /**
     * This method is called by operator== when both instances are deleted to test if the deleted tuples are the same
     * tuple
     */
    virtual bool equalDeleted(const GroupOrmBase& other) const = 0;

    /**
     * Return the table which backs this orm instance
     */
    virtual PersistentTable* getTable() const = 0;

    /**
     * Read a string out of in. This copies the data from in
     */
    static inline NValue readString(SerializeInputBE& in) {
        int32_t length = in.readInt();
        if (length < 0) {
            return ValueFactory::getNullStringValue();
        }

        return ValueFactory::getTempStringValue(in.getRawPointer(length), length);
    }

    /**
     * Read a byte array from in. This copies the data from in
     */
    static inline NValue readBytes(SerializeInputBE& in) {
        int32_t length = in.readInt();
        if (length < 0) {
            return ValueFactory::getNullBinaryValue();
        }

        return ValueFactory::getTempBinaryValue(in.getRawPointer(length), length);
    }

    const TableTuple& getTableTuple() const {
        return m_tableTuple;
    }

    const TableTuple& getUpdateTuple() const {
        return m_update;
    }

    /**
     * Return true if commit() will perform an update when invoked
     */
    bool willUpdate() {
        return isDirty() && isInTable() && !m_update.isNullTuple();
    }

    // Class which holds all of the tables for groups
    const GroupTables& m_tables;

private:
    // Delete copy constructor and operator so that free of update tuple memory cannot be double freed
    GroupOrmBase(const GroupOrmBase&) = delete;
    void operator=(const GroupOrmBase&) = delete;

    /**
     * Initialize the update tuple if not already done
     */
    void setupForUpdate();

    /**
     * Free any data allocated for the update tuple
     */
    void freeUpdateData();

    // Tuple which is currently in the table
    TableTuple m_tableTuple;
    // Tuple which will be in the table after this instance is committed
    TableTuple m_update;
    // List of indexes which need to updated when the update is committed.
    std::vector<TableIndex*> m_updatedIndexes;
    // Whether or not this instance needs to be committed
    bool m_dirty;
    // ID of the group
    const NValue m_groupId;
};

} }
