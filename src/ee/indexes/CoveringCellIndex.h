/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

#include <array>

#include "s2geo/s2regioncoverer.h"

#include "common/tabletuple.h"
#include "indexes/tableindex.h"
#include "indexes/indexkey.h"
#include "structures/CompactingMap.h"

namespace voltdb {

class PersistentTable;

/**
 * The class CoveringCellIndex is used to accelerate queries that use
 * the CONTAINS function which tests to see if a point is contained by
 * a polygon.
 *
 * This index is created in SQL by executing a CREATE TABLE statement on
 * a GEOGRAPHY (i.e., a polygon) column.  The planner will select an
 * index of this type when it finds qualifying filters that use
 * CONTAINS.
 *
 * This index type uses cell coverings provided by S2 to approximate
 * polygons.  As such, a point may be in the cell covering, but not in
 * the polygon itself.  So results from this index need to be further
 * filtered by actually evaluating CONTAINS.
 *
 * Under the hood this index uses two maps:
 * - One map from (cell ID, tuple address) to tuple address
 * - One map from a tuple address to an array of cell IDs (64-bit
 *      unsigned ints)
 *
 * Given a point, the first map facilitates scanning the index for
 * potentially containing polygons.  The point is converted to a cell
 * ID, and this cell (and all its containing cells) are located in the
 * cell map.
 *
 * The second map, the tuple map, facilitates deletion of polygons
 * from the index, so we do not need to recompute cell coverings when
 * polygons are deleted.  (Computation of a cell covering is
 * expensive.)
 */
class CoveringCellIndex : public TableIndex {
public:
    /** The largest number of cells in a polygon's cell covering */
    static const int MAX_CELL_COUNT = S2RegionCoverer::kDefaultMaxCells; // 8

    /** Type names for the cell map */
    using CellMapEntry = PointerKeyValuePair<IntsKey<1>>;
    using CellKeyType = CellMapEntry::first_type;
    using CellKeyComparator = CellKeyType::KeyComparator;
    using CellMapType = CompactingMap<CellMapEntry, CellKeyComparator>;
    using CellMapIterator = CellMapType::iterator;
    using CellMapRange = std::pair<CellMapIterator, CellMapIterator>;

    /** Type names for the tuple map */
    using TupleMapEntry = NormalKeyValuePair<IntsKey<1>, std::array<uint64_t, MAX_CELL_COUNT>>;
    using TupleKeyType = TupleMapEntry::first_type;
    using TupleValueType = TupleMapEntry::second_type;
    using TupleKeyComparator = TupleKeyType::KeyComparator;
    using TupleMapType = CompactingMap<TupleMapEntry, TupleKeyComparator>;
    using TupleMapIterator = TupleMapType::iterator;
    using TupleMapRange = std::pair<TupleMapIterator, TupleMapIterator>;
private:
    /** a map from cell ID to tuple address */
    CellMapType m_cellEntries;

    /** a map from tuple address to cell ID */
    TupleMapType m_tupleEntries;

    /** The index of the GEOGRAPHY column that is indexed  */
    int32_t m_columnIndex;
    /**
     * Given a tuple from the indexed table, extract the polygon from it.
     */
    bool getPolygonFromTuple(const TableTuple *tuple, Polygon* poly) const;

    TableIndex* cloneEmptyNonCountingTreeIndex() const override {
        throwFatalException("Primary key index discovered to be non-unique.");
    }

    void moveToKeyOrGreater(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToKeyOrGreater which has no implementation");
    }

    bool moveToGreaterThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToGreaterThanKey which has no implementation");
    }

    void moveToLessThanKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToLessThanKey which has no implementation");
    }

    void moveToKeyOrLess(TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToKeyOrLess which has no implementation");
    }

    void moveToBeforePriorEntry(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToBeforePriorEntry which has no implementation");
    }

    void moveToPriorEntry(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToPriorEntry which has no implementation");
    }

    void moveToEnd(bool begin, IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method moveToEnd which has no implementation");
    }

    TableTuple nextValue(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method nextValue which has no implementation");
    }

    bool advanceToNextKey(IndexCursor& cursor) const override {
        throwFatalException("Invoked TableIndex virtual method advanceToNextKey which has no implementation");
    }

    TableTuple uniqueMatchingTuple(const TableTuple &searchTuple) const override {
        throwFatalException("Invoked TableIndex virtual method uniqueMatchingTuple which has no use on a non-unique index");
    }

    int64_t getCounterGET(const TableTuple *searchKey, bool isUpper, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterGET which has no implementation");
    }

    int64_t getCounterLET(const TableTuple *searchKey, bool isUpper, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method getCounterLET which has no implementation");
    }

    bool moveToRankTuple(int64_t denseRank, bool forward, IndexCursor& cursor) const override {
        throwFatalException("Invoked non-countable TableIndex virtual method moveToRankTuple which has no implementation");
    }
public:
    /**
     * This constructor is the same as for the other index types.
     */
    CoveringCellIndex(const TupleSchema *keySchema, const TableIndexScheme &scheme) :
        TableIndex(keySchema, scheme)
        , m_cellEntries(false, CellKeyComparator(keySchema))
        , m_tupleEntries(true, TupleKeyComparator(nullptr))
        , m_columnIndex(scheme.columnIndices[0]) {
        vassert(scheme.columnIndices.size() == 1);
    }

    /**
     * A do-nothing, conformant destructor.
     */
    virtual ~CoveringCellIndex();

    /**
     * All keys are fixed size.
     */
    bool keyUsesNonInlinedMemory() const override {
        return false;
    }

    /**
     * Given a search key tuple (always one field of type
     * GEOGRAPHY_POINT), move the cursor to the first containing cell.
     */
    bool moveToCoveringCell(const TableTuple* searchKey, IndexCursor &cursor) const override;

    /**
     * Given a scan that has begun with a call to moveToCoveringCell,
     * returns a tuple containing a polygon that may contain the point
     * in the search key.
     */
    TableTuple nextValueAtKey(IndexCursor& cursor) const override;

    /**
     * Return the number of polygons that are indexed.
     * (Excludes rows in the table with null polygons.
     */
    size_t getSize() const override {
        return m_tupleEntries.size();
    }

    /**
     * An estimate of the amount of memory used by this index.  Result
     * seems to be dependent on the number of blocks that
     * CompactingMap has allocated?
     */
    int64_t getMemoryEstimate() const override {
        return m_tupleEntries.bytesAllocated() + m_cellEntries.bytesAllocated();
    }

    /**
     * The name of this type of index
     */
    std::string getTypeName() const override {
        return "CoveringCellIndex";
    }

    /**
     * Verifies that index matches what is in the table and vice
     * versa.  This can be slow, so this is only for testing.
     */
    bool checkValidityForTest(PersistentTable* table, std::string* reasonInvalid) const;

    /**
     * Used for equality search.  Not supported for this kind of index.
     */
    bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const override {
        throwFatalException("Invoked moveToKey on index %s which is unsupported on geospatial indexes",
                getName().c_str());
    }

    /**
     * Used for resolving conflicts with unique indexes when applying
     * binary log data.  Unneeded here.
     */
    bool moveToKeyByTuple(const TableTuple* searchTuple, IndexCursor &cursor) const override {
        throwFatalException("Invoked moveToKeyByTuple on index %s which is unsupported on geospatial indexes",
                getName().c_str());
    }

    /**
     * Used by index count executor.  This doesn't really make sense for this kind of index.
     */
    bool hasKey(const TableTuple *searchKey) const override {
        throwFatalException("Invoked hasKey on index %s which is unsupported on geospatial indexes",
                getName().c_str());
    }

    /**
     * A structure used to report stats about this index for testing.
     */
    struct StatsForTest {
        int32_t numPolygons = 0;
        int32_t numCells = 0;
        double polygonsArea = 0;
        double cellsArea = 0;
        StatsForTest() = default;
    };

    /**
     * A structure used to report stats about this index for testing.
     */
    StatsForTest getStatsForTest(PersistentTable *table) const;

 protected:

    /**
     * Invoked by superclass.  These indexes are not unique so
     * conflict tuple is not used.
     */
    void addEntryDo(const TableTuple *tuple, TableTuple *conflictTuple) override;

    /**
     * Invoked by superclass.  Removes the tuple with the give data
     * address from the index.
     */
    bool deleteEntryDo(const TableTuple *tuple) override;

    /**
     * This method is invoked when compacting the table.  The index
     * keys stay the same, but the tuple addresses change.
     */
    bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple, const TableTuple &originalTuple) override;

    /**
     * Used to detect if there are UNIQUE constraint conflicts.
     * Unneeded for this type of index.
     */
    bool existsDo(const TableTuple* values) const override {
        throwFatalException("Invoked method exists on index %s which is unsupported on geospatial indexes",
                getName().c_str());
    }

    /**
     * Used when rows are updated to check if an index change is needed.
     */
    bool checkForIndexChangeDo(const TableTuple *lhs, const TableTuple *rhs) const override;
};

} // end namespace voltdb

