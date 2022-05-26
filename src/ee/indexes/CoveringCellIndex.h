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

#ifndef COVERINGCELLINDEX_H
#define COVERINGCELLINDEX_H

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
    typedef PointerKeyValuePair<IntsKey<1> > CellMapEntry;
    typedef CellMapEntry::first_type CellKeyType;
    typedef CellKeyType::KeyComparator CellKeyComparator;
    typedef CompactingMap<CellMapEntry, CellKeyComparator> CellMapType;
    typedef CellMapType::iterator CellMapIterator;
    typedef std::pair<CellMapIterator, CellMapIterator> CellMapRange;

    /** Type names for the tuple map */
    typedef NormalKeyValuePair<IntsKey<1>, std::array<uint64_t, MAX_CELL_COUNT>> TupleMapEntry;
    typedef TupleMapEntry::first_type TupleKeyType;
    typedef TupleMapEntry::second_type TupleValueType;
    typedef TupleKeyType::KeyComparator TupleKeyComparator;
    typedef CompactingMap<TupleMapEntry, TupleKeyComparator> TupleMapType;
    typedef TupleMapType::iterator TupleMapIterator;
    typedef std::pair<TupleMapIterator, TupleMapIterator> TupleMapRange;

    /**
     * This constructor is the same as for the other index types.
     */
    CoveringCellIndex(const TupleSchema *keySchema,
                      const TableIndexScheme &scheme)
        : TableIndex(keySchema, scheme)
        , m_cellEntries(false, CellKeyComparator(keySchema))
        , m_tupleEntries(true, TupleKeyComparator(NULL))
        , m_columnIndex(scheme.columnIndices[0])
    {
        vassert(scheme.columnIndices.size() == 1);
    }

    /**
     * A do-nothing, conformant destructor.
     */
    virtual ~CoveringCellIndex();

    /**
     * All keys are fixed size.
     */
    virtual bool keyUsesNonInlinedMemory() const {
        return false;
    }

    /**
     * Given a search key tuple (always one field of type
     * GEOGRAPHY_POINT), move the cursor to the first containing cell.
     */
    virtual bool moveToCoveringCell(const TableTuple* searchKey,
                                    IndexCursor &cursor) const;

    /**
     * Given a scan that has begun with a call to moveToCoveringCell,
     * returns a tuple containing a polygon that may contain the point
     * in the search key.
     */
    virtual TableTuple nextValueAtKey(IndexCursor& cursor) const;

    /**
     * Return the number of polygons that are indexed.
     * (Excludes rows in the table with null polygons.
     */
    virtual size_t getSize() const {
        return static_cast<size_t>(m_tupleEntries.size());
    }

    /**
     * An estimate of the amount of memory used by this index.  Result
     * seems to be dependent on the number of blocks that
     * CompactingMap has allocated?
     */
    virtual int64_t getMemoryEstimate() const {
        return m_tupleEntries.bytesAllocated() + m_cellEntries.bytesAllocated();
    }

    /**
     * The name of this type of index
     */
    virtual std::string getTypeName() const {
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
    virtual bool moveToKey(const TableTuple *searchKey, IndexCursor& cursor) const {
        throwFatalException("Invoked moveToKey on index %s which is unsupported on geospatial indexes",
                            getName().c_str());
    }

    /**
     * Used for resolving conflicts with unique indexes when applying
     * binary log data.  Unneeded here.
     */
    virtual bool moveToKeyByTuple(const TableTuple* searchTuple, IndexCursor &cursor) const {
        throwFatalException("Invoked moveToKeyByTuple on index %s which is unsupported on geospatial indexes",
                            getName().c_str());
    }

    /**
     * Used by index count executor.  This doesn't really make sense for this kind of index.
     */
    virtual bool hasKey(const TableTuple *searchKey) const {
        throwFatalException("Invoked hasKey on index %s which is unsupported on geospatial indexes",
                            getName().c_str());
    }

    /**
     * A structure used to report stats about this index for testing.
     */
    struct StatsForTest {
        int32_t numPolygons;
        int32_t numCells;

        double polygonsArea;
        double cellsArea;

        StatsForTest()
        : numPolygons(0)
        , numCells(0)
        , polygonsArea(0.0)
        , cellsArea(0.0)
        {
        }
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
    virtual void addEntryDo(const TableTuple *tuple, TableTuple *conflictTuple);

    /**
     * Invoked by superclass.  Removes the tuple with the give data
     * address from the index.
     */
    virtual bool deleteEntryDo(const TableTuple *tuple);

    /**
     * This method is invoked when compacting the table.  The index
     * keys stay the same, but the tuple addresses change.
     */
    virtual bool replaceEntryNoKeyChangeDo(const TableTuple &destinationTuple,
                                           const TableTuple &originalTuple);

    /**
     * Used to detect if there are UNIQUE constraint conflicts.
     * Unneeded for this type of index.
     */
    virtual bool existsDo(const TableTuple* values) const {
        throwFatalException("Invoked method exists on index %s which is unsupported on geospatial indexes",
                            getName().c_str());
    }

    /**
     * Used when rows are updated to check if an index change is needed.
     */
    virtual bool checkForIndexChangeDo(const TableTuple *lhs, const TableTuple *rhs) const;

 private:

    /**
     * Given a tuple from the indexed table, extract the polygon from it.
     */
    bool getPolygonFromTuple(const TableTuple *tuple, Polygon* poly) const;

    /** a map from cell ID to tuple address */
    CellMapType m_cellEntries;

    /** a map from tuple address to cell ID */
    TupleMapType m_tupleEntries;

    /** The index of the GEOGRAPHY column that is indexed  */
    int32_t m_columnIndex;
};

} // end namespace voltdb

#endif
