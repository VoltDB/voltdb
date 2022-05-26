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

package org.voltdb.utils;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.types.IndexType;

/**
 * A collection of classes used for estimating the bytewise size of a database,
 * given a catalog.
 *
 * Currently used by the Compiler Report.
 *
 */
public abstract class CatalogSizing {
    public final static int MAX_BYTES_PER_UTF8_CHARACTER = 4;

    /**
     * Base class for raw catalog sizes.
     */
    public static class CatalogItemSizeBase {
        public long widthMin;
        public long widthMax;

        public CatalogItemSizeBase() {
            this(0, 0);
        }

        public CatalogItemSizeBase(long widthMin, long widthMax) {
            this.widthMin = widthMin;
            this.widthMax = widthMax;
        }
    }

    /**
     * Catalog sizes and cardinality estimate.
     */
    public static class CatalogItemSize extends CatalogItemSizeBase {
        public final long cardinality;

        public CatalogItemSize(long cardinality) {
            super();
            this.cardinality = cardinality;
        }

        public CatalogItemSize(long widthMin, long widthMax, long cardinality) {
            super(widthMin, widthMax);
            this.cardinality = cardinality;
        }

        public long getItemCount() {
            return 1;
        }
    }

    public static class CatalogItemSizeRollup extends CatalogItemSize {
        public long itemCount;

        public CatalogItemSizeRollup(long cardinality, long itemCount) {
            super(cardinality);
            this.itemCount = itemCount;
        }

        @Override
        public long getItemCount() {
            return itemCount;
        }
    }

    /**
     * Raw index sizes.
     */
    public static class IndexSize extends CatalogItemSize {
        public final Index index;
        public final String name;

        public IndexSize(Index index, long widthMin, long widthMax, long cardinality) {
            super(widthMin, widthMax, cardinality);
            this.index = index;
            this.name = index.getTypeName().toLowerCase();
        }
    }

    /**
     * List of catalog item sizes with roll-up support.
     *
     * @param <T>
     */
    public static class CatalogItemSizeList<T extends CatalogItemSize> extends ArrayList<T> {
        private static final long serialVersionUID = -6846163291201059792L;

        public CatalogItemSizeRollup rollup(long cardinality) {
            CatalogItemSizeRollup rollupSize = new CatalogItemSizeRollup(cardinality, 0);
            for (T size: this) {
                rollupSize.widthMin  += (size.widthMin * size.cardinality);
                rollupSize.widthMax  += (size.widthMax * size.cardinality);
                rollupSize.itemCount += size.getItemCount();
            }
            return rollupSize;
        }
    }

    /**
     * Raw table sizes.
     */
    public static class TableSize extends CatalogItemSize {
        public final Table table;
        public final String name;
        public final boolean isView;
        public final CatalogItemSizeList<IndexSize> indexSizes;

        public TableSize(
                Table table,
                boolean isView,
                long widthMin,
                long widthMax,
                long cardinality)
        {
            super(widthMin, widthMax, cardinality);
            this.table = table;
            this.name = table.getTypeName().toLowerCase();
            this.isView = isView;
            this.indexSizes = new CatalogItemSizeList<IndexSize>();
        }

        public void addIndex(Index index, long widthMin, long widthMax) {
            this.indexSizes.add(new IndexSize(index, widthMin, widthMax, 1));
        }

        public CatalogItemSizeRollup indexRollup() {
            return this.indexSizes.rollup(this.cardinality);
        }
    }

    /**
     * Container of raw database size numbers.
     */
    public static class DatabaseSizes {
        public final CatalogItemSizeList<TableSize> tableSizes = new CatalogItemSizeList<TableSize>();
        public final CatalogItemSizeList<TableSize> viewSizes = new CatalogItemSizeList<TableSize>();
        public long indexCount = 0;

        public void addTable(TableSize tableSize) {
            if (tableSize.isView) {
                this.viewSizes.add(tableSize);
            }
            else {
                this.tableSizes.add(tableSize);
            }
            this.indexCount += tableSize.indexSizes.size();
        }

        public CatalogItemSizeRollup tableRollup() {
            return this.tableSizes.rollup(1);
        }

        public CatalogItemSizeRollup viewRollup() {
            return this.viewSizes.rollup(1);
        }

        public CatalogItemSizeRollup indexRollup() {
            CatalogItemSizeList<IndexSize> indexSizes = new CatalogItemSizeList<IndexSize>();
            for (TableSize tsize: this.tableSizes) {
                for (IndexSize isize: tsize.indexSizes) {
                    indexSizes.add(isize);
                }
            }
            for (TableSize vsize: this.viewSizes) {
                for (IndexSize isize: vsize.indexSizes) {
                    indexSizes.add(isize);
                }
            }
            return indexSizes.rollup(1);
        }
    }

    private static int roundedAllocationSize(int min, int contentSize) {
        int bufferSize = min;
        while (bufferSize < contentSize) {
            int increment = bufferSize / 2;
            bufferSize += increment;
            if (bufferSize >= contentSize) {
                break;
            }
            bufferSize += increment;
        }
        return bufferSize;
    }

    private static int getVariableColumnSize(VoltType type, int capacity, int dataSize, boolean forIndex, boolean isNullable) {
        assert(capacity >= 0);
        assert(dataSize >= 0);
        // Smaller capacities get fully consumed (plus 1 byte).
        if (capacity < 64) {
            return capacity + 1;
        }

        // Indexes get 8 byte pointers rather than replicate large data.
        if (forIndex) {
            if (type != VoltType.GEOGRAPHY) {
                return 8;
            }
            else {
                // Instances of GEOGRAPHY are not stored in indices.
                // Geospatial indexes decompose them into cells, which
                // are accounted for separately in the indexes that contain them.
                return 0;
            }
        }

        // For Nullable
        if (isNullable) {
            return 8;
        }

        // Larger capacities use pooled buffers sized in powers of 2 or values halfway
        // between powers of 2.
        // The rounded buffer size includes an object length of 4 bytes and
        // an 8-byte backpointer used in compaction.
        int content = 4 + 8 + dataSize;
        int bufferSize = roundedAllocationSize(8, content);
        // There is also has an 8-byte pointer in the tuple
        // and an 8-byte StringRef indirection pointer.
        return bufferSize + 8 + 8;
    }

    public static int testOnlyAllocationSizeForObject(int dataSize) {
        // See the comments in getVariableColumnSize for the significance of
        // these adjustments.
        int content = 4 + 8 + dataSize;
        if (content <= 48) {
            // Short-cut calculations for sizes that are not used in the
            // catalog sizing.
            return roundedAllocationSize(16, content);
        }
        // Otherwise exercise as much of the catalog sizing code path as
        // possible but strip out any adjustments that are not directly a
        // result of allocation rounding.
        // See the comments in getVariableColumnSize for the significance of
        // these adjustments.
        return getVariableColumnSize(VoltType.VARBINARY, 64, dataSize, false, false) - 8 - 8;
    }

    private static CatalogItemSizeBase getColumnsSize(List<Column> columns, boolean forIndex, boolean bAdjustForDrAA) {
        // See http://voltdb.com/docs/PlanningGuide/ChapMemoryRecs.php
        CatalogItemSizeBase csize = new CatalogItemSizeBase();
        for (Column column: columns) {
            VoltType ctype = VoltType.get((byte)column.getType());
            boolean isNullable = column.getNullable();
            if (ctype.isVariableLength()) {
                int capacity = column.getSize();

                if (ctype == VoltType.STRING && !column.getInbytes()) {
                    capacity *= MAX_BYTES_PER_UTF8_CHARACTER;
                }

                csize.widthMin += getVariableColumnSize(ctype, capacity, 0, forIndex, isNullable);
                csize.widthMax += getVariableColumnSize(ctype, capacity, capacity, forIndex, false);
            }
            else {
                // Fixed type - use the fixed size.
                csize.widthMin += ctype.getLengthInBytesForFixedTypes();
                csize.widthMax += ctype.getLengthInBytesForFixedTypes();
            }
        }

        //For DR active active enabled account for additional timestamp column for conflict detection.
        if (bAdjustForDrAA) {
            csize.widthMin += 8;
            csize.widthMax += 8;
        }
        return csize;
    }

    private static CatalogItemSizeBase getIndexSize(Index index) {

        // this is sizeof(CompactingMap::TreeNode), not counting template parameter KeyValuePair.
        final long TREE_MAP_ENTRY_OVERHEAD = 32;
        final long TUPLE_PTR_SIZE = 8;

        // All index types consume the space taken by the column data,
        // except that 8 byte pointers references replace large var... data.
        // Additional overhead is determined by the index type.
        CatalogMap<ColumnRef> columnRefsMap = index.getColumns();
        List<Column> indexColumns = new ArrayList<Column>(columnRefsMap.size());
        for (ColumnRef columnRef: columnRefsMap) {
            indexColumns.add(columnRef.getColumn());
        }
        //For index Size dont count the DR AA conflict column.
        CatalogItemSizeBase isize = getColumnsSize(indexColumns, true, false);
        if (index.getType() == IndexType.HASH_TABLE.getValue()) {
            // Hash index overhead follows this documented formula:
            //   w=column width, r=row count
            //      (((2 * r) + 1) * 8) + ((w + 32) * r)
            // This can be reduced to the following:
            //      (w + 48) * r + 8
            // For approximation purposes the constant +8 is ignorable.
            isize.widthMin += 48;
            isize.widthMax += 48;
        }
        else if (index.getType() == IndexType.COVERING_CELL_INDEX.getValue()) {
            // Covering cell indexes are implemented in the EE with two maps:
            //
            // [1 entry per table row] tuple address -> fixed-size array of 8 cell ids
            // [1-8 entries per table row]   cell id -> tuple address
            //
            // The polygon value is not referenced at all in the index, just the tuple address.
            // The call to getColumnsSize above purposely omits the size of the pointer to
            // the geography value for this reason.
            //
            // Other columns in the index are included, so if in the future we decide to support
            // multi-component geospatial indexes to optimize predicates like
            // "WHERE id = 10 and contains(geog, ?)", then this code would not need to change.

            final long MIN_CELLS = 1;
            final long MAX_CELLS = 8;
            final long CELL_SIZE = 8;
            final long TUPLE_MAP_ENTRY = TREE_MAP_ENTRY_OVERHEAD + TUPLE_PTR_SIZE + MAX_CELLS * CELL_SIZE;
            final long CELL_MAP_ENTRY = TREE_MAP_ENTRY_OVERHEAD + CELL_SIZE + TUPLE_PTR_SIZE;

            isize.widthMin += TUPLE_MAP_ENTRY + MIN_CELLS * CELL_MAP_ENTRY;
            isize.widthMax += TUPLE_MAP_ENTRY + MAX_CELLS * CELL_MAP_ENTRY;
        }
        else {
            // Tree indexes have a 40 byte overhead per row.
            isize.widthMin += TREE_MAP_ENTRY_OVERHEAD + TUPLE_PTR_SIZE;
            isize.widthMax += TREE_MAP_ENTRY_OVERHEAD + TUPLE_PTR_SIZE;
        }

        return isize;
    }

    private static TableSize getTableSize(Table table, boolean bActiveActiveEnabled) {
        // The cardinality is the estimated tuple count or an arbitrary number
        // if not estimated.
        long cardinality = table.getEstimatedtuplecount();
        if (cardinality <= 0) {
            cardinality = 1000;
        }
        //Do we need to adjust for DR-AA?
        boolean bAdjustForDrAA = (table.getIsdred() && bActiveActiveEnabled);

        // Add up the column widths.
        CatalogMap<Column> columnsMap = table.getColumns();
        List<Column> columns = new ArrayList<Column>(columnsMap.size());
        for (Column column: columnsMap) {
            columns.add(column);
        }
        CatalogItemSizeBase csize = getColumnsSize(columns, false, bAdjustForDrAA);

        boolean isView = table.getMaterializer() != null;
        TableSize tsize = new TableSize(table, isView, csize.widthMin, csize.widthMax, cardinality);

        // Add the table indexes.
        CatalogMap<Index> indexes = table.getIndexes();
        for (Index index: indexes) {
            CatalogItemSizeBase isize = getIndexSize(index);
            tsize.addIndex(index, isize.widthMin, isize.widthMax);
        }

        return tsize;
    }

    /**
     * Produce a sizing of all significant database objects.
     * @param dbCatalog  database catalog
     * @param isXDCR Is XDCR enabled
     * @return  database size result object tree
     */
    public static DatabaseSizes getCatalogSizes(Database dbCatalog, boolean isXDCR) {
        DatabaseSizes dbSizes = new DatabaseSizes();
        for (Table table: dbCatalog.getTables()) {
            dbSizes.addTable(getTableSize(table, isXDCR));
        }
        return dbSizes;
    }
}
