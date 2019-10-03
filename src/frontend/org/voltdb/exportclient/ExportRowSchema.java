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

package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.voltcore.utils.DeferredSerialization;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.utils.CatalogUtil;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * {@code ExportRowSchema} is a specialized form of {@code ExportRow}
 * that only has column definitions but no column values. It has its
 * own specific serialization format, to be used as header metadata
 * in a {@code BinaryDeque<M>} application.
 */
public class ExportRowSchema extends ExportRow implements DeferredSerialization {

    public long initialGenerationId;
    // Serialized schema data is preceded by fixed header
    public static final int EXPORT_BUFFER_VERSION = 1;
    private static final int EXPORT_SCHEMA_HEADER_BYTES = 1 + // export buffer version
            8 + // initial generation id
            8 + // generation id
            4 + // partition id
            4; // schema size

    // Schema columns always contain metadata columns
    private static final String VOLT_TRANSACTION_ID = "VOLT_TRANSACTION_ID";
    private static final String VOLT_EXPORT_TIMESTAMP = "VOLT_EXPORT_TIMESTAMP";
    private static final String VOLT_EXPORT_SEQUENCE_NUMBER = "VOLT_EXPORT_SEQUENCE_NUMBER";
    private static final String VOLT_PARTITION_ID = "VOLT_PARTITION_ID";
    private static final String VOLT_SITE_ID = "VOLT_SITE_ID";
    private static final String VOLT_EXPORT_OPERATION = "VOLT_EXPORT_OPERATION";

    private static final List<String> META_COL_NAMES = ImmutableList.of(
            VOLT_TRANSACTION_ID,
            VOLT_EXPORT_TIMESTAMP,
            VOLT_EXPORT_SEQUENCE_NUMBER,
            VOLT_PARTITION_ID,
            VOLT_SITE_ID,
            VOLT_EXPORT_OPERATION);

    private static final List<VoltType> META_COL_TYPES = ImmutableList.of(
            VoltType.BIGINT,
            VoltType.BIGINT,
            VoltType.BIGINT,
            VoltType.BIGINT,
            VoltType.BIGINT,
            VoltType.TINYINT);

    private static final List<Integer> META_COL_SIZES = ImmutableList.of(
            Long.BYTES,
            Long.BYTES,
            Long.BYTES,
            Long.BYTES,
            Long.BYTES,
            Byte.BYTES);

    // private constructor
    private ExportRowSchema(String tableName, List<String> columnNames, List<VoltType> t, List<Integer> l,
            int partitionId, long initialGeneration, long generation) {
        super(tableName, columnNames, t, l, new Object[] {}, null, -1, partitionId, generation);
        initialGenerationId = initialGeneration;

    }

    /**
     * Create a {@code ExportRowSchema} from a Catalog {@code Table} and additional information.
     *
     * @param table Catalog {@code Table}
     * @param partitionId the partition id
     * @param generationId the generation id
     * @return
     */
    public static ExportRowSchema create(Table table, int partitionId, long initialGenerationId, long generationId) {

        List<String> colNames = new LinkedList<>();
        List<VoltType> colTypes = new LinkedList<>();
        List<Integer> colSizes = new LinkedList<>();

        // Add the meta-column definitions
        colNames.addAll(META_COL_NAMES);
        colTypes.addAll(META_COL_TYPES);
        colSizes.addAll(META_COL_SIZES);

        // Add the Table columns
        for (Column c : CatalogUtil.getSortedCatalogItems(table.getColumns(), "index")) {
            colNames.add(c.getName());
            colTypes.add(VoltType.get((byte) c.getType()));
            colSizes.add(c.getSize());
        }

        return new ExportRowSchema(table.getTypeName(), colNames, colTypes, colSizes,
                partitionId, initialGenerationId, generationId);
    }

    /**
     * Returns true of other is the same schema (excluding generation and tableName).
     *
     * NOTE: ignores the generation, as catalog updates on export updates the generation
     * even if no changes occur on the table.
     *
     * NOTE: ignores the tableName to allow comparing different tables.
     *
     * @param other the other schema to compare
     * @return true if same schema regardless of generations and tableName
     */
    public boolean sameSchema(ExportRowSchema other) {

        if (other == this) {
            return true;
        }
        if (!(other instanceof ExportRowSchema)) {
            return false;
        }
        if (this.names.size() != other.names.size()) {
            return false;
        }

        // Note: valid instances assume those different members have same size
        // Note: schemas are not identical if columns have different order
        Iterator<String> itNames = this.names.iterator();
        Iterator<VoltType> itTypes = this.types.iterator();
        Iterator<Integer> itSizes = this.lengths.iterator();

        Iterator<String> itONames = other.names.iterator();
        Iterator<VoltType> itOTypes = other.types.iterator();
        Iterator<Integer> itOSizes = other.lengths.iterator();

        while(itNames.hasNext()) {
            if (!itNames.next().equals(itONames.next())) {
                return false;
            }
            if (!itTypes.next().equals(itOTypes.next())) {
                return false;
            }
            if (!itSizes.next().equals(itOSizes.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Deserialize {@code ExportRowSchema} from {@code ByteBuffer}
     *
     * @param buf the input buffer, with position and byte order preset
     * @return the deserialized object
     * @throws IOException
     */
    public static ExportRowSchema deserialize(ByteBuffer buf) throws IOException {
        try {
            byte version = buf.get();
            if (version != EXPORT_BUFFER_VERSION) {
                throw new IllegalArgumentException("Illegal version, expected: " + EXPORT_BUFFER_VERSION
                        + ", got: " + version);
            }
            long initialGenId = buf.getLong();
            long genId = buf.getLong();
            int partitionId = buf.getInt();
            int size = buf.getInt();
            int position = buf.position();

            String tableName = ExportRow.decodeString(buf);
            List<String> colNames = new ArrayList<>();
            List<VoltType> colTypes = new ArrayList<>();
            List<Integer> colLengths = new ArrayList<>();
            while (buf.position() < position + size) {
                colNames.add(ExportRow.decodeString(buf));
                colTypes.add(VoltType.get(buf.get()));
                colLengths.add(buf.getInt());
            }
            return new ExportRowSchema(tableName, colNames, colTypes, colLengths, partitionId, initialGenId, genId);
        }
        catch(Exception ex) {
            throw new IOException("Failed to deserialize schema " + ex, ex);
        }
    }

    @Override
    public void serialize(ByteBuffer buf) throws IOException {
        try {
            int required = getSerializedSize();
            if (buf.remaining() < required) {
                throw new IllegalArgumentException("Insufficient space, required: " + required
                        + ", available: " + buf.remaining());
            }
            buf.put((byte)EXPORT_BUFFER_VERSION);
            buf.putLong(this.initialGenerationId);
            buf.putLong(this.generation);
            buf.putInt(this.partitionId);
            buf.putInt(required - EXPORT_SCHEMA_HEADER_BYTES); // size of schema
            buf.putInt(this.tableName.length());
            buf.put(this.tableName.getBytes(Constants.UTF8ENCODING));

            Iterator<String> itNames = this.names.iterator();
            Iterator<VoltType> itTypes = this.types.iterator();
            Iterator<Integer> itSizes = this.lengths.iterator();

            while(itNames.hasNext()) {
                String colName = itNames.next();
                VoltType colType = itTypes.next();
                Integer colSize = itSizes.next();

                buf.putInt(colName.length());
                buf.put(colName.getBytes(Constants.UTF8ENCODING));
                buf.put(colType.getValue());
                buf.putInt(colSize);
            }
        }
        catch (Exception ex) {
            throw new IOException("Failed to serialize schema " + ex, ex);
        }
    }

    @Override
    public void cancel() {
        // *void*
    }

    @Override
    public int getSerializedSize() throws IOException {
        int size = 0;
        for (String colName : this.names) {
            size += 4 /*colName len*/ + colName.length() + 1 /*value type*/ + 4 /*value len*/;
        }
        return EXPORT_SCHEMA_HEADER_BYTES +
                4 /*table name length*/ + this.tableName.length() +
                size /*schema size*/;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.tableName)
                .append(":")
                .append(this.partitionId)
                .append(":")
                .append(this.initialGenerationId)
                .append(":")
                .append(this.generation)
                .append(" - [");

        Iterator<String> itNames = this.names.iterator();
        Iterator<VoltType> itTypes = this.types.iterator();
        Iterator<Integer> itSizes = this.lengths.iterator();
        while(itNames.hasNext()) {
            sb.append(itNames.next())
              .append(":")
              .append(itTypes.next())
              .append(":")
              .append(itSizes.next())
              .append((itNames.hasNext()) ? ", " : "]");
        }
        return sb.toString();
    }

    public VoltTable toVoltTable() {
        assert names.size() != 0 : "creating VoltTable from empty schema";
        ArrayList<ColumnInfo> cols = new ArrayList<>(names.size());

        Iterator<String> itNames = this.names.iterator();
        Iterator<VoltType> itTypes = this.types.iterator();

        while(itNames.hasNext()) {
            cols.add(new ColumnInfo(itNames.next(), itTypes.next()));
        }
        return new VoltTable(cols.toArray(new ColumnInfo[0]));
    }
}
