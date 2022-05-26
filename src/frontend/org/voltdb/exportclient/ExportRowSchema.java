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

package org.voltdb.exportclient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Topic;
import org.voltdb.serdes.FieldDescription;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.SerializationHelper;

import com.google_voltpatches.common.collect.ImmutableList;

/**
 * {@code ExportRowSchema} is a specialized form of {@code ExportRow}
 * that only has column definitions but no column values. It has its
 * own specific serialization format, to be used as header metadata
 * in a {@code BinaryDeque<M>} application.
 */
public class ExportRowSchema extends ExportRow {

    public final long initialGenerationId;
    // Serialized schema data is preceded by fixed header
    private static final byte EXPORT_BUFFER_VERSION_1 = 1;
    private static final byte EXPORT_BUFFER_VERSION_2 = 2;
    private static final byte EXPORT_BUFFER_VERSION_LATEST = EXPORT_BUFFER_VERSION_2;

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
            int partitionColumnIndex, int partitionId, long initialGeneration, long generation) {
        super(tableName, columnNames, t, l, new Object[] {}, null, partitionColumnIndex, partitionId, generation);
        initialGenerationId = initialGeneration;

    }

    /**
     * @return list of {@code FieldDescription} of the export metadata columns
     */
    public static List<FieldDescription> getMetadataFields() {
        List<FieldDescription> metaFields = new ArrayList<>(META_COL_NAMES.size());

        Iterator<String> itNames = META_COL_NAMES.iterator();
        Iterator<VoltType> itTypes = META_COL_TYPES.iterator();
        while(itNames.hasNext()) {
            metaFields.add(new FieldDescription(itNames.next(), itTypes.next(), false));
        }
        return metaFields;
    }

    /**
     * Checks if name is a metadata column name
     *
     * @param name  column name
     * @return      {@code true} if name is a metadata column name
     */
    public static boolean isMetadataColumn(String name) {
        return META_COL_NAMES.contains(name);
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

        ImmutableList.Builder<String> colNames = ImmutableList.builder();
        ImmutableList.Builder<VoltType> colTypes = ImmutableList.builder();
        ImmutableList.Builder<Integer> colSizes = ImmutableList.builder();

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

        Column partitionColumn = table.getPartitioncolumn();
        int partitionColumnIndex = partitionColumn == null ? -1 : partitionColumn.getIndex() + INTERNAL_FIELD_COUNT;

        return new ExportRowSchema(table.getTypeName(), colNames.build(), colTypes.build(), colSizes.build(),
                partitionColumnIndex, partitionId, initialGenerationId, generationId);
    }

    /**
     * Create a {@code ExportRowSchema} from a Catalog {@code Topic}.
     * <p>
     * The created schema has no columns.
     *
     * @param topic Catalog {@code Topic}
     * @param partitionId the partition id
     * @param generationId the generation id
     * @return
     */
    public static ExportRowSchema create(Topic topic, int partitionId, long initialGenerationId, long generationId) {
        return new ExportRowSchema(topic.getTypeName(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                -1, partitionId, initialGenerationId, generationId);
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
     * @throws NullPointerException if {@code other} is {@code null}
     */
    public boolean sameSchema(ExportRowSchema other) {

        if (other == this) {
            return true;
        }

        return names.equals(other.names) && types.equals(other.types) && lengths.equals(other.lengths);
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
            if (version != EXPORT_BUFFER_VERSION_1 && version != EXPORT_BUFFER_VERSION_2) {
                throw new IllegalArgumentException("Illegal version, expected between: " + EXPORT_BUFFER_VERSION_1
                        + " and " + EXPORT_BUFFER_VERSION_LATEST + ", got: " + version);
            }
            long initialGenId = buf.getLong();
            long genId = buf.getLong();
            int partitionId = buf.getInt();
            int partitionColumnIndex = version >= EXPORT_BUFFER_VERSION_2 ? buf.getInt() : -1;

            String tableName = ExportRow.decodeString(buf);
            List<String> colNames = new ArrayList<>();
            List<VoltType> colTypes = new ArrayList<>();
            List<Integer> colLengths = new ArrayList<>();
            int columnCount = buf.getInt();
            for (int i = 0; i < columnCount; ++i) {
                colNames.add(ExportRow.decodeString(buf));
                colTypes.add(VoltType.get(buf.get()));
                colLengths.add(buf.getInt());
            }
            return new ExportRowSchema(tableName, colNames, colTypes, colLengths, partitionColumnIndex, partitionId,
                    initialGenId, genId);
        }
        catch(Exception ex) {
            throw new IOException("Failed to deserialize schema " + ex, ex);
        }
    }

    public void serialize(ByteBuffer buf) throws IOException {
        try {
            buf.put(EXPORT_BUFFER_VERSION_2);
            buf.putLong(this.initialGenerationId);
            buf.putLong(this.generation);
            buf.putInt(this.partitionId);
            buf.putInt(this.partitionColIndex);
            SerializationHelper.writeString(tableName, buf);

            Iterator<String> itNames = this.names.iterator();
            Iterator<VoltType> itTypes = this.types.iterator();
            Iterator<Integer> itSizes = this.lengths.iterator();

            buf.putInt(names.size());
            while(itNames.hasNext()) {
                String colName = itNames.next();
                VoltType colType = itTypes.next();
                Integer colSize = itSizes.next();

                SerializationHelper.writeString(colName, buf);
                buf.put(colType.getValue());
                buf.putInt(colSize);
            }
        }
        catch (Exception ex) {
            throw new IOException("Failed to serialize schema " + ex, ex);
        }
    }

    public int getSerializedSize() {
        int size = 1 + // export buffer version
                8 + // initial generation id
                8 + // generation id
                4 + // partition id
                Integer.BYTES + // partition column index
                SerializationHelper.calculateSerializedSize(tableName) +
                4; // column count
        for (String colName : this.names) {
            size += SerializationHelper.calculateSerializedSize(colName) + 1 /* value type */
                    + 4 /* value len */;
        }
        return size;
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
                .append(" - ")
                .append(toSchemaString());
        return sb.toString();
    }
}
