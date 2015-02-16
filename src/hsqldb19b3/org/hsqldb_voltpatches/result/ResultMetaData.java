/* Copyright (c) 2001-2011, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL HSQL DEVELOPMENT GROUP, HSQLDB.ORG,
 * OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package org.hsqldb_voltpatches.result;

import java.io.IOException;

import org.hsqldb_voltpatches.ColumnBase;
import org.hsqldb_voltpatches.error.Error;
import org.hsqldb_voltpatches.error.ErrorCode;
import org.hsqldb_voltpatches.lib.ArrayUtil;
import org.hsqldb_voltpatches.rowio.RowInputBinary;
import org.hsqldb_voltpatches.rowio.RowOutputInterface;
import org.hsqldb_voltpatches.types.ArrayType;
import org.hsqldb_voltpatches.types.Type;
import org.hsqldb_voltpatches.types.Types;

/**
 * Metadata for a result set.
 *
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.1.1
 * @since 1.8.0
 */
public final class ResultMetaData {

    public static final int RESULT_METADATA          = 1;
    public static final int SIMPLE_RESULT_METADATA   = 2;
    public static final int UPDATE_RESULT_METADATA   = 3;
    public static final int PARAM_METADATA           = 4;
    public static final int GENERATED_INDEX_METADATA = 5;
    public static final int GENERATED_NAME_METADATA  = 6;

    //
    private int type;

    // values overriding table column
    public String[] columnLabels;
    public Type[]   columnTypes;
    private int     columnCount;
    private int     extendedColumnCount;
    public static final ResultMetaData emptyResultMetaData =
        newResultMetaData(0);
    public static final ResultMetaData emptyParamMetaData =
        newParameterMetaData(0);

    // column indexes for mapping or for generated columns
    public int[] colIndexes;

    // columns for data columns
    public ColumnBase[] columns;

    // param mode and nullability for parameter metadata
    public byte[] paramModes;
    public byte[] paramNullable;

    //
    private ResultMetaData(int type) {
        this.type = type;
    }

    public static ResultMetaData newUpdateResultMetaData(Type[] types) {

        ResultMetaData md = new ResultMetaData(UPDATE_RESULT_METADATA);

        md.columnTypes         = new Type[types.length];
        md.columnCount         = types.length;
        md.extendedColumnCount = types.length;

        ArrayUtil.copyArray(types, md.columnTypes, types.length);

        return md;
    }

    public static ResultMetaData newSimpleResultMetaData(Type[] types) {

        ResultMetaData md = new ResultMetaData(SIMPLE_RESULT_METADATA);

        md.columnTypes         = types;
        md.columnCount         = types.length;
        md.extendedColumnCount = types.length;

        return md;
    }

    public static ResultMetaData newResultMetaData(int colCount) {

        Type[] types = new Type[colCount];

        return newResultMetaData(types, null, colCount, colCount);
    }

    public static ResultMetaData newSingleColumnMetaData(String colName) {

        ResultMetaData md = ResultMetaData.newResultMetaData(1);

        md.columns[0] = new ColumnBase(null, null, null, colName);

        md.columns[0].setType(Type.SQL_VARCHAR_DEFAULT);
        md.prepareData();

        return md;
    }

    public static ResultMetaData newResultMetaData(Type[] types,
            int[] baseColumnIndexes, int colCount, int extColCount) {

        ResultMetaData md = new ResultMetaData(RESULT_METADATA);

        md.columnLabels        = new String[colCount];
        md.columns             = new ColumnBase[colCount];
        md.columnTypes         = types;
        md.colIndexes          = baseColumnIndexes;
        md.columnCount         = colCount;
        md.extendedColumnCount = extColCount;

        return md;
    }

    public static ResultMetaData newParameterMetaData(int colCount) {

        ResultMetaData md = new ResultMetaData(PARAM_METADATA);

        md.columnTypes         = new Type[colCount];
        md.columnLabels        = new String[colCount];
        md.paramModes          = new byte[colCount];
        md.paramNullable       = new byte[colCount];
        md.columnCount         = colCount;
        md.extendedColumnCount = colCount;

        return md;
    }

    public static ResultMetaData newGeneratedColumnsMetaData(
            int[] columnIndexes, String[] columnNames) {

        if (columnIndexes != null) {
            ResultMetaData md = new ResultMetaData(GENERATED_INDEX_METADATA);

            md.columnCount         = columnIndexes.length;
            md.extendedColumnCount = columnIndexes.length;
            md.colIndexes          = new int[columnIndexes.length];

            for (int i = 0; i < columnIndexes.length; i++) {
                md.colIndexes[i] = columnIndexes[i] - 1;
            }

            return md;
        } else if (columnNames != null) {
            ResultMetaData md = new ResultMetaData(GENERATED_NAME_METADATA);

            md.columnLabels        = new String[columnNames.length];
            md.columnCount         = columnNames.length;
            md.extendedColumnCount = columnNames.length;
            md.columnLabels        = columnNames;

            return md;
        } else {
            return null;
        }
    }

    public void prepareData() {

        if (columns != null) {
            for (int i = 0; i < columnCount; i++) {
                if (columnTypes[i] == null) {
                    columnTypes[i] = columns[i].getDataType();
                }
            }
        }
    }

    public int getColumnCount() {
        return columnCount;
    }

    public int getExtendedColumnCount() {
        return extendedColumnCount;
    }

    public void resetExtendedColumnCount() {
        extendedColumnCount = columnCount;
    }

    public Type[] getParameterTypes() {
        return columnTypes;
    }

    public String[] getGeneratedColumnNames() {
        return columnLabels;
    }

    public int[] getGeneratedColumnIndexes() {
        return colIndexes;
    }

    public boolean isTableColumn(int i) {

        String colName   = columns[i].getNameString();
        String tableName = columns[i].getTableNameString();

        return tableName != null && tableName.length() > 0 && colName != null
               && colName.length() > 0;
    }

    private static void decodeTableColumnAttrs(int in, ColumnBase column) {

        column.setNullability((byte) (in & 0x00000003));
        column.setIdentity((in & 0x00000004) != 0);
        column.setWriteable((in & 0x00000008) != 0);
        column.setSearchable((in & 0x00000010) != 0);
    }

    private static int encodeTableColumnAttrs(ColumnBase column) {

        int out = column.getNullability();    // always between 0x00 and 0x02

        if (column.isIdentity()) {
            out |= 0x00000004;
        }

        if (column.isWriteable()) {
            out |= 0x00000008;
        }

        if (column.isSearchable()) {
            out |= 0x00000010;
        }

        return out;
    }

    private void decodeParamColumnAttrs(int in, int columnIndex) {
        paramNullable[columnIndex] = (byte) (in & 0x00000003);
        paramModes[columnIndex]    = (byte) ((in >> 4) & 0x0000000f);
    }

    private int encodeParamColumnAttrs(int columnIndex) {

        int out = paramModes[columnIndex] << 4;

        out |= paramNullable[columnIndex];

        return out;
    }

    ResultMetaData(RowInputBinary in) throws IOException {

        type        = in.readInt();
        columnCount = in.readInt();

        switch (type) {

            case UPDATE_RESULT_METADATA :
            case SIMPLE_RESULT_METADATA : {
                columnTypes = new Type[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    columnTypes[i] = readDataTypeSimple(in);
                }

                return;
            }
            case GENERATED_INDEX_METADATA : {
                colIndexes = new int[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    colIndexes[i] = in.readInt();
                }

                return;
            }
            case GENERATED_NAME_METADATA : {
                columnLabels = new String[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    columnLabels[i] = in.readString();
                }

                return;
            }
            case PARAM_METADATA : {
                columnTypes   = new Type[columnCount];
                columnLabels  = new String[columnCount];
                paramModes    = new byte[columnCount];
                paramNullable = new byte[columnCount];

                for (int i = 0; i < columnCount; i++) {
                    columnTypes[i]  = readDataType(in);
                    columnLabels[i] = in.readString();

                    decodeParamColumnAttrs(in.readByte(), i);
                }

                return;
            }
            case RESULT_METADATA : {
                extendedColumnCount = in.readInt();
                columnTypes         = new Type[extendedColumnCount];
                columnLabels        = new String[columnCount];
                columns             = new ColumnBase[columnCount];

                if (columnCount != extendedColumnCount) {
                    colIndexes = new int[columnCount];
                }

                for (int i = 0; i < extendedColumnCount; i++) {
                    Type type = readDataType(in);

                    columnTypes[i] = type;
                }

                for (int i = 0; i < columnCount; i++) {
                    columnLabels[i] = in.readString();

                    String catalog = in.readString();
                    String schema  = in.readString();
                    String table   = in.readString();
                    String name    = in.readString();
                    ColumnBase column = new ColumnBase(catalog, schema, table,
                                                       name);

                    column.setType(columnTypes[i]);
                    decodeTableColumnAttrs(in.readByte(), column);

                    columns[i] = column;
                }

                if (columnCount != extendedColumnCount) {
                    for (int i = 0; i < columnCount; i++) {
                        colIndexes[i] = in.readInt();
                    }
                }

                return;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "ResultMetaData");
            }
        }
    }

    Type readDataTypeSimple(RowInputBinary in) throws IOException {

        int     typeCode = in.readType();
        boolean isArray  = typeCode == Types.SQL_ARRAY;

        if (isArray) {
            typeCode = in.readType();

            return Type.getDefaultArrayType(typeCode);
        }

        return Type.getDefaultType(typeCode);
    }

    Type readDataType(RowInputBinary in) throws IOException {

        int     typeCode = in.readType();
        boolean isArray  = typeCode == Types.SQL_ARRAY;

        if (isArray) {
            typeCode = in.readType();
        }

        long size  = in.readLong();
        int  scale = in.readInt();
        Type type = Type.getType(typeCode, Type.SQL_VARCHAR.getCharacterSet(),
                                 Type.SQL_VARCHAR.getCollation(), size, scale);

        if (isArray) {
            type = new ArrayType(type, ArrayType.defaultArrayCardinality);
        }

        return type;
    }

    void writeDataType(RowOutputInterface out, Type type) {

        out.writeType(type.typeCode);

        if (type.isArrayType()) {
            out.writeType(type.collectionBaseType().typeCode);
        }

        out.writeLong(type.precision);
        out.writeInt(type.scale);
    }

    void writeDataTypeCodes(RowOutputInterface out, Type type) {

        out.writeType(type.typeCode);

        if (type.isArrayType()) {
            out.writeType(type.collectionBaseType().typeCode);
        }
    }

    void write(RowOutputInterface out) throws IOException {

        out.writeInt(type);
        out.writeInt(columnCount);

        switch (type) {

            case UPDATE_RESULT_METADATA :
            case SIMPLE_RESULT_METADATA : {
                for (int i = 0; i < columnCount; i++) {
                    writeDataTypeCodes(out, columnTypes[i]);
                }

                return;
            }
            case GENERATED_INDEX_METADATA : {
                for (int i = 0; i < columnCount; i++) {
                    out.writeInt(colIndexes[i]);
                }

                return;
            }
            case GENERATED_NAME_METADATA : {
                for (int i = 0; i < columnCount; i++) {
                    out.writeString(columnLabels[i]);
                }

                return;
            }
            case PARAM_METADATA :
                for (int i = 0; i < columnCount; i++) {
                    writeDataType(out, columnTypes[i]);
                    out.writeString(columnLabels[i]);
                    out.writeByte(encodeParamColumnAttrs(i));
                }

                return;

            case RESULT_METADATA : {
                out.writeInt(extendedColumnCount);

                for (int i = 0; i < extendedColumnCount; i++) {
                    if (columnTypes[i] == null) {
                        ColumnBase column = columns[i];

                        columnTypes[i] = column.getDataType();
                    }

                    writeDataType(out, columnTypes[i]);
                }

                for (int i = 0; i < columnCount; i++) {
                    ColumnBase column = columns[i];

                    out.writeString(columnLabels[i]);
                    out.writeString(column.getCatalogNameString());
                    out.writeString(column.getSchemaNameString());
                    out.writeString(column.getTableNameString());
                    out.writeString(column.getNameString());
                    out.writeByte(encodeTableColumnAttrs(column));
                }

                if (columnCount != extendedColumnCount) {
                    for (int i = 0; i < colIndexes.length; i++) {
                        out.writeInt(colIndexes[i]);
                    }
                }

                return;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "ResultMetaData");
            }
        }
    }

    public ResultMetaData getNewMetaData(int[] columnMap) {

        ResultMetaData newMeta = newResultMetaData(columnMap.length);

        ArrayUtil.projectRow(columnLabels, columnMap, newMeta.columnLabels);
        ArrayUtil.projectRow(columnTypes, columnMap, newMeta.columnTypes);
        ArrayUtil.projectRow(columns, columnMap, newMeta.columns);

        return newMeta;
    }

    public boolean areTypesCompatible(ResultMetaData newMeta) {

        if (columnCount != newMeta.columnCount) {
            return false;
        }

        for (int i = 0; i < columnCount; i++) {
            if (!columnTypes[i].canConvertFrom(newMeta.columnTypes[i])) {
                return false;
            }
        }

        return true;
    }
}
