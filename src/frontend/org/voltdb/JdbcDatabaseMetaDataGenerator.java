/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.types.VoltDecimalHelper;

public class JdbcDatabaseMetaDataGenerator
{
    static public final ColumnInfo[] TABLE_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("TABLE_CAT", VoltType.STRING),
                          new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                          new ColumnInfo("TABLE_NAME", VoltType.STRING),
                          new ColumnInfo("TABLE_TYPE", VoltType.STRING),
                          new ColumnInfo("REMARKS", VoltType.STRING),
                          new ColumnInfo("TYPE_CAT", VoltType.STRING),
                          new ColumnInfo("TYPE_SCHEM", VoltType.STRING),
                          new ColumnInfo("TYPE_NAME", VoltType.STRING),
                          new ColumnInfo("SELF_REFERENCING_COL_NAME", VoltType.STRING),
                          new ColumnInfo("REF_GENERATION", VoltType.STRING),
                         };

    static public final ColumnInfo[] COLUMN_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("TABLE_CAT", VoltType.STRING),
                          new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                          new ColumnInfo("TABLE_NAME", VoltType.STRING),
                          new ColumnInfo("COLUMN_NAME", VoltType.STRING),
                          new ColumnInfo("DATA_TYPE", VoltType.INTEGER),
                          new ColumnInfo("TYPE_NAME", VoltType.STRING),
                          new ColumnInfo("COLUMN_SIZE", VoltType.INTEGER),
                          new ColumnInfo("BUFFER_LENGTH", VoltType.INTEGER),
                          new ColumnInfo("DECIMAL_DIGITS", VoltType.INTEGER),
                          new ColumnInfo("NUM_PREC_RADIX", VoltType.INTEGER),
                          new ColumnInfo("NULLABLE", VoltType.INTEGER),
                          new ColumnInfo("REMARKS", VoltType.STRING),
                          new ColumnInfo("COLUMN_DEF", VoltType.STRING),
                          new ColumnInfo("SQL_DATA_TYPE", VoltType.INTEGER),
                          new ColumnInfo("SQL_DATETIME_SUB", VoltType.INTEGER),
                          new ColumnInfo("CHAR_OCTET_LENGTH", VoltType.INTEGER),
                          new ColumnInfo("ORDINAL_POSITION", VoltType.INTEGER),
                          new ColumnInfo("IS_NULLABLE", VoltType.STRING),
                          new ColumnInfo("SCOPE_CATALOG", VoltType.STRING),
                          new ColumnInfo("SCOPE_SCHEMA", VoltType.STRING),
                          new ColumnInfo("SCOPE_TABLE", VoltType.STRING),
                          new ColumnInfo("SOURCE_DATA_TYPE", VoltType.SMALLINT),
                          new ColumnInfo("IS_AUTOINCREMENT", VoltType.STRING)
                         };

    static public final ColumnInfo[] INDEXINFO_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("TABLE_CAT", VoltType.STRING),
                          new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                          new ColumnInfo("TABLE_NAME", VoltType.STRING),
                          new ColumnInfo("NON_UNIQUE", VoltType.TINYINT), // SHould be bool, but...
                          new ColumnInfo("INDEX_QUALIFIER", VoltType.STRING),
                          new ColumnInfo("INDEX_NAME", VoltType.STRING),
                          new ColumnInfo("TYPE", VoltType.SMALLINT),
                          new ColumnInfo("ORDINAL_POSITION", VoltType.SMALLINT),
                          new ColumnInfo("COLUMN_NAME", VoltType.STRING),
                          new ColumnInfo("ASC_OR_DESC", VoltType.STRING),
                          new ColumnInfo("CARDINALITY", VoltType.INTEGER),
                          new ColumnInfo("PAGES", VoltType.INTEGER),
                          new ColumnInfo("FILTER_CONDITION", VoltType.STRING)
    };

    static public final ColumnInfo[] PRIMARYKEYS_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("TABLE_CAT", VoltType.STRING),
                          new ColumnInfo("TABLE_SCHEM", VoltType.STRING),
                          new ColumnInfo("TABLE_NAME", VoltType.STRING),
                          new ColumnInfo("COLUMN_NAME", VoltType.STRING),
                          new ColumnInfo("KEY_SEQ", VoltType.SMALLINT),
                          new ColumnInfo("PK_NAME", VoltType.STRING)
        };

    static public final ColumnInfo[] PROCEDURES_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("PROCEDURE_CAT", VoltType.STRING),
                          new ColumnInfo("PROCEDURE_SCHEM", VoltType.STRING),
                          new ColumnInfo("PROCEDURE_NAME", VoltType.STRING),
                          new ColumnInfo("RESERVED1", VoltType.STRING),
                          new ColumnInfo("RESERVED2", VoltType.STRING),
                          new ColumnInfo("RESERVED3", VoltType.STRING),
                          new ColumnInfo("REMARKS", VoltType.STRING),
                          new ColumnInfo("PROCEDURE_TYPE", VoltType.SMALLINT),
                          new ColumnInfo("SPECIFIC_NAME", VoltType.STRING)
        };

    static public final ColumnInfo[] PROCEDURECOLUMNS_SCHEMA =
        new ColumnInfo[] {
                          new ColumnInfo("PROCEDURE_CAT", VoltType.STRING),
                          new ColumnInfo("PROCEDURE_SCHEM", VoltType.STRING),
                          new ColumnInfo("PROCEDURE_NAME", VoltType.STRING),
                          new ColumnInfo("COLUMN_NAME", VoltType.STRING),
                          new ColumnInfo("COLUMN_TYPE", VoltType.SMALLINT),
                          new ColumnInfo("DATA_TYPE", VoltType.INTEGER),
                          new ColumnInfo("TYPE_NAME", VoltType.STRING),
                          new ColumnInfo("PRECISION", VoltType.INTEGER),
                          new ColumnInfo("LENGTH", VoltType.INTEGER),
                          new ColumnInfo("SCALE", VoltType.SMALLINT),
                          new ColumnInfo("RADIX", VoltType.SMALLINT),
                          new ColumnInfo("NULLABLE", VoltType.SMALLINT),
                          new ColumnInfo("REMARKS", VoltType.STRING),
                          new ColumnInfo("COLUMN_DEF", VoltType.STRING),
                          new ColumnInfo("SQL_DATA_TYPE", VoltType.INTEGER),
                          new ColumnInfo("SQL_DATETIME_SUB", VoltType.INTEGER),
                          new ColumnInfo("CHAR_OCTET_LENGTH", VoltType.INTEGER),
                          new ColumnInfo("ORDINAL_POSITION", VoltType.INTEGER),
                          new ColumnInfo("IS_NULLABLE", VoltType.STRING),
                          new ColumnInfo("SPECIFIC_NAME", VoltType.STRING)
        };

    JdbcDatabaseMetaDataGenerator(Catalog catalog)
    {
        m_catalog = catalog;
        m_database = m_catalog.getClusters().get("cluster").getDatabases().get("database");
    }

    public VoltTable getMetaData(String selector)
    {
        VoltTable result = null;
        if (selector.equalsIgnoreCase("TABLES"))
        {
            result = getTables();
        }
        else if (selector.equalsIgnoreCase("COLUMNS"))
        {
            result = getColumns();
        }
        else if (selector.equalsIgnoreCase("INDEXINFO"))
        {
            result = getIndexInfo();
        }
        else if (selector.equalsIgnoreCase("PRIMARYKEYS"))
        {
            result = getPrimaryKeys();
        }
        else if (selector.equalsIgnoreCase("PROCEDURES"))
        {
            result = getProcedures();
        }
        else if (selector.equalsIgnoreCase("PROCEDURECOLUMNS"))
        {
            result = getProcedureColumns();
        }
        return result;
    }

    private String getTableType(Table table)
    {
        String type = "TABLE";
        if (table.getMaterializer() != null)
        {
            type = "VIEW";
        }
        else if (m_database.getConnectors() != null &&
                 m_database.getConnectors().get("0") != null &&
                 m_database.getConnectors().get("0").getTableinfo() != null &&
                 m_database.getConnectors().get("0").getTableinfo().getIgnoreCase(table.getTypeName()) != null)
        {
            type = "EXPORT";
        }
        return type;
    }

    VoltTable getTables()
    {
        VoltTable results = new VoltTable(TABLE_SCHEMA);
        for (Table table : m_database.getTables())
        {
            // REMARKS and all following columns are always null for us.
            results.addRow(null,
                           null, // no schema name
                           table.getTypeName(),
                           getTableType(table),
                           null, // REMARKS
                           null, // unused TYPE_CAT
                           null, // unused TYPE_SCHEM
                           null, // unused TYPE_NAME
                           null, // unused SELF_REFERENCING_COL_NAME
                           null  // unused REF_GENERATION
                          );
        }
        return results;
    }

    // Might consider consolidating this into VoltType if we go big on JDBC stuff.
    private int getColumnSqlDataType(VoltType type)
    {
        int jdbc_sql_type = java.sql.Types.OTHER;
        switch(type)
        {
        case TINYINT:
            jdbc_sql_type = java.sql.Types.TINYINT;
            break;
        case SMALLINT:
            jdbc_sql_type = java.sql.Types.SMALLINT;
            break;
        case INTEGER:
            jdbc_sql_type = java.sql.Types.INTEGER;
            break;
        case BIGINT:
            jdbc_sql_type = java.sql.Types.BIGINT;
            break;
        case FLOAT:
            jdbc_sql_type = java.sql.Types.DOUBLE;
            break;
        case TIMESTAMP:
            jdbc_sql_type = java.sql.Types.TIMESTAMP;
            break;
        case STRING:
            jdbc_sql_type = java.sql.Types.VARCHAR;
            break;
        case DECIMAL:
            jdbc_sql_type = java.sql.Types.DECIMAL;
            break;
        case VARBINARY:
            jdbc_sql_type = java.sql.Types.VARBINARY;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return jdbc_sql_type;
    }

    private String getColumnSqlTypeName(VoltType type)
    {
        String jdbc_sql_typename = "OTHER";
        switch(type)
        {
        case TINYINT:
            jdbc_sql_typename = "TINYINT";
            break;
        case SMALLINT:
            jdbc_sql_typename = "SMALLINT";
            break;
        case INTEGER:
            jdbc_sql_typename = "INTEGER";
            break;
        case BIGINT:
            jdbc_sql_typename = "BIGINT";
            break;
        case FLOAT:
            jdbc_sql_typename = "DOUBLE";
            break;
        case TIMESTAMP:
            jdbc_sql_typename = "TIMESTAMP";
            break;
        case STRING:
            jdbc_sql_typename = "VARCHAR";
            break;
        case DECIMAL:
            jdbc_sql_typename = "DECIMAL";
            break;
        case VARBINARY:
            jdbc_sql_typename = "VARBINARY";
            break;
        default:
            // XXX What's the right behavior here?
        }
        return jdbc_sql_typename;
    }

    // Integer[0] is the column size and Integer[1] is the radix
    private Integer[] getColumnSizeAndRadix(Column column)
    {
        Integer[] col_size_radix = {null, null};
        VoltType type = VoltType.get((byte) column.getType());
        switch(type)
        {
        //
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
            col_size_radix[0] = (column.getSize() * 8) - 1;
            col_size_radix[1] = 2;
            break;
        case FLOAT:
            col_size_radix[0] = 53;  // magic for double
            col_size_radix[1] = 2;
            break;
        case STRING:
            col_size_radix[0] = column.getSize();
            col_size_radix[1] = null;
            break;
        case DECIMAL:
            col_size_radix[0] = VoltDecimalHelper.kDefaultPrecision;
            col_size_radix[1] = 10;
            break;
        case VARBINARY:
            col_size_radix[0] = column.getSize();
            col_size_radix[1] = null;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return col_size_radix;
    }

    private Integer getColumnDecimalDigits(VoltType type)
    {
        Integer num_dec_digits = null;
        switch(type)
        {
        //
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
        case FLOAT:
        case STRING:
        case VARBINARY:
            num_dec_digits = null;
            break;
        case DECIMAL:
            num_dec_digits = VoltDecimalHelper.kDefaultScale;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return num_dec_digits;
    }

    private int getColumnNullable(Column column)
    {
        int nullable = java.sql.DatabaseMetaData.columnNoNulls;
        if (column.getNullable())
        {
            nullable = java.sql.DatabaseMetaData.columnNullable;
        }
        return nullable;
    }

    private String getColumnRemarks(Column column, Table table)
    {
        String remarks = null;
        if (table.getPartitioncolumn() != null &&
            table.getPartitioncolumn().getTypeName().equals(column.getTypeName()))
        {
            remarks = "PARTITION_COLUMN";
        }
        return remarks;
    }

    private String getDefaultValue(Column column)
    {
        String value = column.getDefaultvalue();
        if (value != null &&
            VoltType.get((byte)column.getDefaulttype()) == VoltType.STRING)
        {
            value = "'" + value + "'";
        }
        return value;
    }

    private Integer getCharOctetLength(Column column)
    {
        Integer length = null;
        VoltType type = VoltType.get((byte)column.getType());
        if (type == VoltType.STRING || type == VoltType.VARBINARY)
        {
            length = column.getSize();
        }

        return length;
    }

    private String getColumnIsNullable(Column column)
    {
        String is_nullable = "NO";
        if (column.getNullable())
        {
            is_nullable = "YES";
        }
        return is_nullable;
    }

    VoltTable getColumns()
    {
        VoltTable results = new VoltTable(COLUMN_SCHEMA);
        for (Table table : m_database.getTables())
        {
            for (Column column: table.getColumns())
            {
                results.addRow(
                               null,
                               null, // no schema name
                               table.getTypeName(),
                               column.getTypeName(),
                               getColumnSqlDataType(VoltType.get((byte) column.getType())),
                               getColumnSqlTypeName(VoltType.get((byte) column.getType())),
                               getColumnSizeAndRadix(column)[0],
                               null,
                               getColumnDecimalDigits(VoltType.get((byte) column.getType())),
                               getColumnSizeAndRadix(column)[1],
                               getColumnNullable(column),
                               getColumnRemarks(column, table), // REMARKS
                               getDefaultValue(column), // default value
                               null, // unused SQL_DATA_TYPE
                               null, // unused SQL_DATETIME_SUB
                               getCharOctetLength(column), // char_octet_length
                               column.getIndex() + 1, // ordinal position, starts from 1
                               getColumnIsNullable(column), // IS_NULLABLE
                               null, // unused SCOPE_CATALOG
                               null, // unused SCOPE_SCHEMA
                               null, // unused SCOPE_TABLE
                               null, // unused SOURCE_DATA_TYPE
                               "NO" // no auto-increment yet
                              );
            }
        }
        return results;
    }

    private short getIndexType(Index index)
    {
        short type = java.sql.DatabaseMetaData.tableIndexOther;
        if (index.getType() == IndexType.HASH_TABLE.getValue())
        {
            type = java.sql.DatabaseMetaData.tableIndexHashed;
        }
        return type;
    }

    private String getSortOrder(Index index)
    {
        String sort_order = null;
        if (index.getType() == IndexType.BALANCED_TREE.getValue())
        {
            sort_order = "A";
        }
        return sort_order;
    }

    VoltTable getIndexInfo()
    {
        VoltTable results = new VoltTable(INDEXINFO_SCHEMA);
        for (Table table : m_database.getTables())
        {
            for (Index index : table.getIndexes())
            {
                for (ColumnRef column : index.getColumns())
                {
                    results.addRow(null,
                                   null, // table_schema
                                   table.getTypeName(), // table name
                                   index.getUnique() ? 0 : 1, // non-unique, 0 is unique, 1 is not
                                   null, // index qualifier (always null for us)
                                   index.getTypeName(), // index name
                                   getIndexType(index), // type
                                   column.getRelativeIndex(), // ordinal position
                                   column.getTypeName(), // column name
                                   getSortOrder(index), // ascending or descending
                                   null, // cardinality
                                   null, // pages (always null for us)
                                   null  // filter condition, also null for us
                              );
                }
            }
        }
        return results;
    }

    VoltTable getPrimaryKeys()
    {
        VoltTable results = new VoltTable(PRIMARYKEYS_SCHEMA);
        for (Table table : m_database.getTables())
        {
            for (Constraint c : table.getConstraints())
            {
                if (c.getType() == ConstraintType.PRIMARY_KEY.getValue())
                {
                    for (ColumnRef column : c.getIndex().getColumns())
                    {
                        results.addRow(null,
                                       null, // table schema
                                       table.getTypeName(), // table name
                                       column.getTypeName(), // column name
                                       column.getRelativeIndex(), // key_seq
                                       c.getTypeName() // PK_NAME
                                      );
                    }
                }
            }
        }
        return results;
    }

    VoltTable getProcedures()
    {
        VoltTable results = new VoltTable(PROCEDURES_SCHEMA);
        for (Procedure proc : m_database.getProcedures())
        {
            results.addRow(
                           null,
                           null, // procedure schema
                           proc.getTypeName(), // procedure name
                           null, // reserved
                           null, // reserved
                           null, // reserved
                           null, // REMARKS
                           java.sql.DatabaseMetaData.procedureResultUnknown, // procedure time
                           proc.getTypeName() // specific name
                          );
        }
        return results;
    }

    private String getProcedureColumnRemarks(ProcParameter param, Procedure proc)
    {
        String remarks = null;
        if (proc.getPartitioncolumn() != null)
        {
            if (proc.getPartitionparameter() == param.getIndex())
            {
                remarks = "PARTITION_PARAMETER";
            }
        }
        if (param.getIsarray())
        {
            if (remarks != null)
            {
                remarks = remarks + ",";
            }
            else
            {
                remarks = "";
            }
            remarks = remarks + "ARRAY_PARAMETER";
        }
        return remarks;
    }

    // Integer[0] is the column size and Integer[1] is the radix
    private Integer[] getParamPrecisionAndRadix(ProcParameter param)
    {
        Integer[] col_size_radix = {null, null};
        VoltType type = VoltType.get((byte) param.getType());
        switch(type)
        {
        //
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
            col_size_radix[0] = (type.getLengthInBytesForFixedTypes() * 8) - 1;
            col_size_radix[1] = 2;
            break;
        case FLOAT:
            col_size_radix[0] = 53;  // magic for double
            col_size_radix[1] = 2;
            break;
        case STRING:
            col_size_radix[0] = VoltType.MAX_VALUE_LENGTH;
            col_size_radix[1] = null;
            break;
        case DECIMAL:
            col_size_radix[0] = VoltDecimalHelper.kDefaultPrecision;
            col_size_radix[1] = 10;
            break;
        case VARBINARY:
            col_size_radix[0] = VoltType.MAX_VALUE_LENGTH;
            col_size_radix[1] = null;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return col_size_radix;
    }

    private int getParamLength(ProcParameter param)
    {
        int length = 0;
        VoltType type = VoltType.get((byte) param.getType());
        switch(type)
        {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
        case FLOAT:
        case DECIMAL:
            length = type.getLengthInBytesForFixedTypes();
            break;
        case STRING:
        case VARBINARY:
            length = VoltType.MAX_VALUE_LENGTH;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return length;
    }

    private Integer getParamCharOctetLength(ProcParameter param)
    {
        Integer length = null;
        VoltType type = VoltType.get((byte) param.getType());
        switch(type)
        {
        case TINYINT:
        case SMALLINT:
        case INTEGER:
        case BIGINT:
        case TIMESTAMP:
        case FLOAT:
        case DECIMAL:
            length = null;
            break;
        case STRING:
        case VARBINARY:
            length = VoltType.MAX_VALUE_LENGTH;
            break;
        default:
            // XXX What's the right behavior here?
        }
        return length;
    }

    VoltTable getProcedureColumns()
    {
        VoltTable results = new VoltTable(PROCEDURECOLUMNS_SCHEMA);
        for (Procedure proc : m_database.getProcedures())
        {
            for (ProcParameter param : proc.getParameters())
            {
                results.addRow(
                               null, // procedure catalog
                               null, // procedure schema
                               proc.getTypeName(), // procedure name
                               param.getTypeName(), // param name
                               java.sql.DatabaseMetaData.procedureColumnIn, // param type, all are IN
                               getColumnSqlDataType(VoltType.get((byte)param.getType())), // data type
                               getColumnSqlTypeName(VoltType.get((byte)param.getType())), // type name
                               getParamPrecisionAndRadix(param)[0], // precision
                               getParamLength(param), // length
                               getColumnDecimalDigits(VoltType.get((byte)param.getType())),
                               getParamPrecisionAndRadix(param)[1], // radix
                               java.sql.DatabaseMetaData.procedureNullableUnknown, // nullable
                               getProcedureColumnRemarks(param, proc), // remarks
                               null, // column default.  always null for us
                               null, // reserved
                               null, // reserved
                               getParamCharOctetLength(param), // char octet length
                               param.getIndex() + 1, // ordinal position
                               "", // is_nullable
                               proc.getTypeName()  // specific name
                );
            }
        }
        return results;
    }

    private Catalog m_catalog;
    private Database m_database;
}
