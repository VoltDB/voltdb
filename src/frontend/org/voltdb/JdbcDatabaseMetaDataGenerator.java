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

package org.voltdb;

import java.util.SortedSet;
import java.util.TreeSet;

import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Task;
import org.voltdb.catalog.TaskParameter;
import org.voltdb.catalog.Topic;
import org.voltdb.catalog.User;
import org.voltdb.task.TaskScope;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.InMemoryJarfile;

public class JdbcDatabaseMetaDataGenerator
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String JSON_PARTITION_PARAMETER = "partitionParameter";
    public static final String JSON_PARTITION_PARAMETER_TYPE = "partitionParameterType";
    public static final String JSON_SINGLE_PARTITION = "singlePartition";
    public static final String JSON_READ_ONLY = "readOnly";
    public static final String JSON_COMPOUND = "compound";
    public static final String JSON_PARTITION_COLUMN = "partitionColumn";
    public static final String JSON_SOURCE_TABLE = "sourceTable";
    public static final String JSON_LIMIT_PARTITION_ROWS_DELETE_STMT = "limitPartitionRowsDeleteStmt";
    public static final String JSON_DRED_TABLE = "drEnabled";
    public static final String JSON_ERROR = "error";

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
                          new ColumnInfo("REF_GENERATION", VoltType.STRING)
                         };

    static public final ColumnInfo[] USER_SCHEMA = new ColumnInfo[] {
        new ColumnInfo("USER", VoltType.STRING),
        new ColumnInfo("ROLES", VoltType.STRING)
    };

    static public final ColumnInfo[] ROLE_SCHEMA = new ColumnInfo[] {
        new ColumnInfo("ROLE", VoltType.STRING),
        new ColumnInfo("PERMISSIONS", VoltType.STRING)
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

    static public final ColumnInfo[] FUNCTIONS_SCHEMA =
            new ColumnInfo[] {
                              new ColumnInfo("FUNCTION_TYPE", VoltType.STRING),
                              new ColumnInfo("FUNCTION_NAME", VoltType.STRING),
                              new ColumnInfo("CLASS_NAME", VoltType.STRING),
                              new ColumnInfo("METHOD_NAME", VoltType.STRING)
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

    static public final ColumnInfo[] TYPEINFO_SCHEMA =
        new ColumnInfo[] {
            new ColumnInfo("TYPE_NAME", VoltType.STRING),
            new ColumnInfo("DATA_TYPE", VoltType.INTEGER),
            new ColumnInfo("PRECISION", VoltType.INTEGER),
            new ColumnInfo("LITERAL_PREFIX", VoltType.STRING),
            new ColumnInfo("LITERAL_SUFFIX", VoltType.STRING),
            new ColumnInfo("CREATE_PARAMS", VoltType.STRING),
            new ColumnInfo("NULLABLE", VoltType.SMALLINT),
            new ColumnInfo("CASE_SENSITIVE", VoltType.TINYINT), //should be bool...
            new ColumnInfo("SEARCHABLE", VoltType.SMALLINT),
            new ColumnInfo("UNSIGNED_ATTRIBUTE", VoltType.TINYINT), //should be bool
            new ColumnInfo("FIXED_PREC_SCALE", VoltType.TINYINT), //should be bool
            new ColumnInfo("AUTO_INCREMENT", VoltType.TINYINT), //should be bool
            new ColumnInfo("LOCAL_TYPE_NAME", VoltType.STRING),
            new ColumnInfo("MINIMUM_SCALE", VoltType.SMALLINT),
            new ColumnInfo("MAXIMUM_SCALE", VoltType.SMALLINT),
            new ColumnInfo("SQL_DATA_TYPE", VoltType.INTEGER),
            new ColumnInfo("SQL_DATETIME_SUB", VoltType.INTEGER),
            new ColumnInfo("NUM_PREC_RADIX", VoltType.INTEGER)
        };

    static public final ColumnInfo[] CLASS_SCHEMA =
        new ColumnInfo[] {
            new ColumnInfo("CLASS_NAME", VoltType.STRING),
            new ColumnInfo("VOLT_PROCEDURE", VoltType.TINYINT),
            new ColumnInfo("ACTIVE_PROC", VoltType.TINYINT)
        };

    static public final ColumnInfo[] CONFIG_SCHEMA =
        new ColumnInfo[] {
            new ColumnInfo("CONFIG_NAME", VoltType.STRING),
            new ColumnInfo("CONFIG_VALUE", VoltType.STRING),
            new ColumnInfo("CONFIG_DESCRIPTION", VoltType.STRING)
        };

    static public final ColumnInfo[] TASKS_SCHEMA = new ColumnInfo[] {
            new ColumnInfo("TASK_NAME", VoltType.STRING),
            new ColumnInfo("SCHEDULER_CLASS", VoltType.STRING),
            new ColumnInfo("SCHEDULER_PARAMETERS", VoltType.STRING),
            new ColumnInfo("ACTIONS_CLASS", VoltType.STRING),
            new ColumnInfo("ACTIONS_PARAMETERS", VoltType.STRING),
            new ColumnInfo("SCHEDULE_CLASS", VoltType.STRING),
            new ColumnInfo("SCHEDULE_PARAMETERS", VoltType.STRING),
            new ColumnInfo("ON_ERROR", VoltType.STRING),
            new ColumnInfo("RUN_LOCATION", VoltType.STRING),
            new ColumnInfo("USER", VoltType.STRING),
            new ColumnInfo("ENABLED", VoltType.STRING)
    };

    static public final ColumnInfo[] TOPICS_SCHEMA = new ColumnInfo[] {
            new ColumnInfo("TOPIC_NAME", VoltType.STRING),
            new ColumnInfo("IS_SINGLE", VoltType.STRING),
            new ColumnInfo("IS_OPAQUE", VoltType.STRING),
            new ColumnInfo("STREAM_NAME", VoltType.STRING),
            new ColumnInfo("PROCEDURE_NAME", VoltType.STRING)
    };

    JdbcDatabaseMetaDataGenerator(Catalog catalog, DefaultProcedureManager defaultProcs, InMemoryJarfile jarfile)
    {
        m_catalog = catalog;
        m_defaultProcs = defaultProcs;
        m_database = m_catalog.getClusters().get("cluster").getDatabases().get("database");
        m_jarfile = jarfile;
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
        else if (selector.equalsIgnoreCase("ROLES"))
        {
            result = getRoles();
        }
        else if (selector.equalsIgnoreCase("USERS"))
        {
            result = getUsers();
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
        else if (selector.equalsIgnoreCase("FUNCTIONS"))
        {
            result = getFunctions();
        }
        else if (selector.equalsIgnoreCase("PROCEDURECOLUMNS"))
        {
            result = getProcedureColumns();
        }
        else if (selector.equalsIgnoreCase("TYPEINFO"))
        {
            result = getTypeInfo();
        }
        // This selector is not part of the JDBC standard, but we pile on here
        // because it's a convenient way to get information about the application
        else if (selector.equalsIgnoreCase("CLASSES"))
        {
            result = getClasses();
        } else if (selector.equalsIgnoreCase("TASKS")) {
            result = getTasks();
        } else if (selector.equalsIgnoreCase("TOPICS")) {
            result = getTopics();
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
        else if (TableType.isStream(table.getTabletype()))
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
            String type = getTableType(table);
            Column partColumn;
            if (type.equals("VIEW")) {
                partColumn = table.getMaterializer().getPartitioncolumn();
            }
            else {
                partColumn = table.getPartitioncolumn();
            }

            String remark = null;
            try {
                JSONObject jsObj = new JSONObject();

                if (partColumn != null) {
                    jsObj.put(JSON_PARTITION_COLUMN, partColumn.getName());
                    if (type.equals("VIEW")) {
                        jsObj.put(JSON_SOURCE_TABLE, table.getMaterializer().getTypeName());
                    }
                }

                if (table.getIsdred()) {
                    jsObj.put(JSON_DRED_TABLE, "true");
                }

                remark = jsObj.length() > 0 ? jsObj.toString() : null;

            } catch (JSONException e) {
                hostLog.warn("You have encountered an unexpected error while generating results for the " +
                        "@SystemCatalog procedure call. This error will not affect your database's " +
                        "operation. Please contact VoltDB support with your log files and a " +
                        "description of what you were doing when this error occured.", e);
                remark = "{\"" + JSON_ERROR + "\":\"" + e.getMessage() + "\"}";
            }

            results.addRow(null,
                           null, // no schema name
                           table.getTypeName(),
                           type,
                           remark, // REMARKS
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
    // Considered and done
    private int getColumnSqlDataType(VoltType type)
    {
        return type.getJdbcSqlType();
    }

    private String getColumnSqlTypeName(VoltType type)
    {
        String jdbc_sql_typename = type.toSQLString();
        if (jdbc_sql_typename == null) {
            jdbc_sql_typename = "OTHER";
        }
        return jdbc_sql_typename.toUpperCase();
    }

    // Integer[0] is the column size and Integer[1] is the radix
    private Integer[] getColumnSizeAndRadix(Column column)
    {
        Integer[] col_size_radix = {null, null};
        // This looks similar to VoltType.getTypePrecisionAndRadix.  However,
        // the String/VARBINARY values depend on the schema, not an intrinsic property
        // of the type, so it stays here for now.
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
        // Would be nice to push this into VoltType someday
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

    VoltTable getUsers() {
        VoltTable results = new VoltTable(USER_SCHEMA);
        for (User user : m_database.getUsers()) {
            StringBuilder roles = new StringBuilder();
            String sep = "";
            for (GroupRef role : user.getGroups()) {
                roles.append(sep).append(role.getTypeName());
                sep = ",";
            }
            results.addRow(user.getTypeName(),
                           roles.toString());
        }
        return results;
    }

    VoltTable getRoles() {
        VoltTable results = new VoltTable(ROLE_SCHEMA);
        for (Group group : m_database.getGroups()) {
            StringBuilder permissions = new StringBuilder();
            String sep = "";
            for ( String field : group.getFields()) {
                if((Boolean)group.getField(field)) {
                    permissions.append(sep).append(field);
                    sep = ",";
                }
            }
            results.addRow(group.getTypeName(),
                           permissions.toString());
        }
        return results;
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
                        String columnName;

                        // if the index name is "MATVIEW_PK_CONSTRAINT", the column name for the materialized view is obtained
                        // from its column information - ENG-6927
                        if (c.getTypeName().equals(HSQLInterface.AUTO_GEN_MATVIEW_CONST) ) {
                            columnName = column.getColumn().getTypeName();
                        } else {
                            columnName = column.getTypeName();
                        }
                        results.addRow(null,
                                       null, // table schema
                                       table.getTypeName(), // table name
                                       columnName, // column name
                                       column.getRelativeIndex(), // key_seq
                                       c.getTypeName() // PK_NAME
                                      );
                    }
                }
            }
        }
        return results;
    }

    VoltTable getFunctions()
    {
        VoltTable results = new VoltTable(FUNCTIONS_SCHEMA);

        for (Function func : m_database.getFunctions()) {
            String functionType = func.getMethodname() == null ? "aggregate" : "scalar";
            results.addRow(
                           functionType,           // Function Type
                           func.getFunctionname(),  // Function Name
                           func.getClassname(),     // Class Name
                           func.getMethodname());   // Method Name
        }

        return results;
    }

    VoltTable getProcedures()
    {
        VoltTable results = new VoltTable(PROCEDURES_SCHEMA);

        // merge catalog and default procedures
        SortedSet<Procedure> procedures = new TreeSet<>();
        for (Procedure proc : m_database.getProcedures()) {
            procedures.add(proc);
        }
        if (m_defaultProcs != null) {
            for (Procedure proc : m_defaultProcs.m_defaultProcMap.values()) {
                procedures.add(proc);
            }
        }

        for (Procedure proc : procedures)
        {
            String remark = null;
            try {
                JSONObject jsObj = new JSONObject();
                jsObj.put(JSON_READ_ONLY, proc.getReadonly());
                jsObj.put(JSON_SINGLE_PARTITION, proc.getSinglepartition());
                if (proc.getSinglepartition()) {
                    jsObj.put(JSON_PARTITION_PARAMETER, proc.getPartitionparameter());
                    if (proc.getPartitionparameter() == -1) {
                        jsObj.put(JSON_PARTITION_PARAMETER_TYPE, -1);
                    } else {
                        jsObj.put(JSON_PARTITION_PARAMETER_TYPE, proc.getPartitioncolumn().getType());
                    }
                }
                jsObj.put(JSON_COMPOUND, CatalogUtil.isCompoundProcedure(proc));
                remark = jsObj.toString();
            } catch (JSONException e) {
                hostLog.warn("You have encountered an unexpected error while generating results for the " +
                             "@SystemCatalog procedure call. This error will not affect your database's " +
                             "operation. Please contact VoltDB support with your log files and a " +
                             "description of what you were doing when this error occured.", e);
                remark = "{\"" + JSON_ERROR + "\",\"" + e.getMessage() + "\"}";
            }
            results.addRow(
                           null,
                           null, // procedure schema
                           proc.getTypeName(), // procedure name
                           null, // reserved
                           null, // reserved
                           null, // reserved
                           remark, // REMARKS
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
        VoltType type = VoltType.get((byte) param.getType());
        return type.getTypePrecisionAndRadix();
    }

    private int getParamLength(ProcParameter param)
    {
        VoltType type = VoltType.get((byte) param.getType());
        return type.getMaxLengthInBytes();
    }

    private Integer getParamCharOctetLength(ProcParameter param)
    {
        Integer length = null;
        // Would be nice to push this type-dependent stuff to VoltType someday.
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

        // merge catalog and default procedures
        SortedSet<Procedure> procedures = new TreeSet<>();
        for (Procedure proc : m_database.getProcedures()) {
            procedures.add(proc);
        }
        if (m_defaultProcs != null) {
            for (Procedure proc : m_defaultProcs.m_defaultProcMap.values()) {
                procedures.add(proc);
            }
        }

        for (Procedure proc : procedures) {
            for (ProcParameter param : proc.getParameters()) {
                Integer paramPrecisionAndRadix[] = getParamPrecisionAndRadix(param);
                results.addRow(
                               null, // procedure catalog
                               null, // procedure schema
                               proc.getTypeName(), // procedure name
                               param.getTypeName(), // param name
                               java.sql.DatabaseMetaData.procedureColumnIn, // param type, all are IN
                               getColumnSqlDataType(VoltType.get((byte)param.getType())), // data type
                               getColumnSqlTypeName(VoltType.get((byte)param.getType())), // type name
                               paramPrecisionAndRadix[0], // precision
                               getParamLength(param), // length
                               getColumnDecimalDigits(VoltType.get((byte)param.getType())),
                               paramPrecisionAndRadix[1], // radix
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

    VoltTable getTypeInfo()
    {
        VoltTable results = new VoltTable(TYPEINFO_SCHEMA);
        for (VoltType type : VoltType.values()) {
            if (!type.isJdbcVisible()) {
                continue;
            }
            Byte unsigned = null;
            if (type.isUnsigned() != null) {
                unsigned = (byte)(type.isUnsigned() ? 1 : 0);
            }
            Integer typePrecisionAndRadix[] = type.getTypePrecisionAndRadix();
            results.addRow(type.toSQLString().toUpperCase(),
                    type.getJdbcSqlType(),
                    typePrecisionAndRadix[0],
                    type.getLiteralPrefix(),
                    type.getLiteralSuffix(),
                    type.getCreateParams(),
                    type.getNullable(),
                    type.isCaseSensitive() ? 1 : 0,
                    type.getSearchable(),
                    unsigned,
                    0,  // no money types (according to definition) in Volt?
                    0,  // no auto-increment
                    type.toSQLString().toUpperCase(),
                    type.getMinimumScale(),
                    type.getMaximumScale(),
                    null,
                    null,
                    typePrecisionAndRadix[1]
                    );
        }
        return results;
    }

    VoltTable getClasses()
    {
        VoltTable results = new VoltTable(CLASS_SCHEMA);
        for (String classname : m_jarfile.getLoader().getClassNames()) {
            try {
                Class<?> clazz = m_jarfile.getLoader().loadClass(classname);
                boolean isProc = VoltProcedure.class.isAssignableFrom(clazz)
                        || VoltCompoundProcedure.class.isAssignableFrom(clazz);
                boolean isActive = false;
                if (isProc) {
                    for (Procedure proc : m_database.getProcedures()) {
                        if (proc.getClassname().equals(clazz.getCanonicalName())) {
                            isActive = true;
                            break;
                        }
                    }
                }
                results.addRow(classname, isProc ? 1 : 0, isActive ? 1 : 0);
            }
            catch (Exception e) {
                // if we can't load a class from the jarfile, just pretend it doesn't
                // exist.  Other checks when we actually load the classes should
                // ensure that we don't end up in this state.
            }
        }
        return results;
    }

    VoltTable getTasks() {
        VoltTable results = new VoltTable(TASKS_SCHEMA);
        for (Task task : m_database.getTasks()) {
            results.addRow(task.getName(), task.getSchedulerclass(), getParamsString(task.getSchedulerparameters()),
                    task.getActiongeneratorclass(), getParamsString(task.getActiongeneratorparameters()),
                    task.getScheduleclass(), getParamsString(task.getScheduleparameters()), task.getOnerror(),
                    TaskScope.translateIdToName(task.getScope()), task.getUser(), Boolean.toString(task.getEnabled()));

        }
        return results;
    }

    VoltTable getTopics() {
        VoltTable results = new VoltTable(TOPICS_SCHEMA);
        for (Topic topic : m_database.getTopics()) {
            results.addRow(topic.getTypeName(), Boolean.toString(topic.getIssingle()), Boolean.toString(topic.getIsopaque()),
                    topic.getStreamname(), topic.getProcedurename());
        }
        return results;
    }

    private String getParamsString(CatalogMap<TaskParameter> params) {
        String paramsArray[] = new String[params.size()];
        for (TaskParameter param : params) {
            paramsArray[param.getIndex()] = param.getParameter();
        }
        StringBuilder sb = new StringBuilder("[");
        String prefix = "'";
        for (String param : paramsArray) {
            sb.append(prefix).append(param).append('\'');
            prefix = ", '";
        }
        return sb.append(']').toString();
    }

    private final Catalog m_catalog;
    private final DefaultProcedureManager m_defaultProcs;
    private final Database m_database;
    private final InMemoryJarfile m_jarfile;
}
