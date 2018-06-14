/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.common.Constants;
import org.voltdb.common.Permission;
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ConstraintType;

/**
 *
 */
public abstract class CatalogSchemaTools {

    private final static String spacer = "   ";

    // Set to true to enable dumping to /tmp/canonical-<timestamp>.sql
    // Make sure it's false before committing.
    private final static boolean dumpSchema = false;

    // To support inline batch statements and associated comments.
    private static final String batchSpecificComments =
            "-- This file uses the --inlinebatch feature. Batching processes all of the DDL in a single step\n" +
                    "-- dramatically reducing the time required to apply the schema compared to processing each\n" +
                    "-- command separately.\n" +
                    "--\n";

    private static final String startBatch = "file -inlinebatch END_OF_BATCH\n";
    private static final String endBatch = "END_OF_BATCH\n";

    /**
     * Convert a Table catalog object into the proper SQL DDL, including all indexes,
     * constraints, and foreign key references.
     * Also returns just the CREATE TABLE statement, since, like all good methods,
     * it should have two purposes....
     * It would be nice to have a separate method to just generate the CREATE TABLE,
     * but we use that pass to also figure out what separate constraint and index
     * SQL DDL needs to be generated, so instead, we opt to build the CREATE TABLE DDL
     * separately as we go here, and then fill it in to the StringBuilder being used
     * to construct the full canonical DDL at the appropriate time.
     * @param sb - the schema being built
     * @param catalog_tbl - object to be analyzed
     * @param viewQuery - the Query if this Table is a View
     * @param isExportOnly Is this a export table.
     * @param streamPartitionColumn stream partition column
     * @param streamTarget - true if this Table is an Export Table
     * @return SQL Schema text representing the CREATE TABLE statement to generate the table
     */
    public static String toSchema(StringBuilder sb, Table catalog_tbl, String viewQuery, boolean isExportOnly, String streamPartitionColumn, String streamTarget) {
        assert(!catalog_tbl.getColumns().isEmpty());
        boolean tableIsView = (viewQuery != null);

        // We need the intermediate results of building the table schema string so that
        // we can return the full CREATE TABLE statement, so accumulate it separately
        final StringBuilder table_sb = new StringBuilder();

        final Set<Index> skip_indexes = new HashSet<>();
        final Set<Constraint> skip_constraints = new HashSet<>();

        if (tableIsView) {
            table_sb.append("CREATE VIEW ").append(catalog_tbl.getTypeName()).append(" (");
        }
        else {
            if (isExportOnly) {
                table_sb.append("CREATE STREAM ").append(catalog_tbl.getTypeName());
                if (streamPartitionColumn != null && viewQuery == null) {
                    table_sb.append(" PARTITION ON COLUMN ").append(streamPartitionColumn);
                }
                //Default target means no target.
                if (streamTarget != null && !streamTarget.equalsIgnoreCase(Constants.DEFAULT_EXPORT_CONNECTOR_NAME)) {
                    table_sb.append(" EXPORT TO TARGET ").append(streamTarget);
                }
            } else {
                table_sb.append("CREATE TABLE ").append(catalog_tbl.getTypeName());
            }
            table_sb.append(" (");
        }

        // Columns
        String add = "\n";
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType col_type = VoltType.get((byte)catalog_col.getType());

            // this next assert would be great if we dealt with default values well
            //assert(! ((catalog_col.getDefaultvalue() == null) && (catalog_col.getNullable() == false) ) );

            if (tableIsView) {
                table_sb.append(add).append(spacer).append(catalog_col.getTypeName());
                add = ",\n";
                continue;
            }

            table_sb.append(add).append(spacer).append(catalog_col.getTypeName()).append(" ").append(col_type.toSQLString()).append(col_type.isVariableLength() &&
                    catalog_col.getSize() > 0 ? "(" + catalog_col.getSize() +
                            (catalog_col.getInbytes() ? " BYTES" : "") + ")" : "");

            // Default value
            String defaultvalue = catalog_col.getDefaultvalue();
            //VoltType defaulttype = VoltType.get((byte)catalog_col.getDefaulttype());
            boolean nullable = catalog_col.getNullable();
            // TODO: Shouldn't have to check whether the string contains "null"
            if (defaultvalue == null) {
            }
            else if (defaultvalue.toLowerCase().equals("null") && nullable) {
                defaultvalue = null;
            }
            else {
                if (col_type == VoltType.TIMESTAMP) {
                    if (defaultvalue.startsWith("CURRENT_TIMESTAMP")) {
                        defaultvalue = "CURRENT_TIMESTAMP";
                    }
                    else {
                        assert(defaultvalue.matches("[0-9]+"));
                        long epoch = Long.parseLong(defaultvalue);
                        Date d = new Date(epoch / 1000);
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        defaultvalue = "\'" + sdf.format(d) + "." +
                                StringUtils.leftPad(String.valueOf(epoch % 1000000), 6, "0") + "\'";
                    }
                }
                else {
                    // XXX: if (defaulttype != VoltType.VOLTFUNCTION) {
                    // TODO: Escape strings properly
                    defaultvalue = defaultvalue.replace("\'", "\'\'");
                    defaultvalue = "'" + defaultvalue + "'";
                }
            }
            if (defaultvalue == null) {
                table_sb.append((!nullable ? " NOT NULL" : "") );
            }
            else {
                table_sb.append(" DEFAULT ").append(defaultvalue != null ? defaultvalue : "NULL").append(!nullable ? " NOT NULL" : "");
            }

            // Single-column constraints
            for (ConstraintRef catalog_const_ref : catalog_col.getConstraints()) {
                Constraint catalog_const = catalog_const_ref.getConstraint();
                ConstraintType const_type = ConstraintType.get(catalog_const.getType());

                // Check if there is another column in our table with the same constraint
                // If there is, then we need to add it to the end of the table definition
                boolean found = false;
                for (Column catalog_other_col : catalog_tbl.getColumns()) {
                    if (catalog_other_col.equals(catalog_col)) continue;
                    if (catalog_other_col.getConstraints().getIgnoreCase(catalog_const.getTypeName()) != null) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    switch (const_type) {
                        case FOREIGN_KEY: {
                            Table catalog_fkey_tbl = catalog_const.getForeignkeytable();
                            Column catalog_fkey_col = null;
                            for (ColumnRef ref : catalog_const.getForeignkeycols()) {
                                catalog_fkey_col = ref.getColumn();
                                break; // Nasty hack to get first item
                            }

                            assert(catalog_fkey_col != null);
                            table_sb.append(" REFERENCES ").append(catalog_fkey_tbl.getTypeName()).append(" (").append(catalog_fkey_col.getTypeName()).append(")");
                            skip_constraints.add(catalog_const);
                            break;
                        }
                        default:
                            // Nothing for now
                    }
                }
            }

            add = ",\n";
        }

        // Constraints
        for (Constraint catalog_const : catalog_tbl.getConstraints()) {
            if (skip_constraints.contains(catalog_const)) continue;
            ConstraintType const_type = ConstraintType.get(catalog_const.getType());

            // Primary Keys / Unique Constraints
            if (const_type == ConstraintType.PRIMARY_KEY || const_type == ConstraintType.UNIQUE) {
                Index catalog_idx = catalog_const.getIndex();
                if (!tableIsView) {
                    // Get the ConstraintType.

                    table_sb.append(add).append(spacer);
                    if (!catalog_const.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX)) {
                        table_sb.append("CONSTRAINT ").append(catalog_const.getTypeName()).append(" ");
                    }
                    if (const_type == ConstraintType.PRIMARY_KEY || const_type == ConstraintType.UNIQUE) {
                        if (const_type == ConstraintType.PRIMARY_KEY) {
                            table_sb.append("PRIMARY KEY (");
                        }
                        else {
                            if (catalog_idx.getAssumeunique()) {
                                table_sb.append("ASSUMEUNIQUE (");
                            }
                            else {
                                table_sb.append("UNIQUE (");
                            }
                        }
                        String col_add = "";
                        if (catalog_idx.getExpressionsjson() != null && !catalog_idx.getExpressionsjson().equals("")) {
                            String exprStrings = new String();
                            StmtTargetTableScan tableScan = new StmtTargetTableScan(catalog_tbl);
                            try {
                                List<AbstractExpression> expressions = AbstractExpression.fromJSONArrayString(catalog_idx.getExpressionsjson(), tableScan);
                                String sep = "";
                                for (AbstractExpression expr : expressions) {
                                    exprStrings += sep + expr.explain(catalog_tbl.getTypeName());
                                    sep = ",";
                                }
                            }
                            catch (JSONException e) {
                            }
                            table_sb.append(col_add).append(exprStrings);
                        }
                        else {
                            for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                                table_sb.append(col_add).append(catalog_colref.getColumn().getTypeName());
                                col_add = ", ";
                            } // FOR
                        }
                        table_sb.append(")");
                    }
                }
                if (catalog_idx.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX) ||
                        catalog_idx.getTypeName().startsWith(HSQLInterface.AUTO_GEN_MATVIEW) ) {
                    skip_indexes.add(catalog_idx);
                }

            // Foreign Key
            } else if (const_type == ConstraintType.FOREIGN_KEY) {
                Table catalog_fkey_tbl = catalog_const.getForeignkeytable();
                String col_add = "";
                String our_columns = "";
                String fkey_columns = "";
                for (ColumnRef catalog_colref : catalog_const.getForeignkeycols()) {
                    // The name of the ColumnRef is the column in our base table
                    Column our_column = catalog_tbl.getColumns().getIgnoreCase(catalog_colref.getTypeName());
                    assert(our_column != null);
                    our_columns += col_add + our_column.getTypeName();

                    Column fkey_column = catalog_colref.getColumn();
                    assert(fkey_column != null);
                    fkey_columns += col_add + fkey_column.getTypeName();

                    col_add = ", ";
                }
                table_sb.append(add).append(spacer + "CONSTRAINT ").append(catalog_const.getTypeName()).append(" FOREIGN KEY (")
                        .append(our_columns).append(") REFERENCES ").append(catalog_fkey_tbl.getTypeName())
                        .append(" (").append(fkey_columns).append(")");
            }
            skip_constraints.add(catalog_const);
        }

        if (catalog_tbl.getTuplelimit() != Integer.MAX_VALUE) {
            table_sb.append(add).append(spacer + "LIMIT PARTITION ROWS ").append(String.valueOf(catalog_tbl.getTuplelimit()));
            String deleteStmt = CatalogUtil.getLimitPartitionRowsDeleteStmt(catalog_tbl);
            if (deleteStmt != null) {
                if (deleteStmt.endsWith(";")) {
                    // StatementCompiler appends the semicolon, we don't want it here.
                    deleteStmt = deleteStmt.substring(0, deleteStmt.length() - 1);
                }
                table_sb.append("\n" + spacer + spacer + "EXECUTE (").append(deleteStmt).append(")");
            }
        }

        if (viewQuery != null) {
            table_sb.append("\n) AS \n");
            table_sb.append(spacer).append(viewQuery).append(";\n");
        }
        else {
            table_sb.append("\n)");
            TimeToLive ttl = catalog_tbl.getTimetolive().get(TimeToLiveVoltDB.TTL_NAME);
            if (ttl != null) {
                table_sb.append(" USING TTL " + ttl.getTtlvalue() + " ");
                if (ttl.getTtlunit() != null) {
                    table_sb.append(ttl.getTtlunit());
                }
                table_sb.append(" ON COLUMN " + ttl.getTtlcolumn().getTypeName());
            }
            table_sb.append(";\n");
        }

        // We've built the full CREATE TABLE statement for this table,
        // Append the generated table schema to the canonical DDL StringBuilder
        sb.append(table_sb.toString());

        // Partition Table for regular tables (non-streams)
        if (catalog_tbl.getPartitioncolumn() != null && viewQuery == null && !isExportOnly) {
            sb.append("PARTITION TABLE ").append(catalog_tbl.getTypeName()).append(" ON COLUMN ").append(catalog_tbl.getPartitioncolumn().getTypeName()).append(";\n");
        }

        // All other Indexes
        for (Index catalog_idx : catalog_tbl.getIndexes()) {
            if (skip_indexes.contains(catalog_idx)) continue;

            if (catalog_idx.getUnique()) {
                if (catalog_idx.getAssumeunique()) {
                    sb.append("CREATE ASSUMEUNIQUE INDEX ");
                }
                else {
                    sb.append("CREATE UNIQUE INDEX ");
                }
            }
            else {
                sb.append("CREATE INDEX ");
            }


            sb.append(catalog_idx.getTypeName()).append(" ON ").append(catalog_tbl.getTypeName()).append(" (");
            add = "";

            String jsonstring = catalog_idx.getExpressionsjson();

            if (jsonstring.isEmpty()) {
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    sb.append(add).append(catalog_colref.getColumn().getTypeName());
                    add = ", ";
                }
            } else {
                List<AbstractExpression> indexedExprs = null;
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(jsonstring,
                            new StmtTargetTableScan(catalog_tbl));
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if (indexedExprs != null) {
                    for (AbstractExpression expr : indexedExprs) {
                        sb.append(add).append(expr.explain(catalog_tbl.getTypeName()));
                        add = ", ";
                    }
                }
            }
            sb.append(")");

            String jsonPredicate = catalog_idx.getPredicatejson();
            if (!jsonPredicate.isEmpty()) {
                try {
                    AbstractExpression predicate = AbstractExpression.fromJSONString(jsonPredicate,
                        new StmtTargetTableScan(catalog_tbl));
                    sb.append(" WHERE ").append(predicate.explain(catalog_tbl.getTypeName()));
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            sb.append(";\n");
        }

        if (catalog_tbl.getIsdred()) {
            sb.append("DR TABLE ").append(catalog_tbl.getTypeName()).append(";\n");
        }

        sb.append("\n");
        // Canonical DDL generation for this table is done, now just hand the CREATE TABLE
        // statement to whoever might be interested (DDLCompiler, I'm looking in your direction)
        return table_sb.toString();
    }

    /**
     * Convert a Group (Role) into a DDL string.
     * sb The ddl
     * @param grp The group
     */
    public static void toSchema(StringBuilder sb, Group grp) {
        // Don't output the default roles because user cannot change them.
        if (grp.getTypeName().equalsIgnoreCase("ADMINISTRATOR") || grp.getTypeName().equalsIgnoreCase("USER")) {
            return;
        }

        final EnumSet<Permission> permissions = Permission.getPermissionSetForGroup(grp);
        sb.append("CREATE ROLE ").append(grp.getTypeName());

        String delimiter = " WITH ";
        for (Permission permission : permissions) {
            sb.append(delimiter).append(permission.name());
            delimiter = ", ";
        }

        sb.append(";\n");
    }

    public static void toSchema(StringBuilder sb, Function func)
    {
        String functionDDLTemplate = "CREATE FUNCTION %s FROM METHOD %s.%s;\n\n";
        sb.append(String.format(functionDDLTemplate, func.getFunctionname(), func.getClassname(), func.getMethodname()));
    }

    /**
     * Convert a Catalog Procedure into a DDL string.
     * @param proc
     */
    public static void toSchema(StringBuilder sb, Procedure proc)
    {
        // CRUD: hasJava (false), m_defaultproc (true)
        // SQL: hasJava (false), m_defaultproc (false), m_statements.m_items."SQL"
        // JAVA: hasJava (true, m_defaultproc (false)
        if (proc.getDefaultproc()) {
            return;
        }

        // Build the optional ALLOW clause.
        CatalogMap<GroupRef> roleList = proc.getAuthgroups();
        String add;
        String allowClause = new String();
        if (roleList.size() > 0) {
            add = "\n" + spacer + "ALLOW ";
            for (GroupRef role : roleList) {
                allowClause += add + role.getGroup().getTypeName();
                add = ", ";
            }
        }

        // Build the optional PARTITION clause.
        StringBuilder partitionClause = new StringBuilder();
        ProcedureAnnotation annot = (ProcedureAnnotation) proc.getAnnotation();
        if (CatalogUtil.isProcedurePartitioned(proc)) {
            if (annot != null && annot.classAnnotated) {
                partitionClause.append("--Annotated Partitioning Takes Precedence Over DDL Procedure Partitioning Statement\n--");
            }
            else {
                partitionClause.append("\n");
            }
            partitionClause.append(spacer);
            partitionClause.append(String.format(
                    "PARTITION ON TABLE %s COLUMN %s",
                    proc.getPartitiontable().getTypeName(),
                    proc.getPartitioncolumn().getTypeName() ));
            if (proc.getPartitionparameter() != 0) {
                partitionClause.append(String.format(
                        " PARAMETER %s",
                        String.valueOf(proc.getPartitionparameter()) ));
            }

            // For the second partition clause in 2p txn
            if (proc.getPartitioncolumn2() != null) {
                partitionClause.append(spacer);
                partitionClause.append(String.format(
                        "AND ON TABLE %s COLUMN %s",
                        proc.getPartitiontable2().getTypeName(),
                        proc.getPartitioncolumn2().getTypeName() ));
                if (proc.getPartitionparameter2() != 1) {
                    partitionClause.append(String.format(
                            " PARAMETER %s",
                            String.valueOf(proc.getPartitionparameter2()) ));
                }
            }
        }

        // Build the appropriate CREATE PROCEDURE statement variant.
        if (!proc.getHasjava()) {
            // SQL Statement procedure

            sb.append(String.format(
                    "CREATE PROCEDURE %s%s%s\n%sAS\nBEGIN\n%s%s",
                    proc.getClassname(),
                    allowClause,
                    partitionClause.toString(),
                    spacer,
                    spacer,
                    proc.getStatements().get("SQL0").getSqltext().trim()));

            for (int i = 1 ; i < proc.getStatements().size() ; i++ ) {
                sb.append(String.format(
                        "\n%s%s",
                        spacer,
                        proc.getStatements().get("SQL" + String.valueOf(i)).getSqltext().trim()));
            }
            sb.append("\nEND");
        }
        else {
            // Java Class
            sb.append(String.format(
                    "CREATE PROCEDURE %s%s\n%sFROM CLASS %s",
                    allowClause,
                    partitionClause.toString(),
                    spacer,
                    proc.getClassname()));
        }

        // The SQL statement variant may have terminated the CREATE PROCEDURE statement.
        if (!sb.toString().endsWith(";")) {
            sb.append(";");
        }

        // Give me some space man.
        sb.append("\n\n");
    }

    /**
     * Convert a catalog into a string containing all DDL statements.
     * @param catalog
     * @return String of DDL statements.
     */
    public static String toSchema(Catalog catalog)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("-- This file was generated by VoltDB version ");
        sb.append(VoltDB.instance().getVersionString());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        String time = sdf.format(System.currentTimeMillis());
        sb.append(" on: " + time + ".\n");

        sb.append("-- This file represents the current database schema.\n");
        sb.append("-- Use this file as input to reproduce the current database structure in another database instance.\n");
        sb.append("--\n");

        sb.append(batchSpecificComments);

        sb.append("-- If the schema declares Java stored procedures, be sure to load the .jar file\n");
        sb.append("-- with the classes before loading the schema. For example:\n");
        sb.append("--\n");
        sb.append("-- LOAD CLASSES voltdb-procs.jar;\n");
        sb.append("-- FILE ddl.sql;\n");

        for (Cluster cluster : catalog.getClusters()) {
            for (Database db : cluster.getDatabases()) {
                for (Group grp : db.getGroups()) {
                    toSchema(sb, grp);
                }
                sb.append("\n");

                List<Table> viewList = new ArrayList<>();

                CatalogMap<Table> tables = db.getTables();
                if (! tables.isEmpty()) {
                    sb.append(startBatch);
                    for (Table table : tables) {
                        Object annotation = table.getAnnotation();
                        if (annotation != null && ((TableAnnotation) annotation).ddl != null
                                && table.getMaterializer() != null) {
                            viewList.add(table);
                            continue;
                        }
                        toSchema(sb, table, null, CatalogUtil.isTableExportOnly(db, table),
                                (table.getPartitioncolumn() != null ? table.getPartitioncolumn().getName() : null), CatalogUtil.getExportTargetIfExportTableOrNullOtherwise(db, table));
                    }
                    // A View cannot precede a table that it depends on in the DDL
                    for (Table table : viewList) {
                        String viewQuery = ((TableAnnotation) table.getAnnotation()).ddl;
                        toSchema(sb, table, viewQuery, false, null, null);
                    }
                }

                CatalogMap<Procedure> procedures = db.getProcedures();
                if (! procedures.isEmpty()) {
                    for (Procedure proc : procedures) {
                        toSchema(sb, proc);
                    }
                }

                CatalogMap<Function> functions = db.getFunctions();
                if (! functions.isEmpty()) {
                    for (Function func : functions) {
                        toSchema(sb, func);
                    }
                }

                if (! tables.isEmpty()) {
                    sb.append(endBatch);
                }
            }
        }

        if (dumpSchema) {
            String ts = new SimpleDateFormat("MMddHHmmssSSS").format(new Date());
            File f = new File(String.format("/tmp/canonical-%s.sql", ts));
            try {
                FileWriter fw = new FileWriter(f);
                fw.write(sb.toString());
                fw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return sb.toString();
    }

    /**
     * Given a schema, strips out inline batch statements and associated comments.
     * @param schema The given schema
     * @return The given schema without batch statements and comments.
     */
    public static String toSchemaWithoutInlineBatches(String schema) {
        StringBuilder sb = new StringBuilder(schema);
        int i = sb.indexOf(batchSpecificComments);
        if (i != -1) {
            sb.delete(i, i + batchSpecificComments.length());
        }
        i = sb.indexOf(startBatch);
        if (i != -1) {
            sb.delete(i, i + startBatch.length());
        }
        i = sb.indexOf(endBatch);
        if (i != -1) {
            sb.delete(i, i + endBatch.length());
        }
        return sb.toString();
    }

}
