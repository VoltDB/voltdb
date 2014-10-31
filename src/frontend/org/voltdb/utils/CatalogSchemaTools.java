/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
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

    final static String spacer = "   ";

    /**
     * Convert a Table catalog object into the proper SQL DDL, including all indexes,
     * constraints, and foreign key references.
     * @param Table - object to be analyzed
     * @param String - the Query if this Table is a View
     * @param Boolean - true if this Table is an Export Table
     * @return SQL Schema text representing the table.
     */
    public static void toSchema(StringBuilder sb, Table catalog_tbl, String viewQuery, boolean isExportTable) {
        assert(!catalog_tbl.getColumns().isEmpty());
        boolean tableIsView = (viewQuery != null);

        Set<Index> skip_indexes = new HashSet<Index>();
        Set<Constraint> skip_constraints = new HashSet<Constraint>();

        if (tableIsView) {
            sb.append("CREATE VIEW " + catalog_tbl.getTypeName() + " (");
        }
        else {
            sb.append("CREATE TABLE " + catalog_tbl.getTypeName() + " (");
        }

        // Columns
        String add = "\n";
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType col_type = VoltType.get((byte)catalog_col.getType());

            // this next assert would be great if we dealt with default values well
            //assert(! ((catalog_col.getDefaultvalue() == null) && (catalog_col.getNullable() == false) ) );

            if (tableIsView) {
                sb.append(add + spacer + catalog_col.getTypeName());
                add = ",\n";
                continue;
            }

            sb.append(add + spacer + catalog_col.getTypeName() + " " + col_type.toSQLString() +
                    ((col_type == VoltType.STRING || col_type == VoltType.VARBINARY) &&
                    catalog_col.getSize() > 0 ? "(" + catalog_col.getSize() +
                    (catalog_col.getInbytes() ? " BYTES" : "") + ")" : "") );

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
                sb.append((!nullable ? " NOT NULL" : "") );
            }
            else {
                sb.append(" DEFAULT " + (defaultvalue != null ? defaultvalue : "NULL") +
                        (!nullable ? " NOT NULL" : "") );
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
                            sb.append(" REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" +
                                    catalog_fkey_col.getTypeName() + ")" );
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

                    sb.append(add + spacer);
                    if (!catalog_const.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX)) {
                        sb.append("CONSTRAINT " + catalog_const.getTypeName() + " ");
                    }
                    if (const_type == ConstraintType.PRIMARY_KEY || const_type == ConstraintType.UNIQUE) {
                        if (const_type == ConstraintType.PRIMARY_KEY) {
                            sb.append("PRIMARY KEY (");
                        }
                        else {
                            if (catalog_idx.getAssumeunique()) {
                                sb.append("ASSUMEUNIQUE (");
                            }
                            else {
                                sb.append("UNIQUE (");
                            }
                        }
                        String col_add = "";
                        String exprStrings = new String();
                        if (catalog_idx.getExpressionsjson() != null && !catalog_idx.getExpressionsjson().equals("")) {
                            StmtTargetTableScan tableScan = new StmtTargetTableScan(catalog_tbl, catalog_tbl.getTypeName());
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
                            sb.append(col_add + exprStrings);
                        }
                        else {
                            for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                                sb.append(col_add + catalog_colref.getColumn().getTypeName() );
                                col_add = ", ";
                            } // FOR
                        }
                        sb.append(")");
                    }
                    else
                    if (const_type == ConstraintType.LIMIT) {
                        sb.append("LIMIT PARTITION ROWS " + String.valueOf(catalog_tbl.getTuplelimit()) );
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
                sb.append(add + spacer + "CONSTRAINT " + catalog_const.getTypeName() + " " +
                                         "FOREIGN KEY (" + our_columns + ") " +
                                         "REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" + fkey_columns + ")" );
            }
            skip_constraints.add(catalog_const);
        }

        if (catalog_tbl.getTuplelimit() != Integer.MAX_VALUE) {
            sb.append(add + spacer + "LIMIT PARTITION ROWS " + String.valueOf(catalog_tbl.getTuplelimit()) );
        }

        if (viewQuery != null) {
            sb.append("\n) AS \n");
            sb.append(spacer + viewQuery + ";\n");
        }
        else {
            sb.append("\n);\n");
        }

        // Partition Table
        if (catalog_tbl.getPartitioncolumn() != null && viewQuery == null) {
            sb.append("PARTITION TABLE " + catalog_tbl.getTypeName() + " ON COLUMN " +
                    catalog_tbl.getPartitioncolumn().getTypeName() + ";\n" );
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


            sb.append(catalog_idx.getTypeName() +
                   " ON " + catalog_tbl.getTypeName() + " (");
            add = "";

            String jsonstring = catalog_idx.getExpressionsjson();

            if (jsonstring.isEmpty()) {
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    sb.append(add + catalog_colref.getColumn().getTypeName() );
                    add = ", ";
                }
            } else {
                List<AbstractExpression> indexedExprs = null;
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(jsonstring,
                            new StmtTargetTableScan(catalog_tbl, catalog_tbl.getTypeName()));
                } catch (JSONException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                for (AbstractExpression expr : indexedExprs) {
                    sb.append(add + expr.explain(catalog_tbl.getTypeName()) );
                    add = ", ";
                }
            }
            sb.append(");\n");
        }

        if (isExportTable) {
            sb.append("EXPORT TABLE " + catalog_tbl.getTypeName() + ";\n");
        }

        sb.append("\n");
    }

    /**
     * Convert a Group (Role) into a DDL string.
     * @param Group
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

    /**
     * Convert a Catalog Procedure into a DDL string.
     * @param Procedure proc
     */
    public static void toSchema(StringBuilder sb, Procedure proc)
    {
        CatalogMap<GroupRef> roleList = proc.getAuthgroups();
        String add;
        String roleNames = new String();
        if (roleList.size() > 0) {
            add = "\n" + spacer + "ALLOW ";
            for (GroupRef role : roleList) {
                roleNames += add + role.getGroup().getTypeName();
                add = ", ";
            }
        }

        // Groovy: hasJava (true), m_language ("GROOVY"), m_defaultproc (false)
        // CRUD: hasJava (false), m_language (""), m_defaultproc (true)
        // SQL: hasJava (false), m_language(""), m_defaultproc (false), m_statements.m_items."SQL"
        // JAVA: hasJava (true, m_language ("JAVA"), m_defaultproc (false)
        if (proc.getDefaultproc()) {
            return;
        }
        ProcedureAnnotation annot = (ProcedureAnnotation) proc.getAnnotation();
        if (!proc.getHasjava()) {
            // SQL Statement procedure
            sb.append("CREATE PROCEDURE " + proc.getClassname() + roleNames + "\n" + spacer + "AS\n");
            String sqlStmt = proc.getStatements().get("SQL").getSqltext();
            if (sqlStmt.endsWith(";")) {
                sb.append(spacer + sqlStmt + "\n");
            }
            else {
                sb.append(spacer + sqlStmt + ";\n");
            }
        }
        else if (proc.getLanguage().equals("JAVA")) {
            // Java Class
            sb.append("CREATE PROCEDURE " + roleNames + "\n" + spacer + "FROM CLASS " + proc.getClassname() + ";\n");
        }
        else {
            // Groovy procedure
            sb.append("CREATE PROCEDURE " + proc.getClassname() + roleNames + "\n" + spacer + "AS ###");
            sb.append(annot.scriptImpl + "### LANGUAGE GROOVY;\n");
        }
        if (proc.getSinglepartition()) {
            if (annot != null && annot.classAnnotated) {
                sb.append("--Annotated Partitioning Takes Precedence Over DDL Procedure Partitioning Statement\n--");
            }
            sb.append("PARTITION PROCEDURE " + proc.getTypeName() + " ON TABLE " +
                    proc.getPartitiontable().getTypeName() + " COLUMN " +
                    proc.getPartitioncolumn().getTypeName() );
            if (proc.getPartitionparameter() != 0)
                sb.append(" PARAMETER " + String.valueOf(proc.getPartitionparameter()) );
            sb.append(";\n\n");
        }
        else {
            sb.append("\n");
        }
    }

    /**
     * Convert a List of class names into a string containing equivalent IMPORT CLASS DDL statements.
     * @param String[] classNames
     * @return Set of Catalog Tables.
     */
    public static void toSchema(StringBuilder sb, String[] importLines)
    {
        for (String importLine : importLines) {
            sb.append(importLine);
        }
    }


    /**
     * Convert a catalog into a string containing all DDL statements.
     * @param String[] classNames
     * @return String of DDL statements.
     */
    public static String toSchema(Catalog catalog, String[] importLines)
    {
        StringBuilder sb = new StringBuilder();

        for (Cluster cluster : catalog.getClusters()) {
            for (Database db : cluster.getDatabases()) {
                toSchema(sb, importLines);

                for (Group grp : db.getGroups()) {
                    toSchema(sb, grp);
                }
                sb.append("\n");

                List<Table> viewList = new ArrayList<Table>();
                for (Table table : db.getTables()) {
                    Object annotation = table.getAnnotation();
                    if (annotation != null && ((TableAnnotation)annotation).ddl != null
                            && table.getMaterializer() != null) {
                        viewList.add(table);
                        continue;
                    }
                    toSchema(sb, table, null, CatalogUtil.isTableExportOnly(db, table));
                }
                // A View cannot preceed a table that it depends on in the DDL
                for (Table table : viewList) {
                    String viewQuery = ((TableAnnotation)table.getAnnotation()).ddl;
                    toSchema(sb, table, viewQuery, CatalogUtil.isTableExportOnly(db, table));
                }
                sb.append("\n");

                for (Procedure proc : db.getProcedures()) {
                    toSchema(sb, proc);
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

}
