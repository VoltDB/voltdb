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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
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
import org.voltdb.compilereport.ProcedureAnnotation;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ConstraintType;

/**
 *
 */
public abstract class CatalogSchemaTools {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    public static final String CATALOG_FILENAME = "catalog.txt";
    public static final String CATALOG_BUILDINFO_FILENAME = "buildinfo.txt";


    /**
     * Convert a Table catalog object into the proper SQL DDL, including all indexes,
     * constraints, and foreign key references.
     * @param Table - object to be analyzed
     * @param String - the Query if this Table is a View
     * @param Boolean - true if this Table is an Export Table
     * @return SQL Schema text representing the table.
     */
    public static String toSchema(Table catalog_tbl, String viewQuery, boolean isExportTable) {
        assert(!catalog_tbl.getColumns().isEmpty());
        final String spacer = "   ";
        boolean tableIsView = (viewQuery != null);

        Set<Index> skip_indexes = new HashSet<Index>();
        Set<Constraint> skip_constraints = new HashSet<Constraint>();

        String ret = new String();
        if (tableIsView) {
            ret = "CREATE VIEW " + catalog_tbl.getTypeName() + " (";
        }
        else {
            ret = "CREATE TABLE " + catalog_tbl.getTypeName() + " (";
        }

        // Columns
        String add = "\n";
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType col_type = VoltType.get((byte)catalog_col.getType());

            // this next assert would be great if we dealt with default values well
            //assert(! ((catalog_col.getDefaultvalue() == null) && (catalog_col.getNullable() == false) ) );

            if (tableIsView) {
                ret += add + spacer + catalog_col.getTypeName();
                add = ",\n";
                continue;
            }

            ret += add + spacer + catalog_col.getTypeName() + " " + col_type.toSQLString() +
                    (col_type == VoltType.STRING &&
                    catalog_col.getSize() > 0 ? "(" + catalog_col.getSize() + ")" : "");

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
            else { // XXX: if (defaulttype != VoltType.VOLTFUNCTION) {
                // TODO: Escape strings properly
                defaultvalue = "'" + defaultvalue + "'";
            }
            if (defaultvalue == null) {
                ret += (!nullable ? " NOT NULL" : "");
            }
            else {
                ret += " DEFAULT " + (defaultvalue != null ? defaultvalue : "NULL") +
                        (!nullable ? " NOT NULL" : "");
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
                            ret += " REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" + catalog_fkey_col.getTypeName() + ")";
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

                    ret += add + spacer;
                    if (!catalog_const.getTypeName().startsWith("SYS_")) {
                        ret += "CONSTRAINT " + catalog_const.getTypeName() + " ";
                    }
                    if (const_type == ConstraintType.PRIMARY_KEY || const_type == ConstraintType.UNIQUE) {
                        if (const_type == ConstraintType.PRIMARY_KEY) {
                            ret += "PRIMARY KEY (";
                        }
                        else {
                            if (catalog_idx.getAssumeunique()) {
                                ret += "ASSUMEUNIQUE (";
                            }
                            else {
                                ret += "UNIQUE (";
                            }
                        }
                        String col_add = "";
                        for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                            ret += col_add + catalog_colref.getColumn().getTypeName();
                            col_add = ", ";
                        } // FOR
                        ret += ")";
                    }
                    else
                    if (const_type == ConstraintType.LIMIT) {
                        ret += "LIMIT PARTITION ROWS " + String.valueOf(catalog_tbl.getTuplelimit());
                    }

                }
                skip_indexes.add(catalog_idx);

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
                ret += add + spacer + "CONSTRAINT " + catalog_const.getTypeName() + " " +
                                      "FOREIGN KEY (" + our_columns + ") " +
                                      "REFERENCES " + catalog_fkey_tbl.getTypeName() + " (" + fkey_columns + ")";
            }
            skip_constraints.add(catalog_const);
        }

        if (viewQuery != null) {
            ret += "\n) AS \n";
            ret += spacer + viewQuery + ";\n";
        }
        else {
            ret += "\n);\n";
        }

        // Partition Table
        if (catalog_tbl.getPartitioncolumn() != null && viewQuery == null) {
            ret += "PARTITION TABLE " + catalog_tbl.getTypeName() + " ON COLUMN " +
                    catalog_tbl.getPartitioncolumn().getTypeName() + ";\n";
        }

        // All other Indexes
        for (Index catalog_idx : catalog_tbl.getIndexes()) {
            if (skip_indexes.contains(catalog_idx)) continue;

            if (catalog_idx.getUnique()) {
                if (catalog_idx.getAssumeunique()) {
                    ret += "CREATE ASSUMEUNIQUE INDEX ";
                }
                else {
                    ret += "CREATE UNIQUE INDEX ";
                }
            }
            else {
                ret += "CREATE INDEX ";
            }


            ret += catalog_idx.getTypeName() +
                   " ON " + catalog_tbl.getTypeName() + " (";
            add = "";

            String jsonstring = catalog_idx.getExpressionsjson();

            if (jsonstring.isEmpty()) {
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    ret += add + catalog_colref.getColumn().getTypeName();
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
                    ret += add + expr.explain(catalog_tbl.getTypeName());
                    add = ", ";
                }
            }
            ret += ");\n";
        }

        if (isExportTable) {
            ret += "EXPORT TABLE " + catalog_tbl.getTypeName() + ";\n";
        }

        ret += "\n";

        return ret;
    }

    /**
     * Convert a Group (Role) into a DDL string.
     * @param Group
     * @return DDL text representing the Group.
     */
    public static String toSchema(Group grp) {
        String ret = new String();

        if (grp.getAdhoc() || grp.getDefaultproc() || grp.getSysproc()) {
            ret += "CREATE ROLE " + grp.getTypeName() + " WITH ";
            if (grp.getAdhoc()) {
                if (grp.getDefaultproc() || grp.getSysproc()) {
                    ret += "ADHOC, ";
                }
                else {
                    ret += "ADHOC;\n";
                    return ret;
                }
            }
            if (grp.getDefaultproc()) {
                if (grp.getSysproc()) {
                    ret += "DEFAULTPROC, ";
                }
                else {
                    ret += "DEFAULTPROC;\n";
                    return ret;
                }
            }
            if (grp.getSysproc()) {
                ret += "SYSPROC;\n";
            }
        }
        else {
            ret += "CREATE ROLE " + grp.getTypeName() + ";\n";
        }

        return ret;
    }

    /**
     * Convert a Catalog Procedure into a DDL string.
     * @param CatalogMap<Connector>
     * @return Set of Catalog Tables.
     */
    public static String toSchema(Procedure proc)
    {
        String ret = null;
        CatalogMap<GroupRef> roleList = proc.getAuthgroups();
        String spacer = "   ";
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
            return "";
        }
        if (!proc.getHasjava()) {
            // SQL Statement procedure
            ret = "CREATE PROCEDURE " + proc.getTypeName() + roleNames + "\n" + spacer + "AS\n";
            String sqlStmt = proc.getStatements().get("SQL").getSqltext().trim();
            if (sqlStmt.endsWith(";"))
                sqlStmt = sqlStmt.substring(0, sqlStmt.length()-1);
            ret += spacer + sqlStmt + ";\n";
        }
        else if (proc.getLanguage().equals("JAVA")) {
            // Java Class
            ret = "CREATE PROCEDURE " + roleNames + "\n" + spacer + "FROM CLASS " + proc.getClassname() + ";\n";
        }
        else {
            // Groovy procedure
            ret = "CREATE PROCEDURE " + proc.getTypeName() + roleNames + "\n" + spacer + "AS ###\n";
            ProcedureAnnotation annot = (ProcedureAnnotation) proc.getAnnotation();
            ret += annot.scriptImpl + "\n### LANGUAGE GROOVY;\n";
        }
        if (proc.getSinglepartition()) {
            ret += "PARTITION PROCEDURE " + proc.getTypeName() + " ON TABLE " +
                    proc.getPartitiontable().getTypeName() + " COLUMN " +
                    proc.getPartitioncolumn().getTypeName() + ";\n\n";
        }
        else {
            ret += "\n";
        }

        return ret;
    }

    /**
     * Convert a List of class names into a string containing equivalent IMPORT CLASS DDL statements.
     * @param List<String>
     * @return Set of Catalog Tables.
     */
    public static String toSchema(String[] classNames)
    {
        String ret = new String();
        for (String className : classNames) {
            ret += "IMPORT CLASS " + className + ";\n";
        }
        return ret;
    }


    public static String toSchema(Database db, String[] classNames)
    {
        String ret = new String();
        for (Group grp : db.getGroups()) {
            ret += toSchema(grp);
        }
        ret += "\n";

        for (Table table : db.getTables()) {
            String viewQuery = ((TableAnnotation)table.getAnnotation()).ddl;
            ret += CatalogSchemaTools.toSchema(table, viewQuery, CatalogUtil.isTableExportOnly(db, table));
        }
        ret += "\n";

        for (Procedure proc : db.getProcedures()) {
            ret += CatalogSchemaTools.toSchema(proc);
        }
        ret += "\n";

        ret += CatalogSchemaTools.toSchema(classNames);

        return ret;
    }

    public static String encodeDDL(String rawDDL)
    {
        final String[] commands = rawDDL.split("\n");
        String encodeLines = new String();
        for (String command : commands) {
            command = command.trim();
            encodeLines += Encoder.hexEncode(command) + "\n";
        }
        return Encoder.compressAndBase64Encode(encodeLines);
    }

    public static String decodeDDL(String compressedEncoded)
    {
        String ret = new String();
        final String ddl = Encoder.decodeBase64AndDecompress(compressedEncoded);
        final String[] commands = ddl.split("\n");
        for (String command : commands) {
            String decoded_cmd = Encoder.hexDecodeToString(command);
            decoded_cmd = decoded_cmd.trim();
            if (decoded_cmd.length() == 0) {
                continue;
            }
            ret += decoded_cmd + "\n";
        }
        return ret;
    }

}
