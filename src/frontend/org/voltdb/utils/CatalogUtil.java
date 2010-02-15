/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.utils;

import java.util.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.catalog.*;
import org.voltdb.types.*;

/**
 *
 */
public abstract class CatalogUtil {

    public static final String CATALOG_FILENAME = "catalog.txt";

    public static String loadCatalogFromJar(String pathToCatalog, Logger log) {
        assert(pathToCatalog != null);

        String serializedCatalog = null;
        try {
            serializedCatalog = JarReader.readFileFromJarfile(pathToCatalog, CATALOG_FILENAME);
        } catch (Exception e) {
            if (log != null)
                log.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CatalogReadFailure.name(), e);
            return null;
        }

        return serializedCatalog;
    }

    /**
     * Get a unique id for a plan fragment by munging the indices of it's parents
     * and grandparents in the catalog.
     *
     * @param frag Catalog fragment to identify
     * @return unique id for fragment
     */
    public static long getUniqueIdForFragment(PlanFragment frag) {
        long retval = 0;
        CatalogType parent = frag.getParent();
        retval = ((long) parent.getParent().getRelativeIndex()) << 32;
        retval += ((long) parent.getRelativeIndex()) << 16;
        retval += frag.getRelativeIndex();

        return retval;
    }

    /**
     *
     * @param catalogTable
     * @return An empty table with the same schema as a given catalog table.
     */
    public static VoltTable getVoltTable(Table catalogTable) {
        List<Column> catalogColumns = CatalogUtil.getSortedCatalogItems(catalogTable.getColumns(), "index");

        VoltTable.ColumnInfo[] columns = new VoltTable.ColumnInfo[catalogColumns.size()];

        int i = 0;
        for (Column catCol : catalogColumns) {
            columns[i++] = new VoltTable.ColumnInfo(catCol.getTypeName(), VoltType.get((byte)catCol.getType()));
        }

        return new VoltTable(columns);
    }

    /**
     * Given a set of catalog items, return a sorted list of them, sorted by
     * the value of a specified field. The field is specified by name. If the
     * field doesn't exist, trip an assertion. This is primarily used to sort
     * a table's columns or a procedure's parameters.
     *
     * @param <T> The type of item to sort.
     * @param items The set of catalog items.
     * @param sortFieldName The name of the field to sort on.
     * @return A list of catalog items, sorted on the specified field.
     */
    public static <T extends CatalogType> List<T> getSortedCatalogItems(CatalogMap<T> items, String sortFieldName) {
        assert(items != null);
        assert(sortFieldName != null);

        // build a treemap based on the field value
        TreeMap<Object, T> map = new TreeMap<Object, T>();
        boolean hasField = false;
        for (T item : items) {
            // check the first time through for the field
            if (hasField == false)
                hasField = item.getFields().contains(sortFieldName);
            assert(hasField == true);

            map.put(item.getField(sortFieldName), item);
        }

        // create a sorted list from the map
        ArrayList<T> retval = new ArrayList<T>();
        for (T item : map.values()) {
            retval.add(item);
        }

        return retval;
    }

    /**
     * For a given Table catalog object, return the PrimaryKey Index catalog object
     * @param catalogTable
     * @return The index representing the primary key.
     * @throws Exception if the table does not define a primary key
     */
    public static Index getPrimaryKeyIndex(Table catalogTable) throws Exception {

        // We first need to find the pkey constraint
        Constraint catalog_constraint = null;
        for (Constraint c : catalogTable.getConstraints()) {
            if (c.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                catalog_constraint = c;
                break;
            }
        }
        if (catalog_constraint == null) {
            throw new Exception("ERROR: Table '" + catalogTable.getTypeName() + "' does not have a PRIMARY KEY constraint");
        }

        // And then grab the index that it is using
        return (catalog_constraint.getIndex());
    }

    /**
     * Return all the of the primary key columns for a particular table
     * If the table does not have a primary key, then the returned list will be empty
     * @param catalogTable
     * @return An ordered list of the primary key columns
     */
    public static Collection<Column> getPrimaryKeyColumns(Table catalogTable) {
        Collection<Column> columns = new ArrayList<Column>();
        Index catalog_idx = null;
        try {
            catalog_idx = CatalogUtil.getPrimaryKeyIndex(catalogTable);
        } catch (Exception ex) {
            // IGNORE
            return (columns);
        }
        assert(catalog_idx != null);

        for (ColumnRef catalog_col_ref : getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
            columns.add(catalog_col_ref.getColumn());
        }
        return (columns);
    }

    /**
     * Convert a Table catalog object into the proper SQL DDL, including all indexes,
     * constraints, and foreign key references.
     * @param catalog_tbl
     * @return SQL Schema text representing the table.
     */
    public static String toSchema(Table catalog_tbl) {
        assert(!catalog_tbl.getColumns().isEmpty());
        final String spacer = "   ";

        Set<Index> skip_indexes = new HashSet<Index>();
        Set<Constraint> skip_constraints = new HashSet<Constraint>();

        String ret = "CREATE TABLE " + catalog_tbl.getTypeName() + " (";

        // Columns
        String add = "\n";
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            VoltType col_type = VoltType.get((byte)catalog_col.getType());

            // this next assert would be great if we dealt with default values well
            //assert(! ((catalog_col.getDefaultvalue() == null) && (catalog_col.getNullable() == false) ) );

            ret += add + spacer + catalog_col.getTypeName() + " " +
                   col_type.toSQLString() +
                   (col_type == VoltType.STRING && catalog_col.getSize() > 0 ? "(" + catalog_col.getSize() + ")" : "");

            // Default value
            String defaultvalue = catalog_col.getDefaultvalue();
            //VoltType defaulttype = VoltType.get((byte)catalog_col.getDefaulttype());
            boolean nullable = catalog_col.getNullable();
            // TODO: Shouldn't have to check whether the string contains "null"
            if (defaultvalue != null && defaultvalue.toLowerCase().equals("null") && nullable) {
                defaultvalue = null;
            }
            else { // XXX: if (defaulttype != VoltType.VOLTFUNCTION) {
                // TODO: Escape strings properly
                defaultvalue = "'" + defaultvalue + "'";
            }
            ret += " DEFAULT " + (defaultvalue != null ? defaultvalue : "NULL") +
                   (!nullable ? " NOT NULL" : "");

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
                IndexType idx_type = IndexType.get(catalog_idx.getType());
                String idx_suffix = idx_type.getSQLSuffix();

                ret += add + spacer +
                       (!idx_suffix.isEmpty() ? "CONSTRAINT " + catalog_const.getTypeName() + " " : "") +
                       (const_type == ConstraintType.PRIMARY_KEY ? "PRIMARY KEY" : "UNIQUE") + " (";

                String col_add = "";
                for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                    ret += col_add + catalog_colref.getColumn().getTypeName();
                    col_add = ", ";
                } // FOR
                ret += ")";
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
        ret += "\n);\n";

        // All other Indexes
        for (Index catalog_idx : catalog_tbl.getIndexes()) {
            if (skip_indexes.contains(catalog_idx)) continue;

            ret += "CREATE INDEX " + catalog_idx.getTypeName() +
                   " ON " + catalog_tbl.getTypeName() + " (";
            add = "";
            for (ColumnRef catalog_colref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
                ret += add + catalog_colref.getColumn().getTypeName();
                add = ", ";
            }
            ret += ");\n";
        }

        return ret;
    }
}
