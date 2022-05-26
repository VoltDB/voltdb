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

package org.voltdb.sysprocs.saverestore;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.voltdb.ParameterConverter;
import org.voltdb.TableType;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTypeUtil;

public abstract class SavedTableConverter
{

    public static boolean needsConversion(VoltTable inputTable, Table outputTableSchema, String DrRole,
            boolean isRecover) throws VoltTypeException {
        return needsConversion(inputTable, outputTableSchema,
                outputTableSchema.getIsdred() && DrRoleType.XDCR.value().equals(DrRole),
                CatalogUtil.needsViewHiddenColumn(outputTableSchema),
                TableType.isPersistentMigrate(outputTableSchema.getTabletype()), isRecover);
    }

    static boolean needsConversion(VoltTable inputTable,
                                          Table outputTableSchema,
                                          boolean preserveDRHiddenColumn,
                                          boolean preserveViewHiddenColumn,
                                          boolean preserveMigrateHiddenColumn,
                                          boolean isRecover) {
        int columnsToMatch = inputTable.getColumnCount()
                - (preserveDRHiddenColumn ? 1 : 0)
                - (preserveViewHiddenColumn ? 1 : 0)
                - (preserveMigrateHiddenColumn ? 1 : 0);
        // Cannot DR views! Those two flags must not be true at the same time!
        assert((preserveDRHiddenColumn && preserveViewHiddenColumn) == false);
        // We are expecting the hidden column in inputTable
        if (columnsToMatch != outputTableSchema.getColumns().size()) {
            return true;
        }
        if ((preserveDRHiddenColumn || preserveViewHiddenColumn)
                && inputTable.getColumnType(columnsToMatch) != VoltType.BIGINT) {
            // The hidden column, in either use case, must be BIGINT type.
            return true;
        }
        // Make sure input is using the reserved column name of the hidden column
        if (preserveDRHiddenColumn
                && ! inputTable.getColumnName(columnsToMatch).equalsIgnoreCase(CatalogUtil.DR_HIDDEN_COLUMN_NAME)) {
            // passive DR table to active DR table, must be converted
            return true;
        }
        if (preserveViewHiddenColumn
                && ! inputTable.getColumnName(columnsToMatch).equalsIgnoreCase(CatalogUtil.VIEW_HIDDEN_COLUMN_NAME)) {
            return true;
        }

        // There may be more than one hidden column. The one for migrate is the last one
        if (preserveMigrateHiddenColumn) {
           if (!isRecover) {
               // For restore we need to mutate the migrate column to nulls
               return true;
           }
           int migrateHiddenIndex = columnsToMatch + ((preserveDRHiddenColumn || preserveViewHiddenColumn) ? 1 : 0);
           if (inputTable.getColumnType(migrateHiddenIndex) != VoltType.BIGINT) {
               return true;
           }
           if (!inputTable.getColumnName(columnsToMatch).equalsIgnoreCase(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME)) {
               return true;
           }
        }
        for (int ii = 0; ii < columnsToMatch; ii++) {
            final String name = inputTable.getColumnName(ii);
            final VoltType type = inputTable.getColumnType(ii);
            final Column column = outputTableSchema.getColumns().get(name);

            if (column == null) {
                return true;
            }

            if (column.getIndex() != ii) {
                return true;
            }

            if (column.getType() != type.getValue()) {
                return true;
            }
        }
        return false;
    }

    public static VoltTable convertTable(VoltTable inputTable,
            Table outputTableSchema, String DrRole, boolean isRecover) throws VoltTypeException {
        return convertTable(inputTable, outputTableSchema,
                outputTableSchema.getIsdred() && DrRoleType.XDCR.value().equals(DrRole),
                CatalogUtil.needsViewHiddenColumn(outputTableSchema),
                TableType.isPersistentMigrate(outputTableSchema.getTabletype()), isRecover);
    }

    static VoltTable convertTable(VoltTable inputTable,
                                         Table outputTableSchema,
                                         boolean preserveDRHiddenColumn,
                                         boolean preserveViewHiddenColumn,
                                         boolean preserveMigrateHiddenColumn,
                                         boolean isRecover) throws VoltTypeException {
        VoltTable newTable;
        // if the DR hidden column should be preserved in conversion, append it to the end of target schema
        int drColumnInx = -1;
        if (preserveDRHiddenColumn) {
            if (preserveMigrateHiddenColumn) {
                newTable = CatalogUtil.getVoltTable(outputTableSchema,  CatalogUtil.DR_HIDDEN_COLUMN_INFO, CatalogUtil.MIGRATE_HIDDEN_COLUMN_INFO);
                drColumnInx = newTable.getColumnCount() - 2;
            } else {
                newTable = CatalogUtil.getVoltTable(outputTableSchema, CatalogUtil.DR_HIDDEN_COLUMN_INFO);
                drColumnInx = newTable.getColumnCount() - 1;
            }
        } else if (preserveViewHiddenColumn) {
            newTable = CatalogUtil.getVoltTable(outputTableSchema, CatalogUtil.VIEW_HIDDEN_COLUMN_INFO);
        } else if (preserveMigrateHiddenColumn) {
            newTable = CatalogUtil.getVoltTable(outputTableSchema, CatalogUtil.MIGRATE_HIDDEN_COLUMN_INFO);
        } else {
            newTable = CatalogUtil.getVoltTable(outputTableSchema);
        }

        Map<Integer, Integer> columnCopyIndexMap =
            computeColumnCopyIndexMap(inputTable, newTable);

        boolean addHiddenViewColumn = preserveViewHiddenColumn &&
                ! columnCopyIndexMap.containsKey(newTable.getColumnCount() - 1);
        if (addHiddenViewColumn) {
            throw new RuntimeException("Cannot find internal group counts in the snapshot for view without a COUNT(*): " + outputTableSchema.getTypeName());
        }
        // if original table does not have hidden column present, we need to add
        boolean addDRHiddenColumn = preserveDRHiddenColumn &&
                !columnCopyIndexMap.containsKey(drColumnInx);
        Column catalogColumnForHiddenColumn = null;
        if (addDRHiddenColumn) {
            catalogColumnForHiddenColumn = new Column();
            catalogColumnForHiddenColumn.setName(CatalogUtil.DR_HIDDEN_COLUMN_NAME);
            catalogColumnForHiddenColumn.setType(VoltType.BIGINT.getValue());
            catalogColumnForHiddenColumn.setSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
            catalogColumnForHiddenColumn.setInbytes(false);
            // small hack here to let logic below fill VoltType.NULL_BIGINT in for the hidden column
            // actually this column is not nullable in EE, but it will be set to correct value before
            // insert(restore) to the corresponding table
            catalogColumnForHiddenColumn.setNullable(true);
        }

        Column catalogColumnForMigrateHiddenColumn = null;
        if (preserveMigrateHiddenColumn) {
            catalogColumnForMigrateHiddenColumn = new Column();
            catalogColumnForMigrateHiddenColumn.setName(CatalogUtil.MIGRATE_HIDDEN_COLUMN_NAME);
            catalogColumnForMigrateHiddenColumn.setType(VoltType.BIGINT.getValue());
            catalogColumnForMigrateHiddenColumn.setSize(VoltType.BIGINT.getLengthInBytesForFixedTypes());
            catalogColumnForMigrateHiddenColumn.setInbytes(false);
            catalogColumnForMigrateHiddenColumn.setNullable(true);
            if (!isRecover) {
                // If this is a restore force all migrate values to NULL
                columnCopyIndexMap.remove(newTable.getColumnCount()-1);
            }
        }

        // Copy all the old tuples into the new table
        while (inputTable.advanceRow()) {
            Object[] coerced_values = new Object[newTable.getColumnCount()];
            boolean drHiddenColumnAdded = !preserveDRHiddenColumn;
            for (int i = 0; i < newTable.getColumnCount(); i++) {
                if (columnCopyIndexMap.containsKey(i)) {
                    int origColumnIndex = columnCopyIndexMap.get(i);
                    // For column we have in new table convert and make compatible value.
                    coerced_values[i] = ParameterConverter.tryToMakeCompatible(
                            newTable.getColumnType(i).classFromType(),
                            inputTable.get(origColumnIndex,
                                    inputTable.getColumnType(origColumnIndex)));
                } else {
                    // otherwise if it's nullable, insert null,
                    Column catalogColumn =
                        outputTableSchema.getColumns().
                        get(newTable.getColumnName(i));
                    // construct an artificial catalog column for hidden columns
                    if (catalogColumn == null) {
                        // add dr hidden column first
                        if (preserveDRHiddenColumn && !drHiddenColumnAdded) {
                            catalogColumn = catalogColumnForHiddenColumn;
                            drHiddenColumnAdded = true;
                        } else if (preserveMigrateHiddenColumn && drHiddenColumnAdded) {
                            // add migrate hidden column afte dr hidden
                            catalogColumn = catalogColumnForMigrateHiddenColumn;
                        }
                    }
                    VoltType default_type = VoltType.get((byte)catalogColumn.getDefaulttype());
                    if (default_type != VoltType.INVALID)  {
                        // if there is a default value for this table/column
                        // insert the default value
                        try {
                            coerced_values[i] =
                                VoltTypeUtil.
                                getObjectFromString(default_type,
                                                    catalogColumn.getDefaultvalue());
                        }  catch (ParseException e) {
                            String message = "Column: ";
                            message += newTable.getColumnName(i);
                            message += " has an unparseable default: ";
                            message += catalogColumn.getDefaultvalue();
                            message += " for VoltType: ";
                            message += default_type.toString();
                            throw new VoltTypeException(message);
                        }
                    } else if (catalogColumn.getNullable()) {
                        coerced_values[i] = null;
                    } else {
                        throw new VoltTypeException("Column: " +
                                                    newTable.getColumnName(i) +
                                                    " has no default " +
                                                    "and null is not permitted");
                    }
                }
            }

            newTable.addRow(coerced_values);
        }

        return newTable;
    }

    /**
     * @param origTable a table with the original saved-to-disk schema
     * @param newTable a table with the current catalog schema
     * @return a map of current schema column index to original
     *         schema column index.
     */
    private static Map<Integer, Integer>
    computeColumnCopyIndexMap(VoltTable origTable, VoltTable newTable)
    {
        Map<Integer, Integer> index_map = new HashMap<Integer, Integer>();

        Set<String> old_column_names = getColumnNames(origTable);
        Set<String> new_column_names = getColumnNames(newTable);

        for (String name : new_column_names)
        {
            if (old_column_names.contains(name))
            {
                index_map.put(newTable.getColumnIndex(name),
                              origTable.getColumnIndex(name));
            }
        }

        return index_map;
    }

    private static Set<String> getColumnNames(VoltTable table)
    {
        Set<String> names = new HashSet<String>();
        int column_count = table.getColumnCount();
        for (int i = 0; i < column_count; i++)
        {
            names.add(table.getColumnName(i));
        }
        return names;
    }

//    public static Object establishTableConversionNecessity(
//            VoltTable inputTable,
//            Table outputTableSchema) {
//
//    }
}
