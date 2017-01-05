/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTypeUtil;

public abstract class SavedTableConverter
{

    public static Boolean needsConversion(VoltTable inputTable,
                                          Table outputTableSchema,
                                          boolean shouldPreserveDRHiddenColumn) {
        int columnsToMatch;
        if (shouldPreserveDRHiddenColumn) {
            // We are expecting the hidden column in inputTable
            columnsToMatch = inputTable.getColumnCount() - 1;
            if (columnsToMatch != outputTableSchema.getColumns().size()) {
                return true;
            }
            if (!inputTable.getColumnName(columnsToMatch).equalsIgnoreCase(CatalogUtil.DR_HIDDEN_COLUMN_NAME) ||
                    inputTable.getColumnType(columnsToMatch) != VoltType.BIGINT) {
                // Make sure input isn't using the reserved column name of the hidden column
                // passive DR table to active DR table, must be converted
                return true;
            }
        }
        else {
            columnsToMatch = inputTable.getColumnCount();
            if (columnsToMatch != outputTableSchema.getColumns().size()) {
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
                                         Table outputTableSchema,
                                         boolean shouldPreserveDRHiddenColumn)
    throws VoltTypeException
    {
        VoltTable new_table;

        if (shouldPreserveDRHiddenColumn) {
            // if the DR hidden column should be preserved in conversion, append it to the end of target schema
            new_table = CatalogUtil.getVoltTable(outputTableSchema, CatalogUtil.DR_HIDDEN_COLUMN_INFO);
        } else {
            new_table = CatalogUtil.getVoltTable(outputTableSchema);
        }

        Map<Integer, Integer> column_copy_index_map =
            computeColumnCopyIndexMap(inputTable, new_table);

        // if original table does not have hidden column present, we need to add
        boolean addDRHiddenColumn = shouldPreserveDRHiddenColumn &&
                !column_copy_index_map.containsKey(new_table.getColumnCount() - 1);
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

        // Copy all the old tuples into the new table
        while (inputTable.advanceRow())
        {
            Object[] coerced_values =
                new Object[new_table.getColumnCount()];

            for (int i = 0; i < new_table.getColumnCount(); i++)
            {
                if (column_copy_index_map.containsKey(i))
                {
                    int orig_column_index = column_copy_index_map.get(i);
                    // For column we have in new table convert and make compatible value.
                    coerced_values[i] = ParameterConverter.tryToMakeCompatible(
                            new_table.getColumnType(i).classFromType(),
                            inputTable.get(orig_column_index,
                                    inputTable.getColumnType(orig_column_index)));
                }
                else
                {
                    // otherwise if it's nullable, insert null,
                    Column catalog_column =
                        outputTableSchema.getColumns().
                        get(new_table.getColumnName(i));
                    // construct an artificial catalog column for dr hidden column
                    if (shouldPreserveDRHiddenColumn && catalog_column == null) {
                        catalog_column = catalogColumnForHiddenColumn;
                    }
                    VoltType default_type =
                        VoltType.get((byte)catalog_column.getDefaulttype());
                    if (default_type != VoltType.INVALID)
                    {
                        // if there is a default value for this table/column
                        // insert the default value
                        try
                        {
                            coerced_values[i] =
                                VoltTypeUtil.
                                getObjectFromString(default_type,
                                                    catalog_column.getDefaultvalue());
                        }
                        catch (ParseException e)
                        {
                            String message = "Column: ";
                            message += new_table.getColumnName(i);
                            message += " has an unparseable default: ";
                            message += catalog_column.getDefaultvalue();
                            message += " for VoltType: ";
                            message += default_type.toString();
                            throw new VoltTypeException(message);
                        }
                    }
                    else if (catalog_column.getNullable())
                    {
                        coerced_values[i] = null;
                    }
                    else
                    {
                        throw new VoltTypeException("Column: " +
                                                    new_table.getColumnName(i) +
                                                    " has no default " +
                                                    "and null is not permitted");
                    }
                }
            }

            new_table.addRow(coerced_values);
        }

        return new_table;
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
