/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltdb.CatalogContext.ProcedurePartitionInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;
import org.voltdb.utils.CatalogUtil;

/**
 * The DefaultProcedureManager is the hub for all default procedure code and it lives in the
 * CatalogContext. It doesn't exist outside of a running VoltDB.
 *
 * It has three jobs. First, know what the set of callable default procs is. This is used by
 * the system catalog stats (jdbc metadata, etc..) and by ClientInterface to accept calls for
 * default procs. Second, it has to generate SQL for default procs. Third, it actually supplies
 * compiled org.voltdb.catalog.Procedure instaces to execution sites upon request.
 *
 */
public class DefaultProcedureManager {

    Map<String, Procedure> m_defaultProcMap = new HashMap<>();

    final Database m_db;
    // fake db makes it easy to create procedures that aren't
    // part of the main catalog
    final Database m_fakeDb;

    public DefaultProcedureManager(Database db) {
        m_db = db;
        m_fakeDb = new Catalog().getClusters().add("cluster").getDatabases().add("database");
        build();
    }

    public Procedure checkForDefaultProcedure(String name) {
        return m_defaultProcMap.get(name.toLowerCase());
    }

    private void build() {
        for (Table table : m_db.getTables()) {
            String prefix = table.getTypeName() + '.';

            // skip export tables XXX why no insert?
            if (CatalogUtil.isTableExportOnly(m_db, table)) {
                continue;
            }

            // skip views XXX why no get by pkey?
            if (table.getMaterializer() != null) {
                continue;
            }

            // select/delete/update crud requires pkey. Pkeys are stored as constraints.
            final CatalogMap<Constraint> constraints = table.getConstraints();
            final Iterator<Constraint> it = constraints.iterator();
            Constraint pkey = null;
            while (it.hasNext()) {
                Constraint constraint = it.next();
                if (constraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                    pkey = constraint;
                    break;
                }
            }

            if (table.getIsreplicated()) {
                // Creating multi-partition insert procedures for replicated table
                addShimProcedure(prefix + "insert", table, null, true, -1, null, false);
                // Creating multi-partition delete/update/upsert procedures for replicated table with pkey
                if (pkey != null) {
                    addShimProcedure(prefix + "delete", table, pkey, false, -1, null, false);
                    addShimProcedure(prefix + "update", table, pkey, true, -1, null, false);
                    addShimProcedure(prefix + "upsert", table, null, true, -1, null, false);
                }
                continue;
            }

            // get the partition column
            final Column partitioncolumn = table.getPartitioncolumn();
            // this check is an accommodation for some tests that don't flesh out a catalog
            if (partitioncolumn == null) {
                continue;
            }
            final int partitionIndex = partitioncolumn.getIndex();

            // all partitioned tables get insert crud procs
            addShimProcedure(prefix + "insert", table, null, true, partitionIndex, partitioncolumn, false);

            // Skip creation of CRUD select/delete/update for partitioned table if no primary key is declared.
            if (pkey == null) {
                continue;
            }

            // Primary key must include the partition column for the table
            // for select/delete/update
            int pkeyPartitionIndex = -1;
            CatalogMap<ColumnRef> pkeycols = pkey.getIndex().getColumns();
            Iterator<ColumnRef> pkeycolsit = pkeycols.iterator();
            while (pkeycolsit.hasNext()) {
                ColumnRef colref = pkeycolsit.next();
                if (colref.getColumn().equals(partitioncolumn)) {
                    pkeyPartitionIndex = colref.getIndex();
                    break;
                }
            }

            // Skip creation of CRUD select/delete/update for partitioned table
            // if primary key does not include the partitioning column.
            if (pkeyPartitionIndex < 0) {
                continue;
            }

            int columnCount = table.getColumns().size();

            // select, delete, update and upsert here (insert generated above)
            // these next 3 prefix params with the pkey so the partition on the index of the partition column
            // within the pkey
            addShimProcedure(prefix + "select", table, pkey, false, pkeyPartitionIndex, partitioncolumn, true);
            addShimProcedure(prefix + "delete", table, pkey, false, pkeyPartitionIndex, partitioncolumn, false);
            // update partitions on the pkey column after the regular column
            addShimProcedure(prefix + "update", table, pkey, true, columnCount + pkeyPartitionIndex, partitioncolumn, false);
            // upsert partitions like a regular insert
            addShimProcedure(prefix + "upsert", table, null, true, partitionIndex, partitioncolumn, false);
        }
    }

    public String sqlForDefaultProc(Procedure defaultProc) {
        String name = defaultProc.getClassname();
        String[] parts = name.split("\\.");
        String action = parts[1];

        Table table = defaultProc.getPartitiontable();
        Column partitionColumn = defaultProc.getPartitioncolumn();

        final CatalogMap<Constraint> constraints = table.getConstraints();
        final Iterator<Constraint> it = constraints.iterator();
        Constraint pkey = null;
        while (it.hasNext()) {
            Constraint constraint = it.next();
            if (constraint.getType() == ConstraintType.PRIMARY_KEY.getValue()) {
                pkey = constraint;
                break;
            }
        }

        switch(action) {
        case "select":
            assert (defaultProc.getSinglepartition());
            return generateCrudSelect(table, partitionColumn, pkey);
        case "insert":
            if (defaultProc.getSinglepartition()) {
                return generateCrudInsert(table, partitionColumn);
            }
            else {
                return generateCrudReplicatedInsert(table);
            }
        case "update":
            if (defaultProc.getSinglepartition()) {
                return generateCrudUpdate(table, partitionColumn, pkey);
            }
            else {
                return generateCrudReplicatedUpdate(table, pkey);
            }
        case "delete":
            if (defaultProc.getSinglepartition()) {
                return generateCrudDelete(table, partitionColumn, pkey);
            }
            else {
                return generateCrudReplicatedDelete(table, pkey);
            }
        case "upsert":
            if (defaultProc.getSinglepartition()) {
                return generateCrudUpsert(table, partitionColumn);
            }
            else {
                return generateCrudReplicatedUpsert(table, pkey);
            }
        default:
            throw new RuntimeException("Invalid input to default proc SQL generator.");
        }
    }

    /** Helper to sort table columns by table column order */
    private static class TableColumnComparator implements Comparator<Column> {
        public TableColumnComparator() {
        }

        @Override
        public int compare(Column o1, Column o2) {
            return o1.getIndex() - o2.getIndex();
        }
    }

    /** Helper to sort index columnrefs by index column order */
    private static class ColumnRefComparator implements Comparator<ColumnRef> {
        public ColumnRefComparator() {
        }

        @Override
        public int compare(ColumnRef o1, ColumnRef o2) {
            return o1.getIndex() - o2.getIndex();
        }
    }

    /**
     * Helper to generate a WHERE pkey_col1 = ?, pkey_col2 = ? ...; clause.
     * @param partitioncolumn partitioning column for the table
     * @param pkey constraint from the catalog
     * @param paramoffset 0-based counter of parameters in the full sql statement so far
     * @param sb string buffer accumulating the sql statement
     * @return offset in the index of the partition column
     */
    private int generateCrudPKeyWhereClause(Column partitioncolumn,
            Constraint pkey, StringBuilder sb)
    {
        // Sort the catalog index columns by index column order.
        ArrayList<ColumnRef> indexColumns = new ArrayList<ColumnRef>(pkey.getIndex().getColumns().size());
        for (ColumnRef c : pkey.getIndex().getColumns()) {
            indexColumns.add(c);
        }
        Collections.sort(indexColumns, new ColumnRefComparator());

        boolean first = true;
        int partitionOffset = -1;

        sb.append(" WHERE ");
        for (ColumnRef pkc : indexColumns) {
            if (!first) sb.append(" AND ");
            first = false;
            sb.append("(" + pkc.getColumn().getName() + " = ?" + ")");
            if (pkc.getColumn() == partitioncolumn) {
                partitionOffset = pkc.getIndex();
            }
        }
        return partitionOffset;

    }

    /**
     * Helper to generate a full col1 = ?, col2 = ?... clause.
     * @param table
     * @param sb
     */
    private void generateCrudExpressionColumns(Table table, StringBuilder sb) {
        boolean first = true;

        // Sort the catalog table columns by column order.
        ArrayList<Column> tableColumns = new ArrayList<Column>(table.getColumns().size());
        for (Column c : table.getColumns()) {
            tableColumns.add(c);
        }
        Collections.sort(tableColumns, new TableColumnComparator());

        for (Column c : tableColumns) {
            if (!first) sb.append(", ");
            first = false;
            sb.append(c.getName() + " = ?");
        }
    }

    /**
     * Helper to generate a full col1, col2, col3 list.
     */
    private void generateCrudColumnList(Table table, StringBuilder sb) {
        boolean first = true;
        sb.append("(");

        // Sort the catalog table columns by column order.
        ArrayList<Column> tableColumns = new ArrayList<Column>(table.getColumns().size());
        for (Column c : table.getColumns()) {
            tableColumns.add(c);
        }
        Collections.sort(tableColumns, new TableColumnComparator());

        // Output the SQL column list.
        for (Column c : tableColumns) {
            assert (c.getIndex() >= 0);  // mostly mask unused 'c'.
            if (!first) sb.append(", ");
            first = false;
            sb.append("?");
        }
        sb.append(")");
    }

    /**
     * Create a statement like:
     *  "delete from <table> where {<pkey-column =?>...}"
     */
    private String generateCrudDelete(Table table, Column partitioncolumn, Constraint pkey) {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());

        generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);
        sb.append(';');

        return sb.toString();
    }

    /**
     * Create a statement like:
     * "update <table> set {<each-column = ?>...} where {<pkey-column = ?>...}
     */
    private String generateCrudUpdate(Table table, Column partitioncolumn, Constraint pkey) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + table.getTypeName() + " SET ");

        generateCrudExpressionColumns(table, sb);
        generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);
        sb.append(';');

        return sb.toString();
    }

    /**
     * Create a statement like:
     *  "insert into <table> values (?, ?, ...);"
     */
    private String generateCrudInsert(Table table, Column partitioncolumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        return sb.toString();
    }

    /**
     * Create a statement like:
     * Hack simple case of implementation SQL MERGE
     *  "upsert into <table> values (?, ?, ...);"
     */
    private String generateCrudUpsert(Table table, Column partitioncolumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        return sb.toString();
    }

    /**
     * Create a statement like:
     *  "insert into <table> values (?, ?, ...);"
     *  for a replicated table.
     */
    private String generateCrudReplicatedInsert(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        return sb.toString();
    }

    /**
     * Create a statement like:
     *  "update <table> set {<each-column = ?>...} where {<pkey-column = ?>...}
     *  for a replicated table.
     */
    private String generateCrudReplicatedUpdate(Table table, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE " + table.getTypeName() + " SET ");

        generateCrudExpressionColumns(table, sb);
        generateCrudPKeyWhereClause(null, pkey, sb);
        sb.append(';');

        return sb.toString();
    }

    /**
     * Create a statement like:
     *  "delete from <table> where {<pkey-column =?>...}"
     * for a replicated table.
     */
    private String generateCrudReplicatedDelete(Table table, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM " + table.getTypeName());

        generateCrudPKeyWhereClause(null, pkey, sb);
        sb.append(';');

        return sb.toString();
    }

    private String generateCrudReplicatedUpsert(Table table, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO " + table.getTypeName() + " VALUES ");

        generateCrudColumnList(table, sb);
        sb.append(";");

        return sb.toString();
    }

    /**
     * Create a statement like:
     *  "select * from <table> where pkey_col1 = ?, pkey_col2 = ? ... ;"
     */
    private String generateCrudSelect(Table table, Column partitioncolumn, Constraint pkey)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM " + table.getTypeName());

        generateCrudPKeyWhereClause(partitioncolumn, pkey, sb);
        sb.append(';');

        return sb.toString();
    }

    private void addShimProcedure(String name,
            Table table,
            Constraint pkey,
            boolean tableCols,
            int partitionParamIndex,
            Column partitionColumn,
            boolean readOnly)
    {
        Procedure proc = m_fakeDb.getProcedures().add(name);
        proc.setClassname(name);
        proc.setDefaultproc(true);
        proc.setHasjava(false);
        proc.setHasseqscans(false);
        proc.setSinglepartition(partitionParamIndex >= 0);
        proc.setPartitioncolumn(partitionColumn);
        proc.setPartitionparameter(partitionParamIndex);
        proc.setReadonly(readOnly);
        proc.setEverysite(false);
        proc.setSystemproc(false);
        proc.setPartitiontable(table);
        if (partitionParamIndex >= 0) {
            proc.setAttachment(new ProcedurePartitionInfo(VoltType.get((byte) partitionColumn.getType()), partitionParamIndex));
        }

        int paramCount = 0;
        if (tableCols) {
            for (Column col : table.getColumns()) {
                // name each parameter "param1", "param2", etc...
                ProcParameter procParam = proc.getParameters().add("param" + String.valueOf(paramCount));
                procParam.setIndex(col.getIndex());
                procParam.setIsarray(false);
                procParam.setType(col.getType());
                paramCount++;
            }
        }
        if (pkey != null) {
            CatalogMap<ColumnRef> pkeycols = pkey.getIndex().getColumns();
            int paramCount2 = paramCount;
            for (ColumnRef cref : pkeycols) {
                // name each parameter "param1", "param2", etc...
                ProcParameter procParam = proc.getParameters().add("param" + String.valueOf(paramCount2));
                procParam.setIndex(cref.getIndex() + paramCount);
                procParam.setIsarray(false);
                procParam.setType(cref.getColumn().getType());
                paramCount2++;
            }
        }

        m_defaultProcMap.put(name.toLowerCase(), proc);
    }
}
