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

package org.voltdb.compilereport;

import static org.apache.commons.lang3.StringEscapeUtils.escapeHtml4;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;

import org.apache.commons.lang3.StringUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.GroupRef;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.Feedback;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogSizing;
import org.voltdb.utils.CatalogSizing.CatalogItemSizeList;
import org.voltdb.utils.CatalogSizing.CatalogItemSizeRollup;
import org.voltdb.utils.CatalogSizing.DatabaseSizes;
import org.voltdb.utils.CatalogSizing.TableSize;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.io.Resources;

public class ReportMaker {

    static Date m_timestamp = new Date();

    /**
     * Make an html bootstrap tag with our custom css class.
     */
    static void tag(StringBuilder sb, String color, String text) {
        sb.append("<span class='label label");
        if (color != null) {
            sb.append("-").append(color);
        }
        String classText = text.replace(' ', '_');
        sb.append(" l-").append(classText).append("'>").append(text).append("</span>");
    }

    static String genrateIndexRow(Table table, Index index) {
        StringBuilder sb = new StringBuilder();
        sb.append("        <tr class='primaryrow2'>");

        // name column
        String anchor = (table.getTypeName() + "-" + index.getTypeName()).toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='s-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='s-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(index.getTypeName());
        sb.append("</a></td>");

        // type column
        sb.append("<td>");
        sb.append(IndexType.get(index.getType()).toString());
        sb.append("</td>");

        // columns column
        sb.append("<td>");
        List<ColumnRef> cols = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
        List<String> columnNames = new ArrayList<String>();
        for (ColumnRef colRef : cols) {
            columnNames.add(colRef.getColumn().getTypeName());
        }
        sb.append(StringUtils.join(columnNames, ", "));
        sb.append("</td>");

        // attribute column
        sb.append("<td>");
        if (index.getAssumeunique()) {
            tag(sb, "success", "AssumeUnique");
        } else if (index.getUnique()) {
            tag(sb, "success", "Unique");
        } else {
            tag(sb, "info", "Nonunique");
        }
        IndexAnnotation annotation = (IndexAnnotation) index.getAnnotation();
        if(annotation == null) {
            sb.append(" ");
            tag(sb, "important", "Unused");
        }

        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE PLAN/DETAIL TABLE
        sb.append("<tr class='dropdown2'><td colspan='5' id='s-"+ table.getTypeName().toLowerCase() +
                "-" + index.getTypeName().toLowerCase() + "--dropdown'>\n");

        if (annotation != null) {
            if (annotation.proceduresThatUseThis.size() > 0) {
                sb.append("<p>Used by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatUseThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
            if (annotation.statementsThatUseThis.size() > 0) {
                assert(annotation.statementsThatUseThis.size() == 1);
                sb.append("<p>Used by the LIMIT PARTITION ROWS Statement</p>");
            }
        }

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateIndexesTable(Table table) {
        StringBuilder sb = new StringBuilder();
        sb.append("    <table class='table tableL2 table-condensed'>\n    <thead><tr>" +
                  "<th>Index Name</th>" +
                  "<th>Type</th>" +
                  "<th>Columns</th>" +
                  "<th>Attributes</th>" +
                  "</tr>\n");

        for (Index index : table.getIndexes()) {
            sb.append(genrateIndexRow(table, index));
        }

        sb.append("    </thead>\n    </table>\n");
        return sb.toString();
    }

    static String generateSchemaRow(Table table, boolean isExportTable) {
        StringBuilder sb = new StringBuilder();
        sb.append("<tr class='primaryrow'>");

        // column 1: table name
        String anchor = table.getTypeName().toLowerCase();
        sb.append("<td style='white-space: nowrap;'><i id='s-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='s-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(table.getTypeName());
        sb.append("</a></td>");

        // column 2: type
        sb.append("<td>");
        if (table.getMaterializer() != null) {
            tag(sb, "info", "Materialized View");
        }
        else {
            if (isExportTable) {
                tag(sb, "inverse", "Export Table");
            } else {
                tag(sb, null, "Table");
            }
        }
        sb.append("</td>");

        // column 3: partitioning
        sb.append("<td style='whitespace: nowrap;'>");
        if (table.getIsreplicated()) {
            tag(sb, "warning", "Replicated");
        }
        else {
            tag(sb, "success", "Partitioned");
            Column partitionCol = table.getPartitioncolumn();
            if (partitionCol != null) {
                sb.append("<small> on " + partitionCol.getName() + "</small>");
            }
            else {
                Table matSrc = table.getMaterializer();
                if (matSrc != null) {
                    sb.append("<small> with " + matSrc.getTypeName() + "</small>");
                }
            }
        }
        sb.append("</td>");

        // column 4: column count
        sb.append("<td>");
        sb.append(table.getColumns().size());
        sb.append("</td>");

        // column 5: index count
        sb.append("<td>");
        sb.append(table.getIndexes().size());

        // computing unused indexes
        int unusedIndexes = 0;
        for (Index index : table.getIndexes()) {
            IndexAnnotation indexAnnotation = (IndexAnnotation) index.getAnnotation();
               if(indexAnnotation == null) {
                   unusedIndexes++;
               }
        }
        if(unusedIndexes !=0 ) {
            sb.append(" (" + unusedIndexes +" unused)");
        }
        sb.append("</td>");

        // column 6: has pkey
        sb.append("<td>");
        boolean found = false;
        for (Constraint constraint : table.getConstraints()) {
            if (ConstraintType.get(constraint.getType()) == ConstraintType.PRIMARY_KEY) {
                found = true;
                break;
            }
        }
        if (found) {
            tag(sb, "info", "Has-PKey");
        }
        else {
            tag(sb, null, "No-PKey");
        }
        sb.append("</td>");

        // column 6: has tuple limit
        sb.append("<td>");
        if (table.getTuplelimit() != Integer.MAX_VALUE) {
            tag(sb, "info", String.valueOf(table.getTuplelimit()));
        } else {
            tag(sb, null, "No-limit");
        }
        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE DDL / INDEXES DETAIL

        sb.append("<tr class='tablesorter-childRow'><td class='invert' colspan='7' id='s-"+ table.getTypeName().toLowerCase() + "--dropdown'>\n");

        TableAnnotation annotation = (TableAnnotation) table.getAnnotation();
        if (annotation != null) {
            // output the DDL
            if (annotation.ddl == null) {
                sb.append("<p>MISSING DDL</p>\n");
            }
            else {
                String ddl = escapeHtml4(annotation.ddl);
                sb.append("<p><pre>" + ddl + "</pre></p>\n");
            }

            // make sure procs appear in only one category
            annotation.proceduresThatReadThis.removeAll(annotation.proceduresThatUpdateThis);

            if (annotation.proceduresThatReadThis.size() > 0) {
                sb.append("<p>Read-only by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatReadThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
            if (annotation.proceduresThatUpdateThis.size() > 0) {
                sb.append("<p>Read/Write by procedures: ");
                List<String> procs = new ArrayList<String>();
                for (Procedure proc : annotation.proceduresThatUpdateThis) {
                    procs.add("<a href='#p-" + proc.getTypeName() + "'>" + proc.getTypeName() + "</a>");
                }
                sb.append(StringUtils.join(procs, ", "));
                sb.append("</p>");
            }
        }

        // LIMIT PARTITION ROW statement may also use the index in this table, prepare the information for report
        if (! table.getTuplelimitdeletestmt().isEmpty()) {
            assert(table.getTuplelimitdeletestmt().size() == 1);
            Statement stmt = table.getTuplelimitdeletestmt().iterator().next();

            for (String tableDotIndexPair : stmt.getIndexesused().split(",")) {
                if (tableDotIndexPair.length() == 0) continue;
                String parts[] = tableDotIndexPair.split("\\.", 2);
                assert(parts.length == 2);
                if (parts.length != 2) continue;
                String tableName = parts[0];
                String indexName = parts[1];
                if (! table.getTypeName().equals(tableName)) continue;

                Index i = table.getIndexes().get(indexName);
                assert(i != null);
                IndexAnnotation ia = (IndexAnnotation) i.getAnnotation();
                if (ia == null) {
                    ia = new IndexAnnotation();
                    i.setAnnotation(ia);
                }
                ia.statementsThatUseThis.add(stmt);
            }
        }

        if (table.getIndexes().size() > 0) {
            sb.append(generateIndexesTable(table));
        }
        else {
            sb.append("<p>No indexes defined on table.</p>\n");
        }

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateSchemaTable(Database db) {
        StringBuilder sb = new StringBuilder();
        SortedSet<Table> exportTables = CatalogUtil.getExportTables(db);
        for (Table table : db.getTables()) {
            sb.append(generateSchemaRow(table, exportTables.contains(table) ? true : false));
        }
        return sb.toString();
    }

    static String genrateStatementRow(CatalogMap<Table> tables, Procedure procedure, Statement statement) {
        // get the proc annotation which should exist or be created just before this is called
        ProcedureAnnotation procAnnotation = (ProcedureAnnotation) procedure.getAnnotation();
        assert(procAnnotation != null);

        StringBuilder sb = new StringBuilder();
        sb.append("        <tr class='primaryrow2'>");

        // name column
        String anchor = (procedure.getTypeName() + "-" + statement.getTypeName()).toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='p-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#' id='p-");
        sb.append(anchor).append("' class='togglex'>");
        sb.append(statement.getTypeName());
        sb.append("</a></td>");

        // sql column
        sb.append("<td><tt>");
        sb.append(escapeHtml4(statement.getSqltext()));
        sb.append("</td></tt>");

        // params column
        sb.append("<td>");
        List<StmtParameter> params = CatalogUtil.getSortedCatalogItems(statement.getParameters(), "index");
        List<String> paramTypes = new ArrayList<String>();
        for (StmtParameter param : params) {
            paramTypes.add(VoltType.get((byte) param.getJavatype()).name());
        }
        if (paramTypes.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(paramTypes, ", "));
        sb.append("</td>");

        // r/w column
        sb.append("<td>");
        if (statement.getReadonly()) {
            tag(sb, "success", "Read");
        }
        else {
            tag(sb, "warning", "Write");
        }
        sb.append("</td>");

        // attributes
        sb.append("<td>");

        if (!statement.getIscontentdeterministic() || !statement.getIsorderdeterministic()) {
            tag(sb, "inverse", "Determinism");
        }

        if (statement.getSeqscancount() > 0) {
            tag(sb, "important", "Scans");
        }

        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE PLAN/DETAIL TABLE
        sb.append("<tr class='dropdown2'><td colspan='5' id='p-"+ procedure.getTypeName().toLowerCase() +
                "-" + statement.getTypeName().toLowerCase() + "--dropdown'>\n");

        sb.append("<div class='well well-small'><h4>Explain Plan:</h4>\n");

        String plan = escapeHtml4(Encoder.hexDecodeToString(statement.getExplainplan()));
        plan = plan.replace("\n", "<br/>");
        plan = plan.replace(" ", "&nbsp;");

        for (String tableName : statement.getTablesread().split(",")) {
            if (tableName.length() == 0) continue;

            Table t = tables.get(tableName);
            assert(t != null);
            TableAnnotation ta = (TableAnnotation) t.getAnnotation();
            assert(ta != null);
            ta.statementsThatReadThis.add(statement);
            ta.proceduresThatReadThis.add(procedure);
            procAnnotation.tablesRead.add(t);

            String uname = tableName.toUpperCase();
            String link = "\"<a href='#s-" + tableName + "'>" + uname + "</a>\"";
            plan = plan.replace("&quot;" + uname + "&quot;", link);
        }
        for (String tableName : statement.getTablesupdated().split(",")) {
            if (tableName.length() == 0) continue;

            Table t = tables.get(tableName);
            assert(t != null);
            TableAnnotation ta = (TableAnnotation) t.getAnnotation();
            assert(ta != null);
            ta.statementsThatUpdateThis.add(statement);
            ta.proceduresThatUpdateThis.add(procedure);
            procAnnotation.tablesUpdated.add(t);

            String uname = tableName.toUpperCase();
            String link = "\"<a href='#s-" + tableName + "'>" + uname + "</a>\"";
            plan = plan.replace("&quot;" + uname + "&quot;", link);
        }
        for (String tableDotIndexPair : statement.getIndexesused().split(",")) {
            if (tableDotIndexPair.length() == 0) continue;
            String parts[] = tableDotIndexPair.split("\\.", 2);
            assert(parts.length == 2);
            if (parts.length != 2) continue;
            String tableName = parts[0];
            String indexName = parts[1];

            Table t = tables.get(tableName);
            assert(t != null);
            Index i = t.getIndexes().get(indexName);
            assert(i != null);
            IndexAnnotation ia = (IndexAnnotation) i.getAnnotation();
            if (ia == null) {
                ia = new IndexAnnotation();
                i.setAnnotation(ia);
            }
            ia.proceduresThatUseThis.add(procedure);
            procAnnotation.indexesUsed.add(i);

            String uindexName = indexName.toUpperCase();
            String link = "\"<a href='#s-" + tableName + "-" + indexName +"'>" + uindexName + "</a>\"";
            plan = plan.replace("&quot;" + uindexName + "&quot;", link);
        }

        sb.append("<tt>").append(plan).append("</tt>");

        sb.append("</div>\n");

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateStatementsTable(CatalogMap<Table> tables, Procedure procedure) {
        StringBuilder sb = new StringBuilder();
        sb.append("    <table class='table tableL2 table-condensed'>\n    <thead><tr>" +
                  "<th><span style='white-space: nowrap;'>Statement Name</span></th>" +
                  "<th>Statement SQL</th>" +
                  "<th>Params</th>" +
                  "<th>R/W</th>" +
                  "<th>Attributes</th>" +
                  "</tr>\n");

        for (Statement statement : procedure.getStatements()) {
            sb.append(genrateStatementRow(tables, procedure, statement));
        }

        sb.append("    </thead>\n    </table>\n");
        return sb.toString();
    }

    static String generateProcedureRow(CatalogMap<Table> tables, Procedure procedure) {
        StringBuilder sb = new StringBuilder();
        sb.append("<tr class='primaryrow'>");

        // column 1: procedure name
        String anchor = procedure.getTypeName().toLowerCase();
        sb.append("<td style='white-space: nowrap'><i id='p-" + anchor + "--icon' class='icon-chevron-right'></i> <a href='#p-");
        sb.append(anchor).append("' id='p-").append(anchor).append("' class='togglex'>");
        sb.append(procedure.getTypeName());
        sb.append("</a></td>");

        // column 2: parameter types
        sb.append("<td>");
        List<ProcParameter> params = CatalogUtil.getSortedCatalogItems(procedure.getParameters(), "index");
        List<String> paramTypes = new ArrayList<String>();
        for (ProcParameter param : params) {
            String paramType = VoltType.get((byte) param.getType()).name();
            if (param.getIsarray()) {
                paramType += "[]";
            }
            paramTypes.add(paramType);
        }
        if (paramTypes.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(paramTypes, ", "));
        sb.append("</td>");

        // column 3: partitioning
        sb.append("<td>");
        if (procedure.getSinglepartition()) {
            tag(sb, "success", "Single");
        }
        else {
            tag(sb, "warning", "Multi");
        }
        sb.append("</td>");

        // column 4: read/write
        sb.append("<td>");
        if (procedure.getReadonly()) {
            tag(sb, "success", "Read");
        }
        else {
            tag(sb, "warning", "Write");
        }
        sb.append("</td>");

        // column 5: access
        sb.append("<td>");
        List<String> groupNames = new ArrayList<String>();
        for (GroupRef groupRef : procedure.getAuthgroups()) {
            groupNames.add(groupRef.getGroup().getTypeName());
        }
        if (groupNames.size() == 0) {
            sb.append("<i>None</i>");
        }
        sb.append(StringUtils.join(groupNames, ", "));
        sb.append("</td>");

        // column 6: attributes
        sb.append("<td>");
        if (procedure.getHasjava()) {
            tag(sb, "info", "Java");
        }
        else {
            tag(sb, null, "Single-Stmt");
        }
        boolean isND = false;
        int scanCount = 0;
        for (Statement stmt : procedure.getStatements()) {
            scanCount += stmt.getSeqscancount();
            if (!stmt.getIscontentdeterministic() || !stmt.getIsorderdeterministic()) {
                isND = false;
            }
        }
        if (isND) {
            tag(sb, "inverse", "Determinism");
        }
        if (scanCount > 0) {
            tag(sb, "important", "Scans");
        }
        sb.append("</td>");

        sb.append("</tr>\n");

        // BUILD THE DROPDOWN FOR THE STATEMENT/DETAIL TABLE

        sb.append("<tr class='tablesorter-childRow'><td class='invert' colspan='6' id='p-"+ procedure.getTypeName().toLowerCase() + "--dropdown'>\n");

        // output partitioning parameter info
        if (procedure.getSinglepartition()) {
            String pTable = procedure.getPartitioncolumn().getParent().getTypeName();
            String pColumn = procedure.getPartitioncolumn().getTypeName();
            int pIndex = procedure.getPartitionparameter();

            sb.append(String.format("<p>Partitioned on parameter %d which maps to column %s" +
                                    " of table <a class='invert' href='#s-%s'>%s</a>.</p>",
                                    pIndex, pColumn, pTable, pTable));
        }

        // get the annotation or ensure it's there
        ProcedureAnnotation annotation = (ProcedureAnnotation) procedure.getAnnotation();
        if (annotation == null) {
            annotation = new ProcedureAnnotation();
            procedure.setAnnotation(annotation);
        }

        // this needs to be run before the ProcedureAnnotation is used below
        // because it modifies it
        String statementsTable = generateStatementsTable(tables, procedure);

        // output what schema this interacts with
        // make sure tables appear in only one category
        annotation.tablesRead.removeAll(annotation.tablesUpdated);

        if (annotation.tablesRead.size() > 0) {
            sb.append("<p>Read-only access to tables: ");
            List<String> tableList = new ArrayList<String>();
            for (Table table : annotation.tablesRead) {
                tableList.add("<a href='#s-" + table.getTypeName() + "'>" + table.getTypeName() + "</a>");
            }
            sb.append(StringUtils.join(tableList, ", "));
            sb.append("</p>");
        }
        if (annotation.tablesUpdated.size() > 0) {
            sb.append("<p>Read/Write access to tables: ");
            List<String> tableList = new ArrayList<String>();
            for (Table table : annotation.tablesUpdated) {
                tableList.add("<a href='#s-" + table.getTypeName() + "'>" + table.getTypeName() + "</a>");
            }
            sb.append(StringUtils.join(tableList, ", "));
            sb.append("</p>");
        }
        if (annotation.indexesUsed.size() > 0) {
            sb.append("<p>Uses indexes: ");
            List<String> indexes = new ArrayList<String>();
            for (Index index : annotation.indexesUsed) {
                Table table = (Table) index.getParent();
                indexes.add("<a href='#s-" + table.getTypeName() + "-" + index.getTypeName() + "'>" + index.getTypeName() + "</a>");
            }
            sb.append(StringUtils.join(indexes, ", "));
            sb.append("</p>");
        }

        sb.append(statementsTable);

        sb.append("</td></tr>\n");

        return sb.toString();
    }

    static String generateProceduresTable(CatalogMap<Table> tables, CatalogMap<Procedure> procedures) {
        StringBuilder sb = new StringBuilder();
        for (Procedure procedure : procedures) {
            if (procedure.getDefaultproc()) {
                continue;
            }
            sb.append(generateProcedureRow(tables, procedure));
        }
        return sb.toString();
    }

    static String generateSizeTable(DatabaseSizes sizes) {
        StringBuilder sb = new StringBuilder();
        sb.append("<!--##SIZES##-->\n");
        int nrow = 0;
        for (TableSize tsize: sizes.tableSizes) {
            sb.append(generateSizeRow(tsize, ++nrow));
        }
        for (TableSize vsize: sizes.viewSizes) {
            sb.append(generateSizeRow(vsize, ++nrow));
        }
        return sb.toString();
    }

    static String generateSizeRow(TableSize tsize, int nrow) {
        StringBuilder sb = new StringBuilder();
        sb.append("<tr class='primaryrow'>");

        // column 1: table name
        String anchor = String.format("size-%d", nrow);
        sb.append(String.format(
            "<td class='table-view-name' id='s-%s'>", anchor)).append(
                tsize.name).append(
            "</td>\n");

        // column 2: type
        sb.append("<td>");
        if (tsize.isView) {
            tag(sb, "info", "Materialized View");
        }
        else {
            tag(sb, null, "Table");
        }
        sb.append("</td>\n");

        // column 3: estimated row count
        final String updateCode = "sizes_update_all();";
        sb.append(
            "<td>").append(
                "<div class='ecount'>").append(
                    "<input type='text' class='form-control count-input'").append(String.format(
                            " id='s-%s-count'", anchor)).append(String.format(
                            " onblur='%s'", updateCode)).append(String.format(
                            " value='%d'", tsize.cardinality)).append(
                            " class='form-control'").append(
                            " placeholder='.ecount'").append(
                        ">").append(
                    "</input>\n").append(
                "</div>").append(
            "</td>\n");

        // column 4: row min size
        sb.append(String.format("<td id='s-%s-rmin' class='right-cell'>%d</td>\n", anchor, tsize.widthMin));

        // column 5: row max size
        sb.append(String.format("<td id='s-%s-rmax' class='right-cell'>%d</td>\n", anchor, tsize.widthMax));

        // Roll up index sizes since a table can have multiple indexes.
        CatalogItemSizeRollup indexSizeRollup = tsize.indexRollup();

        // column 6: index min size
        sb.append(String.format("<td id='s-%s-imin' class='right-cell'>%d</td>\n", anchor, indexSizeRollup.widthMin));

        // column 7: index max size
        sb.append(String.format("<td id='s-%s-imax' class='right-cell'>%d</td>\n", anchor, indexSizeRollup.widthMax));

        // column 8: table min size (including index min size)
        // Updated by Javascript and this initial number is thrown away.
        long tmin = (tsize.widthMin + indexSizeRollup.widthMin) * tsize.cardinality;
        sb.append(String.format("<td id='s-%s-tmin' class='right-cell calculated-cell'>%d</td>\n", anchor, tmin));

        // column 9: table max size (including index max size)
        // Updated by Javascript and this initial number is thrown away.
        long tmax = (tsize.widthMax + indexSizeRollup.widthMax) * tsize.cardinality;
        sb.append(String.format("<td id='s-%s-tmax' class='right-cell calculated-cell'>%d</td>\n", anchor, tmax));

        sb.append("</tr>\n");

        //=== Details drop-down.

        sb.append(
            "<tr class='tablesorter-childRow'>").append(String.format(
                "<td class='invert' colspan='6' id='s-%s--dropdown'>\n", anchor));

        TableAnnotation annotation = (TableAnnotation) tsize.table.getAnnotation();
        if (annotation != null) {
            // output the DDL
            if (annotation.ddl == null) {
                sb.append("<p>MISSING DDL</p>\n");
            }
            else {
                String ddl = escapeHtml4(annotation.ddl);
                sb.append("<p><pre>" + ddl + "</pre></p>\n");
            }
        }

        if (tsize.table.getIndexes().size() > 0) {
            sb.append(generateIndexesTable(tsize.table));
        }
        else {
            sb.append("<p>No indexes defined on table.</p>\n");
        }

        sb.append(
                "</td>").append(
            "</tr>\n");

        return sb.toString();
    }

    static String generateSizeSummary(DatabaseSizes dbSizes) {
        StringBuilder sb = new StringBuilder();

        CatalogItemSizeList<CatalogItemSizeRollup> rollups =
                new CatalogItemSizeList<CatalogItemSizeRollup>();
        rollups.add(dbSizes.tableRollup());
        rollups.add(dbSizes.viewRollup());
        rollups.add(dbSizes.indexRollup());
        CatalogItemSizeRollup rollupRollup = rollups.rollup(1);

        sb.append("<table class='table size-summary-table'>\n");
        generateSizeRollupSummary("tables whose row data ", "table", sb, rollups.get(0));
        generateSizeRollupSummary("materialized views whose row data ", "view", sb, rollups.get(1));
        generateSizeRollupSummary("indexes whose key data and overhead ", "index", sb, rollups.get(2));

        sb.append("<tr><td colspan='6'>&nbsp;</td></tr>\n"); // blank row

        // write the totals
        sb.append("<tr>");
        if (rollupRollup.widthMin == rollupRollup.widthMax) {
            sb.append("<td colspan='2'><b>Total user data is expected to use about</b>&nbsp;</td>");
            sb.append(String.format("<td id='s-size-summary-total-min' class='right-cell calculated-cell'>%d</td>", rollupRollup.widthMin));
            sb.append("<td colspan='3'>&nbsp;of memory.</td>");
        }
        else {
            sb.append("<td colspan='2'><b>Total user data is expected to use between</b>&nbsp;</td>");
            sb.append(String.format("<td id='s-size-summary-total-min' class='right-cell calculated-cell'>%d</td>", rollupRollup.widthMin));
            sb.append("<td>&nbsp;<b>and</b>&nbsp;</td>");
            sb.append(String.format("<td id='s-size-summary-total-max' class='right-cell calculated-cell'>%d</td>", rollupRollup.widthMax));
            sb.append("<td>&nbsp<b>of memory.</b></td>");
        }
        sb.append("</tr>\n");

        sb.append("</table>\n");
        return sb.toString();
    }

    private static void generateSizeRollupSummary(
            String name,
            String label,
            StringBuilder sb,
            CatalogItemSizeRollup rollup)
    {
        String prefix = String.format("s-size-summary-%s", label);
        sb.append("<tr>");
        sb.append(String.format("<td id='%s-count' class='right-cell'>%d</td>", prefix, rollup.itemCount));
        sb.append(String.format("<td>%s is expected to use", name));
        // different output if the range is 0
        if (rollup.widthMin == rollup.widthMax) {
            sb.append(" about&nbsp;</td>");
            sb.append(String.format("<td id='%s-min' class='right-cell calculated-cell'>%d</td>", prefix, rollup.widthMin));
            sb.append("<td colspan='3'>");
        }
        else {
            sb.append(" between&nbsp;</td>");
            sb.append(String.format("<td id='%s-min' class='right-cell calculated-cell'>%d</td>", prefix, rollup.widthMin));
            sb.append("<td>&nbsp;and&nbsp;</td>");
            sb.append(String.format("<td id='%s-max' class='right-cell calculated-cell'>%d</td>", prefix, rollup.widthMax));
            sb.append("<td>");
        }
        sb.append("&nbsp; of memory.&nbsp;</td>");
        sb.append("</tr>\n");
    }


    /**
     * Get some embeddable HTML of some generic catalog/application stats
     * that is drawn on the first page of the report.
     */
    static String getStatsHTML(Database db, ArrayList<Feedback> warnings) {
        StringBuilder sb = new StringBuilder();
        sb.append("<table class='table table-condensed'>\n");

        // count things
        int indexes = 0, views = 0, statements = 0;
        int partitionedTables = 0, replicatedTables = 0;
        int partitionedProcs = 0, replicatedProcs = 0;
        int readProcs = 0, writeProcs = 0;
        for (Table t : db.getTables()) {
            if (t.getMaterializer() != null) {
                views++;
            }
            else {
                if (t.getIsreplicated()) {
                    replicatedTables++;
                }
                else {
                    partitionedTables++;
                }
            }

            indexes += t.getIndexes().size();
        }
        for (Procedure p : db.getProcedures()) {
            // skip auto-generated crud procs
            if (p.getDefaultproc()) {
                continue;
            }

            if (p.getSinglepartition()) {
                partitionedProcs++;
            }
            else {
                replicatedProcs++;
            }

            if (p.getReadonly()) {
                readProcs++;
            }
            else {
                writeProcs++;
            }

            statements += p.getStatements().size();
        }

        // version
        sb.append("<tr><td>Compiled by VoltDB Version</td><td>");
        sb.append(VoltDB.instance().getVersionString()).append("</td></tr>\n");

        // timestamp
        sb.append("<tr><td>Compiled on</td><td>");
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss z");
        sb.append(sdf.format(m_timestamp)).append("</td></tr>\n");

        // tables
        sb.append("<tr><td>Table Count</td><td>");
        sb.append(String.format("%d (%d partitioned / %d replicated)",
                partitionedTables + replicatedTables, partitionedTables, replicatedTables));
        sb.append("</td></tr>\n");

        // views
        sb.append("<tr><td>Materialized View Count</td><td>").append(views).append("</td></tr>\n");

        // indexes
        sb.append("<tr><td>Index Count</td><td>").append(indexes).append("</td></tr>\n");

        // procedures
        sb.append("<tr><td>Procedure Count</td><td>");
        sb.append(String.format("%d (%d partitioned / %d replicated) (%d read-only / %d read-write)",
                partitionedProcs + replicatedProcs, partitionedProcs, replicatedProcs,
                readProcs, writeProcs));
        sb.append("</td></tr>\n");

        // statements
        sb.append("<tr><td>SQL Statement Count</td><td>").append(statements).append("</td></tr>\n");
        sb.append("</table>\n\n");

        // warnings, add warning section if any
        if (warnings.size() > 0){
            sb.append("<h4>Warnings</h4>");
            sb.append("<table class='table table-condensed'>\n");
            for (Feedback warning : warnings) {
                String procName = warning.getFileName().replace(".class", "");
                String nameLink = "";
                // not a warning during compiling procedures, must from the schema
                if (procName.compareToIgnoreCase("null") == 0) {
                    String schemaName = "";
                    String warningMsg = warning.getMessage().toLowerCase();
                    if (warningMsg.contains("table ")) {
                        int begin = warningMsg.indexOf("table ") + 6;
                        int end = (warningMsg.substring(begin)).indexOf(" ");
                        schemaName = warningMsg.substring(begin, begin + end);
                    }
                    nameLink = "<a href='#s-" + schemaName + "'>" + schemaName.toUpperCase() + "</a>";
                } else {
                    nameLink = "<a href='#p-" + procName.toLowerCase() + "'>" + procName + "</a>";
                }
                sb.append("<tr><td>").append(nameLink).append("</td><td>").append(escapeHtml4(warning.getMessage())).append("</td></tr>\n");
            }
            sb.append("").append("</table>\n").append("</td></tr>\n");
        }

        return sb.toString();
    }

    /**
     * Generate the HTML catalog report from a newly compiled VoltDB catalog
     */
    public static String report(Catalog catalog, ArrayList<Feedback> warnings, String autoGenDDL) throws IOException {
        // asynchronously get platform properties
        new Thread() {
            @Override
            public void run() {
                PlatformProperties.getPlatformProperties();
            }
        }.start();


        URL url = Resources.getResource(ReportMaker.class, "template.html");
        String contents = Resources.toString(url, Charsets.UTF_8);

        Cluster cluster = catalog.getClusters().get("cluster");
        assert(cluster != null);
        Database db = cluster.getDatabases().get("database");
        assert(db != null);

        String statsData = getStatsHTML(db, warnings);
        contents = contents.replace("##STATS##", statsData);

        // generateProceduresTable needs to happen before generateSchemaTable
        // because some metadata used in the later is generated in the former
        String procData = generateProceduresTable(db.getTables(), db.getProcedures());
        contents = contents.replace("##PROCS##", procData);

        String schemaData = generateSchemaTable(db);
        contents = contents.replace("##SCHEMA##", schemaData);

        DatabaseSizes sizes = CatalogSizing.getCatalogSizes(db);

        String sizeData = generateSizeTable(sizes);
        contents = contents.replace("##SIZES##", sizeData);

        String sizeSummary = generateSizeSummary(sizes);
        contents = contents.replace("##SIZESUMMARY##", sizeSummary);

        String platformData = PlatformProperties.getPlatformProperties().toHTML();
        contents = contents.replace("##PLATFORM##", platformData);

        contents = contents.replace("##VERSION##", VoltDB.instance().getVersionString());

        contents = contents.replace("##DDL##", escapeHtml4(autoGenDDL));

        DateFormat df = new SimpleDateFormat("d MMM yyyy HH:mm:ss z");
        contents = contents.replace("##TIMESTAMP##", df.format(m_timestamp));

        String msg = Encoder.hexEncode(VoltDB.instance().getVersionString() + "," + System.currentTimeMillis());
        contents = contents.replace("get.py?a=KEY&", String.format("get.py?a=%s&", msg));

        return contents;
    }

    public static String getLiveSystemOverview()
    {
        // get the start time
        long t = SystemStatsCollector.getStartTime();
        Date date = new Date(t);
        long duration = System.currentTimeMillis() - t;
        long minutes = duration / 60000;
        long hours = minutes / 60; minutes -= hours * 60;
        long days = hours / 24; hours -= days * 24;
        String starttime = String.format("%s (%dd %dh %dm)",
                date.toString(), days, hours, minutes);

        // handle the basic info page below this
        SiteTracker st = VoltDB.instance().getSiteTrackerForSnapshot();

        // get the cluster info
        String clusterinfo = st.getAllHosts().size() + " hosts ";
        clusterinfo += " with " + st.getAllSites().size() + " sites ";
        clusterinfo += " (" + st.getAllSites().size() / st.getAllHosts().size();
        clusterinfo += " per host)";

        StringBuilder sb = new StringBuilder();
        sb.append("<table class='table table-condensed'>\n");
        sb.append("<tr><td>Mode                     </td><td>" + VoltDB.instance().getMode().toString() + "</td><td>\n");
        sb.append("<tr><td>VoltDB Version           </td><td>" + VoltDB.instance().getVersionString() + "</td><td>\n");
        sb.append("<tr><td>Buildstring              </td><td>" + VoltDB.instance().getBuildString() + "</td><td>\n");
        sb.append("<tr><td>Cluster Composition      </td><td>" + clusterinfo + "</td><td>\n");
        sb.append("<tr><td>Running Since            </td><td>" + starttime + "</td><td>\n");

        sb.append("</table>\n");

        return sb.toString();
    }

    /**
     * Find the pre-compild catalog report in the jarfile, and modify it for use in the
     * the built-in web portal.
     */
    public static String liveReport() {
        byte[] reportbytes = VoltDB.instance().getCatalogContext().getFileInJar("catalog-report.html");
        String report = new String(reportbytes, Charsets.UTF_8);

        // remove commented out code
        report = report.replace("<!--##RESOURCES", "");
        report = report.replace("##RESOURCES-->", "");

        // inject the cluster overview
        String clusterStr = "<h4>System Overview</h4>\n<p>" + getLiveSystemOverview() + "</p><br/>\n";
        report = report.replace("<!--##CLUSTER##-->", clusterStr);

        // inject the running system platform properties
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String ppStr = "<h4>Cluster Platform</h4>\n<p>" + pp.toHTML() + "</p><br/>\n";
        report = report.replace("<!--##PLATFORM2##-->", ppStr);

        // change the live/static var to live
        if (VoltDB.instance().getConfig().m_isEnterprise) {
            report = report.replace("&b=r&", "&b=e&");
        }
        else {
            report = report.replace("&b=r&", "&b=c&");
        }

        return report;
    }
}
